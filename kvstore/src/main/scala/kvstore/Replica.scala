package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout
import scala.language.postfixOps

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class ReplicateTimerRsp(rid: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with akka.actor.ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]

  val maxRetries = 9

  val delay = 100 milliseconds

  // Map of sequence number of snapshot to the actual snapshot and
  // the actor to send the SnapshotAck response
  var snapShotMap = Map.empty[Long, (Snapshot, ActorRef)]

  var prstTmrMap = Map.empty[Long, (Cancellable)]

  // Timer map to get response from replicas.
  var replicateTMap = Map.empty[Long, (Cancellable)]

  // Transaction Id -> (sendor of Insert/Delete, List of replicators)
  var transIdReplicator = Map.empty[Long, (ActorRef, Replicate, Set[ActorRef])]

  var primaryPendingPersist = Set.empty[Long]

  // a map from replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]

  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var nextSeqNum = 0L

  val pActor = context.actorOf(persistenceProps)

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def addnewkey(k: String, v: String) = kv = kv updated (k, v)

  def removekey(k: String): Unit = if (kv contains k) { kv = kv - k }

  def sendSnapshotAck(s: Snapshot, a: ActorRef) = a ! SnapshotAck(s.key, s.seq)

  def sendPersist(s: Snapshot, tryNum: Int): Unit = {
    val t =
      context.system.scheduler.scheduleOnce(this.delay,
        context.self,
        PersistenceTimerRsp(s.seq, tryNum))
    prstTmrMap = prstTmrMap updated (s.seq, t)
    val sk = s.key
    val sv = s.valueOption
    val perid = s.seq
    pActor ! Persist(s.key, s.valueOption, s.seq)
  }

  def handleSnapShot(s: Snapshot, saveVal: Boolean):Unit = {
    if (s.seq == nextSeqNum) {
      // If there is already a pending persist dont process this.
      if (snapShotMap contains s.seq) return
      snapShotMap = snapShotMap updated (s.seq, (s, context.sender()))

      s.valueOption match {
        case Some(v) => addnewkey(s.key, v)
        case None => removekey(s.key)
      }

      sendPersist(s, 1)
    } else if (s.seq < nextSeqNum) {
      sendSnapshotAck(s, context.sender())
    }
  }

  def handlePersistenceAck(pack: Persisted) = {
    if (snapShotMap contains pack.id) {
      //println("Got persistence ack")
      val (s, a) = snapShotMap.get(pack.id).get
      sendSnapshotAck(s, a)
      nextSeqNum = nextSeqNum + 1
    }
    snapShotMap = snapShotMap - pack.id
    if (prstTmrMap contains(pack.id)) {
      val t = prstTmrMap.get(pack.id).get
      if (!t.isCancelled) t.cancel()
      prstTmrMap = prstTmrMap - pack.id
    }
  }

  def handlePersistTimerExp(texp: PersistenceTimerRsp): Unit = {
    if (texp.numtimes == this.maxRetries) {
      // Cant do anything now...
      prstTmrMap = prstTmrMap - texp.seq
      snapShotMap = snapShotMap - texp.seq
    } else {
      if (snapShotMap contains texp.seq) {
        val (s, _) = snapShotMap.get(texp.seq).get
        sendPersist(s, texp.numtimes+1)
      }
    }
  }

  def sendReplicateMsgs(r: Replicate) : Unit = {
    // Insert the new transaction in the Map with the set of Replicas as the value
    transIdReplicator = transIdReplicator updated (r.id, (context.sender(), r, this.replicators.toSet))
    // Send the Replicate Message to all the current replicas

    replicators foreach (repl => {
      repl ! r
    })
    // Start a 1 second timer
    val t =
      context.system.scheduler.scheduleOnce(1 second,
        context.self,
        ReplicateTimerRsp(r.id))
    replicateTMap = replicateTMap updated (r.id, t)
  }

  def checkAllConditions(id: Long): Unit = {
    //println("Check if primary persist is pending")
    if (primaryPendingPersist contains id) return

    // Now check if all secondaries have responded.
    if (transIdReplicator contains id) {
      val (aref, repl, l) = transIdReplicator.get(id).get
      if (l.size == 0) {
        //println("size of pending secondary persisted messages = 0")
        aref ! OperationAck(repl.id)
        transIdReplicator = transIdReplicator - id
        val tmr = replicateTMap.get(id).get
        if (!tmr.isCancelled) {
          tmr.cancel()
        }
        replicateTMap = replicateTMap - id
      }
    } else {
      //println(s"Could not find transaction id $id")
    }
  }

  def handleReplicatedMsg(id: Long, replicator: ActorRef): Unit = {
    //println("handleReplicatedMsg")
    // Lookup transaction in the transaction Map.
    if (transIdReplicator contains  id) {
      //println("Got transaction map")
      val (sndr, r, sr) = transIdReplicator.get(id).get
      //println("Got replicated message")
      val newsr = sr - replicator
      transIdReplicator = transIdReplicator updated (id, (sndr, r, newsr))
      if (newsr.size == 0) {
        //println("Got all replicated responses")
        checkAllConditions(id)
      } else {
        val left = newsr.size
        //println(s"Number of replicated messages left is $left" )
      }
    } else
      return
  }

  def handleReplicateTmr(rtm: ReplicateTimerRsp): Unit = {
    //println("Replicate timer expired.... Sending failed response")
    if (transIdReplicator contains rtm.rid) {
      val (sndr, _, sr) = transIdReplicator.get(rtm.rid).get
      replicateTMap = replicateTMap - rtm.rid
      transIdReplicator = transIdReplicator - rtm.rid
      sndr ! OperationFailed(rtm.rid)
    }
  }

  def syncNewReplica(newr: ActorRef): Unit = {
    // Weak sync.... We will send a replicate message and Hope for the best !!!!
    val replicator = secondaries.get(newr).get
    val kvindex = kv.zipWithIndex

    kvindex foreach ( (x) => {
      val key = x._1._1
      val value = Some(x._1._2)
      replicator ! Replicate(key, value, x._2)
    })
  }

  def addNewReplicas(newr: Set[ActorRef]):Unit = {
    newr foreach (r => {
      val newreplicator = context.actorOf(Replicator.props(r))
      //println(s"Adding new replicator $newreplicator for replica $r")
      secondaries = secondaries updated (r, newreplicator)
      replicators = replicators + newreplicator
      syncNewReplica(r)
    })
  }

  def removeReplicas(oldr: Set[ActorRef]): Unit = {
    val removedReplicators = oldr.map(r => secondaries.get(r).get)
    //println("List of removed replicas")
    //removedReplicators.foreach(println(_))
    val idRemovedActor = for {
      (id, (sndr, r, l)) <- transIdReplicator
      removedActor <- removedReplicators
      if l.contains(removedActor)
    } yield (id, removedActor)

    //println(s"--- $idRemovedActor")
    idRemovedActor foreach ( x => handleReplicatedMsg(x._1, x._2))
    removedReplicators foreach (a => a ! PoisonPill)
  }

  def sendPrimaryPersist(s: Snapshot): Unit = {
    if (snapShotMap contains s.seq) return
    snapShotMap = snapShotMap updated (s.seq, (s, context.sender()))
    sendPersist(s, 1)
    primaryPendingPersist = primaryPendingPersist + s.seq
  }

  def processPrimaryPersisted(pack: Persisted): Unit = {
    //println("Got primary persisted")
    primaryPendingPersist = primaryPendingPersist - pack.id
    snapShotMap = snapShotMap - pack.id

    if (prstTmrMap contains(pack.id)) {
      val t = prstTmrMap.get(pack.id).get
      if (!t.isCancelled) t.cancel()
      prstTmrMap = prstTmrMap - pack.id
    }
    checkAllConditions(pack.id)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(k, v, id) => {
      addnewkey(k, v)
      sendReplicateMsgs(Replicate(k, Some(v), id))
      sendPrimaryPersist(Snapshot(k, Some(v), id))
    }

    case Remove(k, id) => {
      removekey(k)
      sendReplicateMsgs(Replicate(k, None, id))
      sendPrimaryPersist(Snapshot(k, None, id))
    }

    case s: Snapshot => handleSnapShot(s, false)

    case pack: Persisted => processPrimaryPersisted(pack)

    case texp: PersistenceTimerRsp => handlePersistTimerExp(texp)

    case rpltd: Replicated => handleReplicatedMsg(rpltd.id, context.sender())

    case Get(k, id) => context.sender() ! GetResult(k, kv get k, id)

    case rpltmrrsp: ReplicateTimerRsp => handleReplicateTmr(rpltmrrsp)

    case r: Replicas => {
      val currReplicas = secondaries.keys.toSet
      val replicas_except_primary = r.replicas - this.self
      //println(s"Current replicas $currReplicas")
      //println(s"replicas expcept primary $replicas_except_primary")
      val newr = replicas_except_primary diff currReplicas
      val removedr = currReplicas diff replicas_except_primary
      //println(s"new replicas $newr")
      //println(s"Removed replicas $removedr")
      addNewReplicas(newr)
      removeReplicas(removedr)
    }

    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(k, id) => context.sender() ! GetResult(k, kv get k, id)

    case s: Snapshot => handleSnapShot(s, true)

    case pack: Persisted => handlePersistenceAck(pack)

    case texp: PersistenceTimerRsp => handlePersistTimerExp(texp)

    case _ =>
  }

  // Send a message to the arbiter that I joined
  arbiter ! Join

}

