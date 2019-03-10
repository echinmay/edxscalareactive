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

  case class ReplicateTimerRsp(seq: Long, numtimes: Int)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  val maxRetries = 9

  val delay = 100 milliseconds

  // Map of sequence number of snapshot to the actual snapshot and
  // the actor to send the SnapshotAck response
  var snapShotMap = Map.empty[Long, (Snapshot, ActorRef)]
  var tMap = Map.empty[Long, (Cancellable)]

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
        ReplicateTimerRsp(s.seq, tryNum))
    tMap = tMap updated (s.seq, t)
    val sk = s.key
    val sv = s.valueOption
    val perid = s.seq
    println(s"Sending persist $sk val : $sv $pActor, id : $perid")
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
      val (s, a) = snapShotMap.get(pack.id).get
      sendSnapshotAck(s, a)
      nextSeqNum = nextSeqNum + 1
    }
    snapShotMap = snapShotMap - pack.id
    if (tMap contains(pack.id)) {
      val t = tMap.get(pack.id).get
      if (!t.isCancelled) t.cancel()
      tMap = tMap - pack.id
    }
  }

  def handleReplicationTimerExp(texp: ReplicateTimerRsp): Unit = {
    println("Got timer expriry")
    if (texp.numtimes == this.maxRetries) {
      // Cant do anything now...
      tMap = tMap - texp.seq
      snapShotMap = snapShotMap - texp.seq
    } else {
      if (snapShotMap contains texp.seq) {
        val (s, _) = snapShotMap.get(texp.seq).get
        sendPersist(s, texp.numtimes+1)
      }
    }
  }



  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(k, v, id) => {
      addnewkey(k, v)
      context.sender() ! OperationAck(id)
    }
    case Remove(k, id) => {
      removekey(k)
      context.sender() ! OperationAck(id)
    }

    case s: Snapshot => handleSnapShot(s, false)

    case pack: Persisted => handlePersistenceAck(pack)

    case texp: ReplicateTimerRsp => handleReplicationTimerExp(texp)

    case Get(k, id) => context.sender() ! GetResult(k, kv get k, id)

    case r: Replicas =>

    case _ =>
  }


  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(k, id) => context.sender() ! GetResult(k, kv get k, id)

    case s: Snapshot => handleSnapShot(s, true)

    case pack: Persisted => handlePersistenceAck(pack)

    case texp: ReplicateTimerRsp => handleReplicationTimerExp(texp)

    case _ =>
  }

  // Send a message to the arbiter that I joined
  arbiter ! Join

}

