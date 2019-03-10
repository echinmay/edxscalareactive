package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import com.sun.org.apache.xerces.internal.xs.XSNamespaceItemList
import scala.language.postfixOps

import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class SnapShotTimer(seq: Long, num: Int)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  val maxRetries = 3

  val delay = 200 milliseconds

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  //var pending = Vector.empty[Snapshot]
  var pending = List[Snapshot]()

  // Map the snapshot sequence number to the
  var tMap = Map.empty[Long, (Cancellable)]

  var snapMap = Map.empty[Long, Snapshot]

  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def sendSnapshot(s: Snapshot, tryNum : Int): Unit = {
    replica ! s
    // start timer for 100 ms
    val t =
      context.system.scheduler.scheduleOnce(delay,
        context.self,
        SnapShotTimer(s.seq, tryNum))
    tMap = tMap updated (s.seq, t)
  }

  def stopTimer(seq: Long) = {
    if (tMap contains seq) {
      val t = tMap.get(seq).get
      if (!t.isCancelled) t.cancel()
    }
  }

  def cleanUp(seq: Long) = {
    stopTimer(seq)
    snapMap = snapMap - seq
    acks = acks - seq
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(k, vo, id) => {
      val s = Snapshot(k, vo, nextSeq())
      // Add the snap shot to the replication request
      acks = acks updated (s.seq, (context.sender(), Replicate(k, vo, id)))
      snapMap = snapMap updated (s.seq, s)
      if (pending.length == 0) {
        sendSnapshot(s, 1)
      } else {
        pending = pending ++ List(Snapshot(k, vo, nextSeq()))
      }
    }

    case SnapshotAck(key, seq) => {
      if (acks contains seq) {
        val x = acks.get(seq)
        x.map( ar => {
          ar._1 ! Replicated(ar._2.key, ar._2.id)
        })
      }
      cleanUp(seq)
      // Take the next pending snapshot and send
      if (pending.size > 0) {
        val s = pending.head
        pending = pending.tail
        sendSnapshot(s, 1)
      }
    }

    case SnapShotTimer(seq, numtimes) => {
      if ((numtimes == maxRetries) || (!snapMap.contains(seq))) {
        // Tried three times. We now give up.
        cleanUp(seq)
      } else {
        // Retry one more time
        val s = snapMap.get(seq).get
        sendSnapshot(s, numtimes + 1)
      }
    }
    case _ =>
  }

}
