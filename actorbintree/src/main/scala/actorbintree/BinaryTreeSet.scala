/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root forward op
    case GC => {
      val newroot = createRoot
      context.become(garbageCollecting(newroot))
      // Send a copy to message to the old root
      root ! CopyTo(newroot)
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => {
      pendingQueue = pendingQueue.enqueue(op)
    }
    case CopyFinished => {
      pendingQueue.foreach(o => newRoot ! o)
      pendingQueue = Queue.empty[Operation]
      root = newRoot
      context.become(normal)
    }
    // Ignore GC messages
    case GC =>
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def isActorPresent(position: Position): Boolean = this.subtrees.contains(position)

  def getActor(position: Position): ActorRef = this.subtrees.get(position).get

  def getPosActorRef(position: Position, elem: Int):ActorRef = {
    if (!isActorPresent(position)) {
      this.subtrees = this.subtrees.updated(position, context.actorOf(props(elem, false)))
    }
    getActor(position)
  }

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, elem) => {
      if (elem == this.elem) {
        removed = false
        requester ! OperationFinished(id)
      } else if (elem < this.elem) {
        // Go left
        getPosActorRef(Left, elem) ! Insert(requester, id, elem)
      } else {
        // Go right
        getPosActorRef(Right, elem) ! Insert(requester, id, elem)
      }
    }

    case Contains(requester, id, elem) => {
      if (elem == this.elem) {
        requester ! ContainsResult(id, !this.removed)
      } else if (elem < this.elem) {
        if (isActorPresent(Left)) {
          getActor(Left) ! Contains(requester, id, elem)
        } else {
          requester ! ContainsResult(id, false)
        }
      } else {
        if (isActorPresent(Right)) {
          getActor(Right) ! Contains(requester, id, elem)
        } else {
          requester ! ContainsResult(id, false)
        }
      }
    }

    case Remove(requester, id, elem) => {
      if (elem == this.elem) {
        removed = true
        requester ! OperationFinished(id)
      } else if (elem < this.elem) {
        if (isActorPresent(Left)) {
          getActor(Left) ! Remove(requester, id, elem)
        } else {
          requester ! OperationFinished(id)
        }
      } else {
        if (isActorPresent(Right)) {
          getActor(Right) ! Remove(requester, id, elem)
        } else {
          requester ! OperationFinished(id)
        }
      }

    }
    case CopyTo(newRoot) => {
      var insertConfirmed: Boolean = false
      //println(s"CopyTo for $elem ")
      if (!this.removed) {
        // Copy this node
        newRoot ! Insert(context.self, this.elem, this.elem)
       // println(s"CopyTo for $elem started")
      } else {
       // println(s"copyTo for $elem is skipped because it is removed")
        insertConfirmed = true
      }

      if (doneCopying(this.subtrees.values.toSet, insertConfirmed)) {
       // println(s"Done copying for $elem")
        cleanupActor()
      } else {
        // Copy the left and right nodes
        // println(s"CopyTo for $elem. Going over the mapValues: $sizeOfTrees ")

        if (this.subtrees.contains((Left))) {
          // println("Sending to left subtree")
          this.subtrees.get(Left).get ! CopyTo(newRoot)
        }

        if (this.subtrees.contains((Right))) {
          // println("Sending to right subtree")
          this.subtrees.get(Right).get ! CopyTo(newRoot)
        }


        context.become(copying(this.subtrees.values.toSet, insertConfirmed ))
      }
    }
  }

  def doneCopying(expected: Set[ActorRef], insertConfirmed: Boolean): Boolean =
    expected.size == 0 && insertConfirmed

  def cleanupActor() = {
    context.parent ! CopyFinished
    context.self ! PoisonPill
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(id) => {
      if (doneCopying(expected, true)) {
        cleanupActor()
      } else {
        context.become(copying(expected, true))
      }
    }

    case CopyFinished => {
      //println(s"Got CopyFinished for $elem")
      if (doneCopying(expected - sender(), insertConfirmed)) {
        cleanupActor()
      } else {
        context.become(copying(expected - sender(), insertConfirmed))
      }
    }
  }

}
