import java.security.MessageDigest

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks._

/**
 * Created by swarnatonse on 18-10-2015.
 */
object Project3 extends App{
  var numOfNodes:Int =args(0).toInt
  var numOfReq:Int = args(1).toInt
  var nodeList:List[Node] = Nil
  Initialize.initializeHopCount(numOfNodes)
  nodeList = Initialize.initNodes(numOfNodes)
  Initialize.initNetwork(numOfNodes,nodeList)
  for(n<-0 to numOfNodes-1){
    nodeList(n).findKeys(numOfReq, nodeList)
  }
  println("Average number of hops is "+Initialize.findAverageHops(numOfReq));

}


object Initialize{
  var hopCount:ArrayBuffer[Int] = ArrayBuffer[Int]()
  def initNodes(numOfNodes:Int):List[Node] = {
    var nodeList:List[Node] = Nil
    for(i<-0 to numOfNodes-1)
    {
      nodeList ::= new Node()
    }
    return nodeList
  }
  def initNetwork(numOfNodes:Int, nodeList:List[Node])={
    var hashNodeList:ArrayBuffer[Int] = ArrayBuffer[Int]()

    for(i<-0 to numOfNodes-1)
    {
      hashNodeList = nodeList(i).join(nodeList, hashNodeList, i)
    }
    for(i<-0 to numOfNodes-1) {
      println()
      println("Node:"+nodeList(i).nodeID)
      for (j <- 0 to 7) {

        println("start:" + nodeList(i).fingerTable(j).start + "     end:" + nodeList(i).fingerTable(j).endInterval + "     nodeNum:" + nodeList(i).fingerTable(j).nodeNum)
      }
    }
  }
  def initializeHopCount(numOfNodes:Int)={

    for(i<-0 to numOfNodes-1)
    {
      var someInt:Int = 0
      hopCount.append(someInt)
    }
  }

  def findAverageHops(numOfReq:Int):Int={
    var sum:Int = 0
    for(n<-0 to hopCount.size-1)
    {
      var avg = hopCount(n)/numOfReq
      sum = sum+avg
    }
    return sum/(hopCount.size-1)
  }
}

class FingerTable
{
  var start:Int = -99
  var nodeNum:Int = -99
  var endInterval:Int = -99
}

class Node {
  var fingerTable: ArrayBuffer[FingerTable] = ArrayBuffer[FingerTable]()
  //var successor:Int = -999
  var predecessor: Int = -999
  var nodeID: Int = -999
  var m = 8
  var points = scala.math.pow(2, m)
  var hashNodeIDList: ArrayBuffer[Int] = null
  val msgDigest = MessageDigest.getInstance("SHA-1")

  def join(nodeList: List[Node], hashNodeList: ArrayBuffer[Int], newNode: Int): ArrayBuffer[Int] = {
    hashNodeIDList = ArrayBuffer[Int]()
    hashNodeIDList = hashNodeList
    if (hashNodeIDList.size == 0) {
      nodeID = hashNodeID(newNode)
      hashNodeIDList.append(nodeID)
      populateFingerTable()
      fingerTable(0).nodeNum = nodeID
      predecessor = nodeID
    }
    else {
      var randomNode = hashNodeIDList.length - 1
      nodeID = hashNodeID(newNode)
      hashNodeIDList.append(nodeID)
      populateFingerTable()
      initFingerTable(randomNode, nodeList)
      fixFingers(nodeList)
      //  updateOthers(nodeID,nodeList)

    }
    return hashNodeIDList
  }

  def hashNodeID(newNode: Int): Int = {
    var nodeStr: String = "N" + newNode
    var hashVal = msgDigest.digest(nodeStr.getBytes("UTF-8"))
    var hashNodeVal: String = hashVal.map("%02x" format _).mkString.substring(0, m - 1)
    nodeID = Integer.parseInt(hashNodeVal, 16) % (scala.math.pow(2, m).toInt)
    return nodeID
  }

  def populateFingerTable() = {
    for (i <- 0 to m - 1) {
      var index: Int = nodeID + scala.math.pow(2, i).toInt
      var ft = new FingerTable
      ft.start = index % scala.math.pow(2, m).toInt
      if (hashNodeIDList.length == 1) {
        ft.nodeNum = nodeID
      }
      fingerTable.append(ft)
    }
    for (i <- 0 to m - 1) {
      if (i != m - 1) {
        fingerTable(i).endInterval = fingerTable(i + 1).start
      }
      else {
        fingerTable(i).endInterval = nodeID
      }
    }
  }

  def findSuccessor(newNodeID: Int, oldNode: Int, nodeList: List[Node], searching:Boolean = false): Int = {

    var p = findPredecessor(newNodeID, oldNode, nodeList,searching)
    if(searching == true)
    {
      Initialize.hopCount(oldNode) = Initialize.hopCount(oldNode) + 1
    }
    return nodeList(returnNodeFromNodeID(p)).fingerTable(0).nodeNum
    //return nodeList(returnNodeFromNodeID(p)).Successor(nodeList:List[Node])
  }

  def findPredecessor(newNodeID: Int, node: Int, nodeList: List[Node],searching:Boolean = false): Int = {
    var oldNode: Int = node
    var oldNodeID: Int = nodeID
    var newNodeIden = newNodeID
    breakable {
      while (findInterval(oldNodeID, nodeList(oldNode).fingerTable(0).nodeNum, newNodeIden, true, true, false) != true) {
        if(searching == true)
        {
          Initialize.hopCount(oldNode) = Initialize.hopCount(oldNode) + 1
        }
        var prevOldNode: Int = oldNodeID
        oldNodeID = nodeList(oldNode).closestPrecedingFinger(newNodeIden, oldNode, oldNodeID, nodeList)
        if (prevOldNode == oldNodeID) break
        oldNode = returnNodeFromNodeID(oldNodeID)
      }
    }
    return oldNodeID
  }

  def closestPrecedingFinger(newNodeID: Int, oldNode: Int, oldNodeID: Int, nodeList: List[Node]): Int = {
    var n: Int = m - 1
    while (n >= 0) {
      var fingerNode: Int = fingerTable(n).nodeNum
      if (findInterval(oldNodeID, newNodeID, fingerNode, true, false, true)) {
        return fingerNode
      }
      n = n - 1
    }
    return oldNodeID
  }

  def findInterval(startInterval: Int, endInterval: Int, elem: Int, frontOpened: Boolean, endClosed: Boolean, endsOpen: Boolean): Boolean = {
    var j: Int = startInterval
    if (endsOpen != true && startInterval == endInterval) {
      return true
    }
    if (frontOpened) {
      j = (j + 1) % points.toInt

    }
    while (j != endInterval) {
      if (j == elem) {
        return true
      }
      if (j == points - 1) {
        j = 0
      }
      else {
        j = j + 1
      }
    }
    if (endClosed == true) {
      if (endInterval == elem) {
        return true
      }
    }
    return false
  }

  def returnNodeFromNodeID(nodeID: Int): Int = {
    return hashNodeIDList.indexOf(nodeID)
  }

  def initFingerTable(oldNode: Int, nodeList: List[Node]) = {
    fingerTable(0).nodeNum = nodeList(oldNode).findSuccessor(fingerTable(0).start, oldNode, nodeList)
    var finger1Node: Int = fingerTable(0).nodeNum
    predecessor = nodeList(returnNodeFromNodeID(finger1Node)).predecessor
    nodeList(returnNodeFromNodeID(finger1Node)).predecessor = nodeID
    nodeList(returnNodeFromNodeID(predecessor)).fingerTable(0).nodeNum = nodeID
    for (i <- 0 to m - 2) {
      if (findInterval(nodeID, fingerTable(i).nodeNum, fingerTable(i + 1).start, false, false, false)) {
        fingerTable(i + 1).nodeNum = fingerTable(i).nodeNum
      }
      else {
        fingerTable(i + 1).nodeNum = nodeList(oldNode).findSuccessor(fingerTable(i + 1).start, oldNode, nodeList)
      }
    }
  }

  def updateOthers(currentNodeID: Int, nodeList: List[Node]) = {
    var i: Int = 0
    for (i <- 0 to m - 1) {
      var j = currentNodeID - scala.math.pow(2, i).toInt
      if (j < 0) j = j + points.toInt
      var p = findPredecessor(j, hashNodeIDList.indexOf(currentNodeID), nodeList)
      var nodeIndex = returnNodeFromNodeID(p)
      nodeList(nodeIndex).updateFingerTable(currentNodeID, i, p, nodeList)
    }
  }

  def updateFingerTable(s: Int, i: Int, n: Int, nodeList: List[Node]): Boolean = {
    if (findInterval(n, fingerTable(i).nodeNum, s, false, false, false)) {
      printf("Swarna: Since %d falls between [%d,%d), %d.fingerTable(%d).nodeNum is %d\n", s, n, fingerTable(i).nodeNum, nodeID, i, s)
      fingerTable(i).nodeNum = s
      var p = predecessor
      if (predecessor != s) {
        var nodeIndex = returnNodeFromNodeID(p)
        nodeList(nodeIndex).updateFingerTable(s, i, p, nodeList)
      }

    }
    return true
  }

  def fixFingers(nodeList: List[Node]) = {
    for (i <- 0 to hashNodeIDList.size - 2) {
      for (j <- 1 to m - 1) {
        nodeList(i).fingerTable(j).nodeNum = nodeList(i).findSuccessor(nodeList(i).fingerTable(j).start, i, nodeList)
      }
    }
  }

  def findKeys(numOfReq: Int, nodeList: List[Node]) = {
    for (i <- 1 to numOfReq) {
      var randomKey = Random.nextInt(points.toInt)
      var n = findSuccessor(randomKey, hashNodeIDList.indexOf(nodeID), nodeList, true)
    }
  }
}
