import org.w3c.dom.NodeList

import scala.util.Random

import scala.collection.mutable.ArrayBuffer
import akka.actor.{Props, ActorRef, Actor, ActorSystem}
import java.security.MessageDigest
import scala.math
import util.control.Breaks._
import akka.actor._
import akka.pattern.ask

/**
 * Created by swarnatonse on 18-10-2015.
 */
object Project3 extends App{
  var numOfNodes:Int =args(0).toInt
  var numOfReq:Int = args(1).toInt
  //var nodeList:List[ActorRef] = Nil
  Initialize.initNodes(numOfNodes)
  Initialize.initNetwork(numOfNodes)
  Initialize.printFingerTable(numOfNodes)
}


object Initialize{
  val system=ActorSystem("NodeActor")
  var nodeList:List[ActorRef] = Nil
  var hashNodeList:ArrayBuffer[Int] = ArrayBuffer[Int]()
  def initNodes(numOfNodes:Int) {

    for(i<-0 to numOfNodes-1)
    {
      nodeList ::= system.actorOf(Props[Node])
    }
  }

  def initNetwork(numOfNodes:Int)={
  //  nodeList(i) ! join(nodeList, i)
    for(i<-0 to numOfNodes-1)
    {
      if(i!=0){
        Thread sleep 10000
      }

      nodeList(i) ! join(nodeList, i)
      Thread sleep 10000
    }

  }
  def printFingerTable(numOfNodes:Int)={
    for(i<-0 to numOfNodes-1)
    { Thread sleep 1000
      nodeList(i) ! printTable()
    }

  }
}

class FingerTable
{
  var start:Int = -99
  var nodeNum:Int = -99
  var endInterval:Int = -99
}

class Node extends Actor{
  var fingerTable:ArrayBuffer[FingerTable]=ArrayBuffer[FingerTable]()
  //var successor:Int = -999
  var endPoint:Int= -999
  var predecessor:Int = -999
  var nodeID:Int = -999
  var m=3
  var points=scala.math.pow(2,m)
  var ReqCount:Int = 0
  val msgDigest = MessageDigest.getInstance("SHA-1")
  var recursiveNode:Int= -999
  var fixFingerNode:Int= -999
  def receive = {
    case join(nodeList, newNode) => {

      if (Initialize.hashNodeList.size == 0) {

        nodeID = hashNodeID(newNode)
        //println("Initial node:" + nodeID)
        Initialize.hashNodeList.append(nodeID)
        //successor = nodeID
        populateFingerTable()
        fingerTable(0).nodeNum = nodeID
        predecessor = nodeID
      }
      else {

        var randomNode = Initialize.hashNodeList.length - 1
        nodeID = hashNodeID(newNode)
        Initialize.hashNodeList.append(nodeID)
        populateFingerTable()
        initFingerTable(randomNode, nodeList)
        fixFingers(nodeList)

      }
    }
    case printTable() =>{
      for(i<-0 to m-1){
        println("start:" + fingerTable(i).start + "     end:" + fingerTable(i).endInterval + "     nodeNum:" + fingerTable(i).nodeNum)
      }
    }
    //    }
  case getSuccessor(senderID,index, nodeList) => {
        //sender
        nodeList(senderID) ! updateSuccessor(index,fingerTable(0).nodeNum)
   }
    case updateSuccessor(index,succ)=>{

      fingerTable(index).nodeNum=succ

    }
      case findSuccessor(newNodeID, oldNode, nodeList,nodeIndex,index) => {
        var p=findPredecessor(newNodeID, oldNode, nodeList)
        nodeList(returnNodeFromNodeID(p)) ! getSuccessor(nodeIndex,index, nodeList)

      }
      case updateRecursiveNode(value) =>{
        recursiveNode=value
      }
      case closestPrecedingFinger(newNodeID, oldNode, oldNodeID, nodeList,nodeIndex) => {
        var n: Int = m - 1
        var flag:Boolean=true
        breakable {
          while (n >= 0) {
            var fingerNode: Int = fingerTable(n).nodeNum
            if (findInterval(oldNodeID, newNodeID, fingerNode, true, false, true)) {
              nodeList(nodeIndex) ! updateRecursiveNode(fingerNode)
              flag=false
              break
            }

            n = n - 1
          }
        }
        if(flag) {
          nodeList(nodeIndex) ! updateRecursiveNode(oldNodeID)
        }
      }

    case getInterval(nodeIndex,nodeList)=>{

      nodeList(nodeIndex) ! setEndPoint(fingerTable(0).nodeNum)
    }
    case setEndPoint(end)=>{
      endPoint=end
    }

   case getPredecessor(nodeIndex,nodeList) => {
    nodeList(nodeIndex) ! updatePredecessor(predecessor)
  }
    case updatePredecessor(predVal)=>{
      predecessor=predVal
    }
    case getFingerStart(index,nodeIndex,nodeList)=>{
      nodeList(nodeIndex) ! updateFixFingerNode(fingerTable(index).start)
    }
    case updateFixFingerNode(value)=>{
      fixFingerNode=value
    }
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
      if (Initialize.hashNodeList.length == 1) {
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
  def findInterval(startInterval: Int, endInterval: Int, elem: Int, frontOpened: Boolean, endClosed: Boolean, endsOpen: Boolean):Boolean = {
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
  def fixFingers(nodeList:List[ActorRef]) = {
    for(i<-0 to Initialize.hashNodeList.size-2){
      for(j<-1 to m-1){
        nodeList(i) ! getFingerStart(j,returnNodeFromNodeID(nodeID),nodeList)
        nodeList(i) ! findSuccessor(fixFingerNode,i,nodeList,i,j)
      }
    }
   }
  def initFingerTable(oldNode: Int, nodeList: List[ActorRef]) = {
    nodeList(oldNode) ! findSuccessor(fingerTable(0).start,oldNode,nodeList,returnNodeFromNodeID(nodeID),0)
    Thread sleep 10000
    var finger1Node:Int=fingerTable(0).nodeNum

    nodeList(returnNodeFromNodeID(finger1Node)) ! getPredecessor(returnNodeFromNodeID(nodeID),nodeList)
    nodeList(returnNodeFromNodeID(finger1Node)) ! updatePredecessor(nodeID)
    nodeList(returnNodeFromNodeID(predecessor)) ! updateSuccessor(0,nodeID)

    for(i<-0 to m-2){
      if(findInterval(nodeID,fingerTable(i).nodeNum,fingerTable(i+1).start,false,false,false)){
        fingerTable(i+1).nodeNum=fingerTable(i).nodeNum
      }
      else{
        nodeList(oldNode) ! findSuccessor(fingerTable(i+1).start,oldNode,nodeList,returnNodeFromNodeID(nodeID),i+1)

        //fingerTable(i+1).nodeNum=nodeList(oldNode).findSuccessor(fingerTable(i+1).start,oldNode,nodeList)
      }
    }
  }
  def findPredecessor(newNodeID: Int, node: Int, nodeList: List[ActorRef]):Int= {
    var oldNode: Int = node

//    println("&&&&&&&&&&&&&&&&&&&&&&&&" + oldNode)
    recursiveNode = nodeID
    var newNodeIden = newNodeID
    nodeList(oldNode) ! getInterval(returnNodeFromNodeID(nodeID),nodeList)
    Thread sleep 5000
    breakable {

      while (findInterval(recursiveNode, endPoint, newNodeIden, true, true, false) != true) {
        var prevOldNode: Int = recursiveNode
        nodeList(oldNode) ! closestPrecedingFinger(newNodeIden, oldNode, recursiveNode, nodeList,returnNodeFromNodeID(nodeID))

        if (prevOldNode == recursiveNode) break
        oldNode = returnNodeFromNodeID(recursiveNode)
        nodeList(oldNode) ! getInterval(returnNodeFromNodeID(nodeID),nodeList)
      }
    }
    return recursiveNode
  }
  def returnNodeFromNodeID(nodeID:Int):Int = {
    return Initialize.hashNodeList.indexOf(nodeID)
  }

}


case class join(nodeList:List[ActorRef], newNode:Int)
case class findSuccessor(newNodeID: Int, oldNode: Int, nodeList: List[ActorRef],nodeID:Int,index:Int)
//case class findPredecessor(newNodeID: Int, node: Int, nodeList: List[ActorRef])
case class closestPrecedingFinger(newNodeID: Int, oldNode: Int, oldNodeID: Int, nodeList: List[ActorRef],nodeIndex:Int)
case class getSuccessor(senderID:Int, index:Int,nodeList:List[ActorRef])
case class updateSuccessor(index:Int,successor:Int)
case class getInterval(nodeIndex:Int,nodeList:List[ActorRef])
case class setEndPoint(end:Int)
case class updateRecursiveNode(value:Int)
case class getPredecessor(nodeIndex:Int,nodeList:List[ActorRef])
case class updatePredecessor(predVal:Int)
case class getFingerStart(index:Int,nodeIndex:Int,nodeList:List[ActorRef])
case class updateFixFingerNode(value:Int)
case class printTable()
//case class findInterval(startInterval: Int, endInterval: Int, elem: Int, frontOpened: Boolean, endClosed: Boolean, endsOpen: Boolean)
//case class getSuccessor()
//case class getFingerStart(j:Int)
//case class updateFingerNode(j:Int,fingerNode:Int)
//case class getPredecessor()
//case class returnNodeFromNodeID(nodeID: Int)
//case class fixFingers(nodeList: List[ActorRef])
//case class initFingerTable(oldNode: Int, nodeList: List[ActorRef])
