package org.apache.spark.scheduler

import scala.collection.mutable.TreeSet // used to hold sorted Id's
import scala.collection.mutable.Stack // used for dfs
import scala.collection.mutable.HashMap 
import scala.collection.mutable.ListBuffer

// each Node holds it's parents instead of children because it was easier/ faster to implement that way
// if each node were to hold its child then on would access the taskNodes hashmap each time a new node is made
// this may be better
class TaskNode(
	val taskId: Long,
	val stageId: Int,
	val firstJobId: Int,
	val partitionId: Int,
    val outputForPartition: Array[Long]
    ) 
{
	var sumOfBytesIn: Long = 0
	// initialize an empty child task set which will be populated as more stages complete
	var childTaskIds: TreeSet[Long] = new TreeSet[Long]()
	// a string to indicate the executor
	var executorId: String = "NOT SET"
	var status: Int = 0 // 0 not visited, 1 kept, 2 cut
	def addChild(childTaskId: Long) {
		childTaskIds += childTaskId
	}
	def removeChild(childTaskId: Long) {
		childTaskIds -= childTaskId
	}

	def getOutputForPartition(index: Int): Long = outputForPartition(index)
	def getOutputForPartitionSize(): Int = outputForPartition.length


	override def toString(): String = {
		var toReturn: String = ""
		toReturn += "Task(" + taskId + "), on executor: " + executorId + ", has child tasks( " + childTaskIds.mkString(" ") + " ) and outputs "
		for (i <- 0 to (getOutputForPartitionSize()-1)) {
			toReturn += getOutputForPartition(i) + " to par " + i 
			if (i < (getOutputForPartitionSize()-1))
			toReturn += ", "
		}
		toReturn
	}
}

class RootTaskNode() 
extends TaskNode(-Long.MaxValue, -1, -1, -1, Array.fill[Long](1)(0))
{
	sumOfBytesIn = Long.MaxValue
	override def getOutputForPartition(index: Int): Long = outputForPartition(0)
	override def toString(): String = { 
		"Task(" + taskId + ") has child tasks( " + childTaskIds.mkString(" ") +
		" ) and outputs " + getOutputForPartition(0) + " to every child"
	}
}

class TaskGraph() {
  // When task are made they are added to this HashMap because a stage does not 
  // have access to it's task Id's
  val stageIdToTasks: HashMap[Int, TreeSet[Long]] = new HashMap[Int, TreeSet[Long]]()

  // Task data hashed by taskId
  val taskNodes: HashMap[Long, TaskNode] = new HashMap[Long, TaskNode]()  // elements added as Stages complete
  // executor used for each task
  val taskToExecutor: HashMap[Task[_],String] = new HashMap[Task[_], String]() // elements added as task are sent to backend
  // TODO: FIXTHIS ^^ should only need one hasmap holding node info

  // after each shuffle stage completion the stageId is hashed to the shuffleId
  // so that if another shuffle comes with the same dependency and therefore 
  // same shuffleId, we can then refrence the first stage to complete the shuffle
  val shuffleIdToStageId: HashMap[Int, Int] = new HashMap[Int, Int]()
  
  // add endpoint nodes
  val ROOT_NODE_ID = -Long.MaxValue
  val END_NODE_ID = Long.MaxValue
  // val MAX_WEIGHT = Long.MaxValue

  // Root and End nodes are used for min k cut 
  taskNodes += ROOT_NODE_ID -> new RootTaskNode() // start node
  taskNodes += END_NODE_ID -> new TaskNode(END_NODE_ID, -1, -1, 0, new Array[Long](0)) // end node


  // populates stageIdToTasks HashMap, called at task completetion
  def addTaskToHash(taskId: Long, stageId: Int) {  
  	if (stageIdToTasks.contains(stageId))
  	stageIdToTasks.get(stageId).get += taskId
  	else {
  		stageIdToTasks += stageId -> new TreeSet[Long]()
  		stageIdToTasks.get(stageId).get += taskId
  	}
  }

  // called after stage completetion
  // used to populate the graph with taskNodes within this stage
  // determines all the task dependencies base on the parent stages
  def addStage(stage: Stage, parentStages: List[Stage]) {

  	val parentStagesIds: Array[Int] = parentStages.map(stage => stage.id).toArray

  	val parentTasksIds: TreeSet[Long] = new TreeSet[Long]() // does not need to be a treeset could be a list buffer
  	for (parentStageId <- parentStagesIds) {
  		if (stageIdToTasks.get(parentStageId) != None) {
  			for (parentTask <- stageIdToTasks.get(parentStageId).get){
  				parentTasksIds += parentTask
  			}
  		}
  		else // should never happen
  		println("\n\nIssue with non existing stage " + parentStageId + "\n\n")
  	}

  	val newTasks: Array[Long] = stageIdToTasks.get(stage.id).get.toArray
  	var outputsForPartitions: Array[Array[Long]] = new Array[Array[Long]](0)
  	stage match {
  		case smt : ShuffleMapStage => {
  			val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
  			// populate the shuffleIdToStageId HashMap
  			shuffleIdToStageId += shuffleStage.shuffleDep.shuffleId -> stage.id
  			val mapStatuses: Array[MapStatus] = shuffleStage.outputLocInMapOutputTrackerFormat()
  			outputsForPartitions = Array.ofDim[Long](mapStatuses.length, mapStatuses.length)
  			for (i <- 0 to (mapStatuses.length-1))
  			for (j <- 0 to (mapStatuses.length-1)) 
  			outputsForPartitions(i)(j) = mapStatuses(i).getSizeForBlock(j)
  		}
  		case rt: ResultStage => {} // leave outputsForPartitions empty

  	}

  	for (i <- 0 to (newTasks.size-1)) {
  		stage match {
  			case smt : ShuffleMapStage => {
  				taskNodes += newTasks(i) -> new TaskNode(
  					newTasks(i),
  					stage.id,
  					stage.firstJobId,
  					i, // based on the fact that the lower taskId has the lower partition
					outputsForPartitions(i) // based on the fact that the lower taskId has the lower partition
				)
  			}

  			// MAYBE add this node as achild to the start node if this node has no parents

  			case rt: ResultStage => {
  				val newTaskNode: TaskNode = new TaskNode(
  					newTasks(i),
  					stage.id,
  					stage.firstJobId,
  					i,
  					Array.fill[Long](1)(Long.MaxValue) // TODO: locate where these result tasks are actually writing
				)
				newTaskNode.addChild(END_NODE_ID) // since result task have it's child be the END node
  				taskNodes += newTasks(i) -> newTaskNode
			// MAYBE set this node's childre to be the end node

  			}
  		}
  		if (parentStagesIds.length == 0) { // if task has no parents set its parent to root node
  		  forTaskNodeAddChild(ROOT_NODE_ID, newTasks(i))
  		}
  		else {
  		  for (parentTasksId <- parentTasksIds) {
  			forTaskNodeAddChild(parentTasksId, newTasks(i))
  		}
  	// println(stage.id + ": " + "added task " + newTasks(i) + " part " + i)
  	  }
    }
  }

  def forTaskNodeAddChild(parentTaskNodeId: Long, childTaskNodeId: Long) {
  	val parentTaskNode: TaskNode = taskNodes.get(parentTaskNodeId).get
  	parentTaskNode.addChild(childTaskNodeId)
  	val childtaskNode: TaskNode = taskNodes.get(childTaskNodeId).get
  	if (parentTaskNodeId == ROOT_NODE_ID)
  		childtaskNode.sumOfBytesIn = childtaskNode.outputForPartition.sum
  	// else if (childTaskNodeId == END_NODE_ID)
  	// 	childtaskNode.sumOfBytesIn = Long.MaxValue
  	else 
  		childtaskNode.sumOfBytesIn += parentTaskNode.outputForPartition(childtaskNode.partitionId)

  }

  // called iff there was a stage already processed with the same shuffle dependency
  // maps the prev stageId's tasks to the new stageId so that task dependencies can be made from
  // this new stage.
  def addRepeatedShuffleStage(shuffleId: Int, stageId: Int){ 
  	stageIdToTasks += stageId -> stageIdToTasks.get( shuffleIdToStageId.get(shuffleId).get ).get
  }

  // loging tool to show how much memory is moved between tasks
  def getMemFromParentToChildString(parent: Long, child: Long): String = {
	return "data sent from task " + parent + " to task " + child + " is " + getMemFromParentToChild(parent, child)
  }
  def getMemFromParentToChild(parent: Long, child: Long): Long = {
  	if (parent == child)
  		return 0
  	else if (parent == ROOT_NODE_ID)
  		return taskNodes.get(child).get.outputForPartition.sum
  	return taskNodes.get(parent).get.getOutputForPartition( taskNodes.get(child).get.partitionId )
  }
  // loging tool to show how much memory is moved between each and every tasks with dep
  def printTaskDataDependencies() = {
  	val rooTaskNode: TaskNode = taskNodes.get(ROOT_NODE_ID).get
  	for (childTaskId <- rooTaskNode.childTaskIds)
  	  println(getMemFromParentToChildString(ROOT_NODE_ID, childTaskId))
  	for (parentTaskId <- 0 to (taskNodes.size-3)) { // -3 because of the dummy first and last tasks
  	  val parentTaskNode: TaskNode = taskNodes.get(parentTaskId).get
  		for (childTaskId <- parentTaskNode.childTaskIds)
  		  println(getMemFromParentToChildString(parentTaskId, childTaskId))
  	}
  }

  // prints so of the graph info and calls the toString of each node
  override def toString(): String = {
  	for ((task, executorId) <- taskToExecutor) {
  		val taskId: Long = getTaskId(task)
  		taskNodes.get(taskId).get.executorId = executorId
  	}
  	getMemTransferBetweenExecutors()
  	// printStageTaskGroupings()
  	// dfs(ROOT_NODE_ID, END_NODE_ID)

  	var toReturn: String = ""
  	toReturn += "TaskGraph has " + stageIdToTasks.size + " stages and " + (taskNodes.size-3).toString + " tasks\n"
  	toReturn += "\t" + taskNodes(ROOT_NODE_ID).toString + "\n"
  	for (i <- 0 to (taskNodes.size-3)) // -3 because of the dummy first and last tasks
  	  toReturn += "\t" + taskNodes(i).toString + "\n"
  	toReturn += "\t" + taskNodes(END_NODE_ID).toString + "\n"
  	toReturn
  }

  // prints stageId's and the tasks within each
  def printStageTaskGroupings() {
    for ((k,v) <- stageIdToTasks) {
    	print(k + " ")
    	for (t <- v)
    	  print(t + " ")
    	println()
    }
  }

  // prints every path from source to sink
  def dfs(sourceId: Long, sinkId: Long): Boolean = {
  	println("DFS from " + sourceId + " to " + sinkId)
  	var toReturn: Boolean = false
    val stack: Stack[(Long, Boolean)] = new Stack[(Long, Boolean)]()
    val path: Stack[Long] = new Stack[Long]()

    stack.push((sourceId, false))

    while (!stack.isEmpty) {
    	val head = stack.pop
    	if (head._2 == true) {// if visited already
    		if (head._1 == sinkId) {
    			toReturn = true
	    		println("\tpath found: " + path.reverse.toString)
	    	}
    		path.pop
    	}
    	else {
    		stack.push((head._1, true))
	    	path.push(head._1)
	    	val taskNode: TaskNode = taskNodes.get( head._1 ).get
	    	for (childTaskId <- taskNode.childTaskIds) {
	    		stack.push((childTaskId, false))
	    	}
    	}
    }
    if (!toReturn)
    	println("\tNo path found")
    toReturn
  }

  // performs a complete dfs from root to end
  def dfs(): Boolean = dfs(ROOT_NODE_ID, END_NODE_ID)


  // removes edge from parent to child
  def removeEdge(parentTaskId: Long, childTaskId: Long) {
  	taskNodes.get(parentTaskId).get.removeChild(childTaskId)
  }


  def minSplit() {
  	for ((key,taskNode) <- taskNodes)
  		taskNode.status = 0
	val cuts: RecusiveStructure = this.getMinCutOfNode(-Long.MaxValue)
	println()
	if (cuts.childCutList.isEmpty) {
		println("SOMTHING WRONG")
	}
	for (cut <- cuts.childCutList)
		println ("cut " + cut._1 + " to " + cut._2 + " (" + cut._3 + ")")
	println()
  }

  def getMinCutOfNode(parentTaskId: Long): RecusiveStructure = {
  	
  	val parentTask: TaskNode = taskNodes.get(parentTaskId).get
  	val toReturn: RecusiveStructure = new RecusiveStructure()
  	
  	for (childTaskId <- parentTask.childTaskIds) {
  		val childtaskNode: TaskNode = taskNodes.get(childTaskId).get
  		val memFromParentToChild: Long = getMemFromParentToChild(parentTaskId, childTaskId)

  		// handle if the node has been visited
  		if (childtaskNode.status == 2) { // edge in was cut before cut this edge 
  			toReturn.childCutList += new Tuple3(parentTaskId, childTaskId, memFromParentToChild)
  		}
  		else if (childtaskNode.status == 1) { // edge in was kept before keep this edge 
  			// do nothing, do not recurse, do not cut
  		}
  		else if (childtaskNode.status == 0) { // first visit to node perform recusion
			val childRS: RecusiveStructure = getMinCutOfNode(childTaskId)
			var childEdgeListSum: Double = 0
	  		childRS.childCutList.foreach(childEdgeListSum += _._3)

		  	val allMemIntoChild: Long = taskNodes.get(childTaskId).get.sumOfBytesIn
	  		val memIntoChildFromOtherParents = allMemIntoChild - memFromParentToChild
	  		// println("\tmem from: " + parentTaskId + " to " + childTaskId + " = " + memFromParentToChild)


	  		if ( childTaskId != END_NODE_ID &&
	  			allMemIntoChild + childRS.otherInputs > childEdgeListSum) {// put prefrence on cutting lower in the graph
		  		// println("\tchosing cuts beyond: " + parentTaskId + " to " + childTaskId + " (" + allMemIntoChild + " vs " + childEdgeListSum + ")")
  				toReturn.childCutList ++= childRS.childCutList
  				toReturn.otherInputs += memIntoChildFromOtherParents
  				childtaskNode.status == 1
	  		}
	  		else {
	  			// println("\t\tchoosing cut at: " + parentTaskId + " to " + childTaskId + " becaue allMemIntoChild (" + allMemIntoChild + ") < childEdgeListSum " + childEdgeListSum)
	  			toReturn.childCutList += new Tuple3(parentTaskId, childTaskId, memFromParentToChild)
	  			childtaskNode.status = 2
	  		}
	  	}
	  }
	return toReturn
  }

  // calculates the memory passed between executors
  // does not include root to first generation tasks and result task to end task mem movement
  // because we did not really define them 
  def getMemTransferBetweenExecutors() {
  	for ((k,v) <- taskNodes)
  		v.status = 0 // indicated not visited
  	var status: Int = 0 // 0 not visited, 1 kept, 2 cut
	/* perform a dfs 
	 * if the child node does not have the same executor then add the mem sent to cross bandwidth
	 */
    val stack: Stack[Long] = new Stack[Long]()
    var memTransfer: Double = 0

    stack.push(ROOT_NODE_ID)

    while (!stack.isEmpty) {
    	val parentTaskId: Long = stack.pop
	    val parentTaskNode: TaskNode = taskNodes.get(parentTaskId).get
	    parentTaskNode.status = 1 // indicate visited so that we do recuse multiple times down
    	for (childTaskId <- parentTaskNode.childTaskIds) {
    		if (childTaskId != END_NODE_ID) {
    			val childTaskNode: TaskNode = taskNodes.get(childTaskId).get
    			if (childTaskNode.status == 0)
    				stack.push(childTaskId)
    			if (parentTaskId != ROOT_NODE_ID && 
    				childTaskNode.executorId != parentTaskNode.executorId) {
    				println("\tadding " + parentTaskId + " to " + childTaskId)
    				memTransfer += getMemFromParentToChild(parentTaskId, childTaskId)
    			}
	    	}
	    }
    }  	
    println("memTransfer == " + memTransfer)
  }

  def getTaskId(task: Task[_]): Long = {
  	val stageTasks: Array[Long] = stageIdToTasks.get(task.stageId).get.toArray
  	return stageTasks(task.partitionId)
  }

  def taskIsForThisDC(execId: String , taskId: Long): Boolean = {
  	println("\tChecking if executor (" + execId + ") and task (" + taskId + ") align for DC")
  	return true
  } 
}

class RecusiveStructure() {
	val childCutList: TreeSet[(Long,Long,Long)] = new TreeSet[(Long,Long,Long)]()
	var otherInputs: Long = 0
}