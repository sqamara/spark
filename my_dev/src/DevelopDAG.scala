import scala.collection.mutable.ListBuffer
object DevelopDAG {
	
	val r = scala.util.Random
	
	def main(args: Array[String]): Unit = {
		r.setSeed(0)
		val nodeCount: Int = 10
		val allNodes: Array[Node] = new Array[Node](nodeCount)
		for (i <- 0 to nodeCount-1) {
			allNodes(i) = randomNodeWithDependencies(i)
		}
		for (i <- 0 to nodeCount-1) {
			println(allNodes(i))
		}
		val dag: Graph = new Graph(allNodes.toList)

		println(dag)
	}

	def randomNodeWithDependencies(nodeID: Int): Node = {
		// let max children be 3
		val parents: ListBuffer[Int] = new ListBuffer()
		if (nodeID > 0) {
			for(i <- 0 to r.nextInt(4)) {
				val randomParent: Int = r.nextInt(nodeID)
				if (!parents.contains(randomParent))
					parents += randomParent
			}
		}
		new Node(nodeID, parents.toList)
		// random amount of children
	}

	class Graph(var nodes: List[Node]) {
		// generate children for all the nodes
		val root: Node = nodes.maxBy(_.nodeID)
		for (node <- nodes){
			for (parent <- node.parents) {
				// find the parent node and add this as its child
				nodes.find(p => p.nodeID == parent).get.addChild(node.nodeID)
			}
		}

		override def toString(): String = {
			var toReturn: String = new String()
			for (node <- nodes) {
				toReturn += node.toString() + "\n"
			}
			toReturn += "Root = " + root.nodeID + "\n"
			toReturn
		}
	}
	class Node(val nodeID: Int, val parents: List[Int]) {
		var children: List[Int] = List[Int]()
		def addChild(child: Int) {
			children = child :: children 
		}
		override def toString(): String = nodeID + " Parent" + parents + " Child" + children;
	}
	// class Edge() {
	// }
}
