/* WordCount.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    
    // first run for ref and trace
    val t0 = System.nanoTime()
    val textFile = sc.textFile("README.md")
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.collect
    val t1 = System.nanoTime()
    println("Ref/Trace time: " + (t1 - t0) + "ns")

    // second for applying executor decisions
    val t2 = System.nanoTime()
    val textFile1 = sc.textFile("README.md")
    val counts1 = textFile1.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts1.collect
    val t3 = System.nanoTime()
    println("EChoice time: " + (t3 - t2) + "ns")

   
    }
}
