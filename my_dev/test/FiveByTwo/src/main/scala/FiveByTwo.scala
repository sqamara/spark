/* FiveByTwo.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Five by Two Application")
    val sc = new SparkContext(conf)
    val r = scala.util.Random
    r.setSeed(System.nanoTime())
    val array = Seq.fill(scala.math.pow(10, args(0).toInt).toInt)(r.nextInt) toArray  
    
    // first run for ref and trace
    val t0 = System.nanoTime()
    val rdd0 = ( sc.parallelize(array, 2).map(v => (v%11, 1))
        .reduceByKey(_+_).map(v => (v._1%7, v._2))
        .reduceByKey(_+_).map(v => (v._1%5, v._2))
        .reduceByKey(_+_).map(v => (v._1%3, v._2))
        .reduceByKey(_+_) )
    rdd0.collect()
    val t1 = System.nanoTime()
    println("Ref/Trace time: " + (t1 - t0) + " ns")

    // second for applying executor decisions
    val t2 = System.nanoTime()
    val rdd1 = ( sc.parallelize(array, 2).map(v => (v%11, 1))
        .reduceByKey(_+_).map(v => (v._1%7, v._2))
        .reduceByKey(_+_).map(v => (v._1%5, v._2))
        .reduceByKey(_+_).map(v => (v._1%3, v._2))
        .reduceByKey(_+_) )
    rdd1.collect()
    val t3 = System.nanoTime()
    println("EChoice time: " + (t3 - t2) + " ns")

   
    }
}
