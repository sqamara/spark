import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Numbers {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Numbers Application")
        val sc = new SparkContext(conf)
        
        val rdd0 = sc.parallelize(Array(0,1,2,3,4,5,6,7,8,9), 2).map(v => (v%5, 1)).reduceByKey(_+_).map(v => (v._1%3, v._2)).reduceByKey(_+_)
        println(rdd0.collect())
    }
}
