/* PageRank.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PageRank {
  def main(args: Array[String]) {
  	if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)
    val r = scala.util.Random
    r.setSeed(System.nanoTime())

    val iters = if (args.length > 1) args(1).toInt else 10
    // first run for ref and trace
    val t0 = System.nanoTime()
    sparkPageRank(sc, args(0), iters)
    val t1 = System.nanoTime()
    println("norm time: " + (t1 - t0) + " ns")

    // second for applying executor decisions
    val t2 = System.nanoTime()
    sparkPageRank(sc, args(0), iters)
    val t3 = System.nanoTime()
    println("choice time: " + (t3 - t2) + " ns")

   
    }

  def sparkPageRank(sc: SparkContext, filename: String, iters: Int) {
  	val lines = sc.textFile(filename)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
  }
}
