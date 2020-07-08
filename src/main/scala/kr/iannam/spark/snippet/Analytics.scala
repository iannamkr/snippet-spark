package kr.iannam.spark.snippet

import org.apache.spark.rdd.RDD

/**
 * @author 남상욱 (ian.nam@navercorp.com)
 */
object Analytics {
    /**
     * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPageRank.scala
     * @param input
     * @param dampingFactor
     */
    def pageRank1(input: RDD[(String, String)], dampingFactor: Double = 0.85, iters: Int = 10): RDD[(String, Double)] = {
        val links = input.distinct().groupByKey().cache()
        var ranks = links.mapValues(_ => 1.0)
        for (i <- 1 to iters) {
            val contribs = links.join(ranks).values.flatMap {
                case (urls, rank) =>
                    val size = urls.size
                    urls.map(url => (url, rank / size))
            }
            ranks = contribs
                    .reduceByKey(_ + _)
                    .mapValues(1 - dampingFactor + dampingFactor * _)
        }
        ranks
    }
}
