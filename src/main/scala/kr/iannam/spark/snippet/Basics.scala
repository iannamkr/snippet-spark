package kr.iannam.spark.snippet

import com.twitter.algebird.{BloomFilter, BloomFilterAggregator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._

/**
 * @author 남상욱 (ian.nam@navercorp.com)
 */
object Basics {
    case class Simple(value: String)
    case class Quotes(word: String, count: Double)
    case class User(age: Int, income: Double, score: Double)

    def sum(input: RDD[Quotes]): RDD[(String, Double)] = {
        input.map(x => (x.word, x.count)).reduceByKey(_ + _)
    }

    def count(input: RDD[Quotes]): Long = {
        input.count()
    }

    def max1(input: RDD[Quotes]): RDD[Quotes] = {
        input.keyBy(_.word).reduceByKey((x, y) => if(x.count > y.count) x else y).values
    }

    def max2(input: RDD[Quotes]): RDD[Quotes] = {
        input.keyBy(_.word).topByKey(1)(Ordering.by(_.count)).flatMap(_._2)
    }

    def min1(input: RDD[Quotes]): RDD[Quotes] = {
        input.keyBy(_.word).reduceByKey((x, y) => if(x.count < y.count) x else y).values
    }

    def min2(input: RDD[Quotes]): RDD[Quotes] = {
        input.keyBy(_.word).topByKey(1)(Ordering.by(-_.count)).flatMap(_._2)
    }

    def topN(input: RDD[Quotes], n: Int): Seq[Quotes] = {
        input.map(x => (x.word, x.count)).top(n)(Ordering.by(_._2)).map(x => Quotes(x._1, x._2))
    }

    def groupedTopN1(input: RDD[Quotes], n: Int): RDD[Quotes] = {
        input.groupBy(_.word).values.flatMap(_.toList.sortBy(-_.count).take(n))
    }

    def groupedTopN2(input: RDD[Quotes], n: Int): RDD[Quotes] = {
        input.keyBy(_.word).topByKey(n)(Ordering.by(_.count)).flatMap(_._2)
    }

    def countDistinct(input: RDD[Quotes]): Long = {
        input.map(_.word).distinct().count()
    }

    def countApprox(input: RDD[Quotes]): Long = {
        input.map(_.word).countApproxDistinct()
    }

    def bloomFilter(lhs: RDD[String], rhs: RDD[String]): RDD[String] = {
        import com.twitter.algebird.spark._

        val numEntries = lhs.countApprox(timeout = 100, confidence = 0.95F).getFinalValue().mean.toInt
        val width = BloomFilter.optimalWidth(numEntries = numEntries, fpProb = 0.05D).get
        val numHashes = BloomFilter.optimalNumHashes(numEntries, width)
        val bf = rhs.algebird.aggregate(BloomFilterAggregator(numHashes, width))
        lhs.filter(s => bf.contains(s).isTrue)
    }

    @transient lazy val sf = new SparkConf().setMaster("local[*]").setAppName("spark-snippet")
    @transient lazy val sc = new SparkContext(sf)
    def main(args: Array[String]): Unit = {

        val lhs = (1 to 10000000).toList.map(_.toString)
        val rhs = (1000 to 1100).toList.map(_.toString)

        bloomFilter(sc.parallelize(lhs), sc.parallelize(rhs)).foreach(println)
    }
}
