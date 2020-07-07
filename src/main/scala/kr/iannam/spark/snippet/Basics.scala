package kr.iannam.spark.snippet

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._

/**
 * @author 남상욱 (ian.nam@navercorp.com)
 */
object Basics {
    case class Quotes(word: String, count: Double)
    case class User(age: Int, income: Double, score: Double)
    case class Stats(max: Double, min: Double, mean: Double, stddev: Double)
    case class UserStats(age: Stats, income: Stats, score: Stats)

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

    def groupedTopN(input: RDD[Quotes], n: Int): RDD[Quotes] = {
        input.groupBy(_.word).values.flatMap(_.toList.sortBy(-_.count).take(n)) // case 1
        input.keyBy(_.word).topByKey(n)(Ordering.by(_.count)).flatMap(_._2) // case 2
    }

    def countDistinct(input: RDD[Quotes]): Long = {
        input.map(_.word).distinct().count()
    }

    def countApprox(input: RDD[Quotes]): Long = {
        input.map(_.word).countApproxDistinct()
    }
}
