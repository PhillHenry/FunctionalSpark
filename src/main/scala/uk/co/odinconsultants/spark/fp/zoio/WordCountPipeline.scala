package uk.co.odinconsultants.spark.fp.zoio

import SparkOperation._
import org.apache.spark.rdd.RDD
import scalaz.Monad

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
trait WordCountPipeline {

  import scalaz.syntax.functor._

  // initial SparkOperation created using companion object
  def linesOp = SparkOperation { sparkContext =>
//    sparkContext.textFile("/TXT/*")
    sparkContext.parallelize((1 to 100).map(i => s"Line $i"))
  }

  // after that we often just need map / flatMap
  def wordsOp: SparkOperation[RDD[String]] = for (lines <- linesOp) yield {
    lines.flatMap { line => line.split("\\W+") }
      .map(_.toLowerCase)
      .filter(!_.isEmpty)
  }

  def countOp: SparkOperation[RDD[(String, Int)]] = for (words <- wordsOp)
    yield words.map((_, 1)).reduceByKey(_ + _)

  def topWordsOp(n: Int): SparkOperation[Map[String, Int]] =
    countOp.map(_.takeOrdered(n)(Ordering.by(-_._2)).toMap)
}
