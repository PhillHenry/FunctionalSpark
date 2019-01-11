package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.rdd.RDD
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation._

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
  def wordsOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[String]] = for (lines <- op) yield {
    lines.flatMap { line => line.split("\\W+") }
      .map(_.toLowerCase)
      .filter(!_.isEmpty)
  }

  def countOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[(String, Int)]] = for (words <- op)
    yield words.map((_, 1)).reduceByKey(_ + _)

  def topWordsOp(op: SparkOperation[RDD[(String, Int)]])(n: Int): SparkOperation[Map[String, Int]] =
    op.map(_.takeOrdered(n)(Ordering.by(-_._2)).toMap)
}
