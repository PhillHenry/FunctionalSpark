package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.rdd.RDD
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
trait WordCountPipeline {

  import scalaz.syntax.monad._

  def linesOp: SparkOperation[RDD[String]] = SparkOperation { sparkContext =>
    sparkContext.parallelize((1 to 100).map(i => s"Line $i"))
  }

  def words(lines: RDD[String]): RDD[String] = lines.flatMap { line => line.split("\\W+") }
    .map(_.toLowerCase)
    .filter(!_.isEmpty)

  def wordsOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[String]] = for (lines <- op) yield words(lines)

  def count(words: RDD[String]): RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

  def countOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[(String, Int)]] = for (words <- op) yield count(words)

  def topWordsOp(op: SparkOperation[RDD[(String, Int)]])(n: Int): SparkOperation[Map[String, Int]] =
    op.map(_.takeOrdered(n)(Ordering.by(-_._2)).toMap)
}
