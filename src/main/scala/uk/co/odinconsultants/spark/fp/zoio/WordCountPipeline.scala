package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.rdd.RDD
import scalaz.{Kleisli, KleisliInstances, ReaderT}
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
trait WordCountPipeline {

  // see http://eed3si9n.com/learning-scalaz/Monad+transformers.html
  type ReaderTSparkOperation[A, B] = ReaderT[SparkOperation, A, B]
  object ReaderTSparkOperation extends KleisliInstances  {
    def apply[A, B](f: A => SparkOperation[B]): ReaderTSparkOperation[A, B] = Kleisli(f)
  }

  import scalaz.syntax.monad._

  def linesOp: SparkOperation[RDD[String]] = SparkOperation { sparkContext =>
    sparkContext.parallelize((1 to 100).map(i => s"Line $i"))
  }

  def words(lines: RDD[String]): RDD[String] = lines.flatMap { line => line.split("\\W+") }
    .map(_.toLowerCase)
    .filter(!_.isEmpty)

//  def wordsT(op: SparkOperation[RDD[String]]): ReaderTSparkOperation[RDD[String], RDD[String]] = ReaderTSparkOperation[RDD[String], RDD[String]] { rdd: RDD[String] => words(rdd) }

  def wordsOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[String]] = for (lines <- op) yield words(lines)

  def count(words: RDD[String]): RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

  def countOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[(String, Int)]] = for (words <- op) yield count(words)

  def topWordsOp(op: SparkOperation[RDD[(String, Int)]])(n: Int): SparkOperation[Map[String, Int]] =
    op.map(_.takeOrdered(n)(Ordering.by(-_._2)).toMap)
}
