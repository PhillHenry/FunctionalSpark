package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.rdd.RDD
import scalaz.{Kleisli, KleisliInstances, ReaderT}
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
trait WordCountPipeline {

  // see http://eed3si9n.com/learning-scalaz/Monad+transformers.html
  type ReaderTOption[A, B] = ReaderT[Option, A, B]
  object ReaderTOption extends KleisliInstances {
    def apply[A, B](f: A => Option[B]): ReaderTOption[A, B] = Kleisli(f)
  }

  import scalaz.syntax.monad._

  def linesOp: SparkOperation[RDD[String]] = SparkOperation { sparkContext =>
    sparkContext.parallelize((1 to 100).map(i => s"Line $i"))
  }

  def words(lines: RDD[String]): RDD[String] = lines.flatMap { line => line.split("\\W+") }
    .map(_.toLowerCase)
    .filter(!_.isEmpty)

  def wordsT: ReaderTOption[SparkOperation[RDD[String]], SparkOperation[RDD[String]]]
    = ReaderTOption[SparkOperation[RDD[String]], SparkOperation[RDD[String]]] { op: SparkOperation[RDD[String]] => Option(wordsOp(op)) }

  def wordsOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[String]] = for (lines <- op) yield words(lines)

  def count(words: RDD[String]): RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

  def countOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[(String, Int)]] = for (words <- op) yield count(words)

  def topWordsOp(op: SparkOperation[RDD[(String, Int)]])(n: Int): SparkOperation[Map[String, Int]] =
    op.map(_.takeOrdered(n)(Ordering.by(-_._2)).toMap)
}
