package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.rdd.RDD
import scalaz.{Kleisli, KleisliInstances, ReaderT}
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation._

import scala.util.Try

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
trait WordCountPipeline {

  def toMonad[T](t: => T): Try[T] = Try(t)

  // see http://eed3si9n.com/learning-scalaz/Monad+transformers.html
  type ReaderTTry[A, B] = ReaderT[Try, A, B]
  object ReaderTTry extends KleisliInstances {
    def apply[A, B](f: A => Try[B]): ReaderTTry[A, B] = Kleisli(f)
  }

  type SparkMonadTransformer[T, U] =  ReaderTTry[SparkOperation[RDD[T]], SparkOperation[RDD[U]]]

  type SparkOpRdd[T] = SparkOperation[RDD[T]]

  import scalaz.syntax.monad._

  val linesT: ReaderTTry[Unit, SparkOperation[RDD[String]]]
    = ReaderTTry[Unit, SparkOpRdd[String]] { x =>
    toMonad(linesOp)
  }

  def linesOp: SparkOperation[RDD[String]] = SparkOperation { sparkContext =>
    sparkContext.parallelize((1 to 100).map(i => s"Line $i"))
  }

  def words(lines: RDD[String]): RDD[String] = lines.flatMap { line => line.split("\\W+") }
    .map(_.toLowerCase)
    .filter(!_.isEmpty)

  val wordsT: SparkMonadTransformer[String, String]
    = ReaderTTry[SparkOpRdd[String], SparkOpRdd[String]] { op: SparkOpRdd[String] => toMonad(wordsOp(op)) }

  def wordsOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[String]] = for (lines <- op) yield words(lines)

  val countT: SparkMonadTransformer[String, (String, Int)]
    = ReaderTTry[SparkOpRdd[String], SparkOpRdd[(String, Int)]] { op: SparkOpRdd[String] => toMonad(countOp(op)) }

  def count(words: RDD[String]): RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

  def countOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[(String, Int)]] = for (words <- op) yield count(words)

  def topT(n: Int): ReaderTTry[SparkOperation[RDD[(String, Int)]], SparkOperation[Map[String, Int]]]
    = ReaderTTry[SparkOpRdd[(String, Int)], SparkOperation[Map[String, Int]]] { op: SparkOpRdd[(String, Int)] =>
    toMonad(topWordsOp(op)(n))
  }

  def topWordsOp(op: SparkOperation[RDD[(String, Int)]])(n: Int): SparkOperation[Map[String, Int]] =
    op.map(_.takeOrdered(n)(Ordering.by(-_._2)).toMap)
}
