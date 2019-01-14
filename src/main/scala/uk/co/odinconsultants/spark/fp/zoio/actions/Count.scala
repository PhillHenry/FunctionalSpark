package uk.co.odinconsultants.spark.fp.zoio.actions

import org.apache.spark.rdd.RDD
import uk.co.odinconsultants.spark.fp.zoio._
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation._
import uk.co.odinconsultants.spark.fp.zoio.actions.Actions._
import scalaz._
import scalaz.Scalaz._

object Count {

  val countT: SparkMonadTransformer[String, (String, Int)]
  = ReaderTEither[SparkOpRdd[String], SparkOpRdd[(String, Int)]] { op: SparkOpRdd[String] => toMonad(countOp(op)) }

  def count(words: RDD[String]): RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

  def countOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[(String, Int)]] = for (words <- op) yield count(words)

}
