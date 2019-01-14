package uk.co.odinconsultants.spark.fp.zoio.actions

import org.apache.spark.rdd.RDD
import scalaz.Scalaz._
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation._
import uk.co.odinconsultants.spark.fp.zoio._
import uk.co.odinconsultants.spark.fp.zoio.actions.Actions._

object Count {

  def count(words: RDD[String]): RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

  val countT = ReaderTEither[RDD[String], RDD[(String, Int)]] { rdd => toMonad(count(rdd)) }

  def countOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[(String, Int)]] = for (words <- op) yield count(words)

}
