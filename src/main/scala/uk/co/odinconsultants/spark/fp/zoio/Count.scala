package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.rdd.RDD
import scalaz.Scalaz._
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation._

object Count {

  val countT: SparkMonadTransformer[String, (String, Int)]
  = ReaderTEither[SparkOpRdd[String], SparkOpRdd[(String, Int)]] { op: SparkOpRdd[String] => toMonad(countOp(op)) }

  def count(words: RDD[String]): RDD[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

  def countOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[(String, Int)]] = for (words <- op) yield count(words)

}
