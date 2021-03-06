package uk.co.odinconsultants.spark.fp.zoio.actions

import org.apache.spark.rdd.RDD
import scalaz.Scalaz._
import uk.co.odinconsultants.spark.fp.zoio._
import uk.co.odinconsultants.spark.fp.zoio.actions.Actions._

object Words {

  def words(lines: RDD[String]): RDD[String] = lines.flatMap { line => line.split("\\W+") }
    .map(_.toLowerCase)
    .filter(!_.isEmpty)

  val wordsT = ReaderTEither[RDD[String], RDD[String]] { rdd => toMonad(words(rdd)) }

  def wordsOp(op: SparkOperation[RDD[String]]): SparkOperation[RDD[String]] = for (lines <- op) yield words(lines)

}
