package uk.co.odinconsultants.spark.fp.zoio.actions

import org.apache.spark.rdd.RDD
import scalaz.Scalaz._
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation
import uk.co.odinconsultants.spark.fp.zoio.actions.Actions.{ReaderTEither, _}

object Top {

  def top(n: Int, x: RDD[(String, Int)]): Map[String, Int] = x.takeOrdered(n)(scala.Ordering.by(-_._2)).toMap

  def topT(n: Int) = ReaderTEither[RDD[(String, Int)], Map[String, Int]] { rdd =>
    toMonad(top(n, rdd))
  }

  def topWordsOp(op: SparkOperation[RDD[(String, Int)]])(n: Int): SparkOperation[Map[String, Int]] = {
    op.map(top(n, _))
  }

}
