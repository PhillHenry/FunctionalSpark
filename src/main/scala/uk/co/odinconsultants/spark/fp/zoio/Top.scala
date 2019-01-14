package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.rdd.RDD
import scalaz.Scalaz._
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation._

object Top {

  def topT(n: Int): ReaderTEither[SparkOperation[RDD[(String, Int)]], SparkOperation[Map[String, Int]]]
  = ReaderTEither[SparkOpRdd[(String, Int)], SparkOperation[Map[String, Int]]] { op: SparkOpRdd[(String, Int)] =>
    toMonad(topWordsOp(op)(n))
  }

  def topWordsOp(op: SparkOperation[RDD[(String, Int)]])(n: Int): SparkOperation[Map[String, Int]] = {
    op.map(_.takeOrdered(n)(scala.Ordering.by(-_._2)).toMap)
  }

}
