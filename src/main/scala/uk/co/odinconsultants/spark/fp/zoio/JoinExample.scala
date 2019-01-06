package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.rdd.RDD
import scalaz.syntax.bind._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
object JoinExample {
  trait K
  trait A
  trait B

  val opA: SparkOperation[RDD[(K,A)]] = ???
  val opB: SparkOperation[RDD[(K,B)]] = ???

  val joinedOp: SparkOperation[RDD[(K,(A,B))]] =
    (opA |@| opB)((rddA,rddB) => rddA.join(rddB))
}
