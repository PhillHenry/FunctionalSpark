package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.rdd.RDD

object Read {

  val linesT: ReaderTEither[Unit, SparkOperation[RDD[String]]]
  = ReaderTEither[Unit, SparkOpRdd[String]] { _ =>
    toMonad(linesOp)
  }

  def linesOp: SparkOperation[RDD[String]] = SparkOperation { sparkContext =>
    sparkContext.parallelize((1 to 100).map(i => s"Line $i"))
  }

}
