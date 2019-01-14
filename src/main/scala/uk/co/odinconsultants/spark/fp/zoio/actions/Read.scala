package uk.co.odinconsultants.spark.fp.zoio.actions

import org.apache.spark.rdd.RDD
import uk.co.odinconsultants.spark.fp.zoio.{ReaderTEither, SparkOpRdd, SparkOperation, toMonad}

object Read {

  val linesT: ReaderTEither[Unit, SparkOperation[RDD[String]]]
  = ReaderTEither[Unit, SparkOpRdd[String]] { _ =>
    toMonad(linesOp)
  }

  def linesOp: SparkOperation[RDD[String]] = SparkOperation { sparkContext =>
    sparkContext.parallelize((1 to 100).map(i => s"Line $i"))
  }

}
