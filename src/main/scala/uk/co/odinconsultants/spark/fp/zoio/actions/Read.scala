package uk.co.odinconsultants.spark.fp.zoio.actions

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation
import uk.co.odinconsultants.spark.fp.zoio.actions.Actions._

object Read {

  def read(sc: SparkContext): RDD[String] = sc.parallelize((1 to 100).map(i => s"Line $i"))

  val linesT = ReaderTEither[SparkContext, RDD[String]] { sc =>
    toMonad(read(sc))
  }

  def linesOp: SparkOperation[RDD[String]] = SparkOperation { sparkContext =>
    read(sparkContext)
  }

}
