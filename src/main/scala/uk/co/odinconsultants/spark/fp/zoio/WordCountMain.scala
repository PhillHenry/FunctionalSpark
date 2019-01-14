package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.{SparkConf, SparkContext}
import uk.co.odinconsultants.spark.fp.zoio.actions.Count._
import uk.co.odinconsultants.spark.fp.zoio.actions.Read._
import uk.co.odinconsultants.spark.fp.zoio.actions.Top._
import uk.co.odinconsultants.spark.fp.zoio.actions.Words._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
object WordCountMain {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("WordCount"))

    val topFn:        Int => SparkOperation[Map[String, Int]] = topWordsOp(countOp(wordsOp(linesOp)))
    val topWordsMap:  Map[String, Int]                        = topFn(100).run(sc)

    val x = for {
      ls <- linesT(sc)
      ws <- wordsT(ls)
      cs <- countT(ws)
      ts <- topT(10)(cs)
    } yield ts

    println(x.map(_.mkString("\n")))

    sc.stop()
  }

}
