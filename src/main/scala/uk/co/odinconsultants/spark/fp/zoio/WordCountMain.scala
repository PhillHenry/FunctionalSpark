package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.{SparkConf, SparkContext}
import scalaz.syntax.monad._
import uk.co.odinconsultants.spark.fp.zoio.actions.Words._
import uk.co.odinconsultants.spark.fp.zoio.actions.Top._
import uk.co.odinconsultants.spark.fp.zoio.actions.Count._
import uk.co.odinconsultants.spark.fp.zoio.actions.Read._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
object WordCountMain {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("WordCount"))

    val topFn:        Int => SparkOperation[Map[String, Int]] = topWordsOp(countOp(wordsOp(linesOp)))
    val topWordsMap:  Map[String, Int]                        = topFn(100).run(sc)

//    val topMonad:  SparkOperation[Map[String, Int]] = for {
//      ls    <- linesOp        // we're inside the SparkOperation monad
//      ws    <- words(ls)      // we're inside the RDD
//      cs    <- count(ws)      // Kaboom! This doesn't work because we're flatMapping an RDD at this point
//      top   <- topWordsOp(cs)
//    } yield top(10)
//    val topWordsMap:  Map[String, Int] = topMonad.run(sc)

    /*
    val zoio = for {
      ls <- linesOp   // RDD[String]
      ws <- words(ls) // expects ws to be a SparkOperation
    }  yield ws
*/

    val x = for {
//      t <- topT(10) // Kleisli[function1]
      ls <- linesT
//      ws <- wordsT()
    } yield ls

    println(x)

//    println(topWordsMap.mkString("\n"))
    sc.stop()
  }

}
