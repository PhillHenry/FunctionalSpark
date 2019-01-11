package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.{SparkConf, SparkContext}
import scalaz.syntax.monad._
import uk.co.odinconsultants.spark.fp.zoio.WordCountMain.words

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
object WordCountMain extends WordCountPipeline {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("WordCount"))

    val topFn:        Int => SparkOperation[Map[String, Int]] = topWordsOp(countOp(wordsOp(linesOp)))
    val topWordsMap:  Map[String, Int]                        = topFn(100).run(sc)

//    val topMonad:  SparkOperation[Map[String, Int]] = for {
//      lines   <- linesOp
//      words   <- words(lines)
//      counts  <- count(words)
//      top     <- topWordsOp(counts)
//    } yield top(10)
//    val topWordsMap:  Map[String, Int] = topMonad.run(sc)


    println(topWordsMap.mkString("\n"))
    sc.stop()
  }

}
