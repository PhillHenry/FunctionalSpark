package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.{ SparkConf, SparkContext }

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
object WordCountMain extends WordCountPipeline {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("WordCount"))
    val topFn:        Int => SparkOperation[Map[String, Int]] = topWordsOp(countOp(wordsOp(linesOp)))
    val topWordsMap:  Map[String, Int]                        = topFn(100).run(sc)
    println(topWordsMap.mkString("\n"))
    sc.stop()
  }

}
