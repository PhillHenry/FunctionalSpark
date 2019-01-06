package uk.co.odinconsultants.spark.fp.zoio

import SparkOperation._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
trait WordCountPipeline {

  // initial SparkOperation created using companion object
  def linesOp = SparkOperation { sparkContext =>
    sparkContext.textFile("/TXT/*")
  }

  // after that we often just need map / flatMap
  def wordsOp = for (lines <- linesOp) yield {
    lines.flatMap { line => line.split("\\W+") }
      .map(_.toLowerCase)
      .filter(!_.isEmpty)
  }

  def countOp = for (words <- wordsOp)
    yield words.map((_, 1)).reduceByKey(_ + _)

  def topWordsOp(n: Int): SparkOperation[Map[String, Int]] =
    countOp.map(_.takeOrdered(n)(Ordering.by(-_._2)).toMap)
}
