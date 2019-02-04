package uk.co.odinconsultants.spark.frameless

import org.apache.spark.sql.SparkSession

class MyClass(x: String)

object DeleteMe {

  def main(args: Array[String]): Unit = {
    println("Hello and goodbye")
  }

  def ideallyThisWouldNotCompile(spark: SparkSession): Unit = {
    import spark.implicits._

    val test = Seq((1, new MyClass("1")), (2, new MyClass("2")))
    val testDf = test.toDF // this should go ka-boom. TODO add Frameless
  }

}
