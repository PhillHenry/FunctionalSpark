package uk.co.odinconsultants.spark.fp

import org.apache.spark.sql.{KeyValueGroupedDataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object LeftOuterMain {
  case class Foo(k:String)
  case class Bar(k:String, b:Boolean)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.Dataset

//    val df = spark.sparkContext.parallelize(Seq(1)).toDF()
//    import df.sqlContext.implicits._

//    val dsFoo = List(Foo("a"), Foo("x")).toDF("k")
//    val dsBar = List(Bar("a", true), Bar("a", false), Bar("x", true)).toDS()

    val dsFoo:Dataset[Foo] = spark.createDataset(List(Foo("a"), Foo("x")))
//    val dsBar:Dataset[Bar] = spark.createDataset(List(Bar("a", true), Bar("a", false), Bar("x", true)))
    val dsBar:Dataset[Bar] = spark.createDataset(List(Bar("a", true), Bar("a", false))) //note no element to link to Foo("x")

    val dsFooBar: Dataset[(Foo, Bar)] = dsFoo.joinWith(dsBar, dsFoo("k") === dsBar("k"), "leftOuter")

    dsFooBar.show()

//    import dsFoo.sqlContext.implicits._

    val dsGrouped: KeyValueGroupedDataset[Foo, (Foo, Bar)] = dsFooBar.groupByKey(_._1) // we want Foos with all their Bars
    val dsGroupedMapped: KeyValueGroupedDataset[Foo, Bar] =  dsGrouped.mapValues(_._2) //we don't need the Foos in the values because we have them in the keys

    dsGroupedMapped.mapGroups( (foo:Foo, bars:Iterator[Bar]) => foo -> bars.toList).show //Baaaam explodes because (I think) Foo("x") has no Bar
  }

}
