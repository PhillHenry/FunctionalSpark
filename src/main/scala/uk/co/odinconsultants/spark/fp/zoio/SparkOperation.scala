package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.SparkContext
import scalaz._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
sealed trait SparkOperation[+A] {
  def run(ctx: SparkContext): A
}

object SparkOperation {
  def apply[A](f: SparkContext => A): SparkOperation[A] = new SparkOperation[A] {
    override def run(ctx: SparkContext): A = f(ctx)
  }

  implicit val monad: Monad[SparkOperation] = new Monad[SparkOperation] {
    override def bind[A, B](fa: SparkOperation[A])(f: A ⇒ SparkOperation[B]): SparkOperation[B] =
      SparkOperation(ctx ⇒ f(fa.run(ctx)).run(ctx))

    override def point[A](a: ⇒ A): SparkOperation[A] = SparkOperation(_ ⇒ a)
  }
}