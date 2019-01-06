package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.SparkContext
import scalaz._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
sealed trait SparkOperation[+A] {
  // executes the transformations
  def run(ctx: SparkContext): A

  // enables chaining pipelines, for comprehensions, etc.
  def map[B](f: A => B): SparkOperation[B]
  def flatMap[B](f: A => SparkOperation[B]): SparkOperation[B]
}

object SparkOperation {
  def apply[A](f: SparkContext => A): SparkOperation[A] = new SparkOperation[A] {
    override def run(ctx: SparkContext): A = f(ctx)
  }

  implicit val monad = new Monad[SparkOperation] {
    override def bind[A, B](fa: SparkOperation[A])(f: A ⇒ SparkOperation[B]): SparkOperation[B] =
      SparkOperation(ctx ⇒ f(fa.run(ctx)).run(ctx))

    override def point[A](a: ⇒ A): SparkOperation[A] = SparkOperation(_ ⇒ a)
  }
}