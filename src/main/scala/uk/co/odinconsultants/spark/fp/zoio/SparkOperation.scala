package uk.co.odinconsultants.spark.fp.zoio

import org.apache.spark.SparkContext
import scalaz._

/**
  * Stolen from http://www.stephenzoio.com/creating-composable-data-pipelines-spark/
  */
sealed trait SparkOperation[+A] {
  // executes the transformations
  def run(ctx: SparkContext): A

  // note you don't have to implement these (as Stephen Zoio indeed says) but you do need:
  // import scalaz.syntax.functor._
  // anywhere that needs map/flatMap instead.
  // enables chaining pipelines, for comprehensions, etc.
//  def map[B](f: A ⇒ B): SparkOperation[B] =
//    SparkOperation { ctx ⇒ f(this.run(ctx)) }
//  def flatMap[B](f: A ⇒ SparkOperation[B]): SparkOperation[B] =
//    SparkOperation { ctx ⇒ f(this.run(ctx)).run(ctx) }
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