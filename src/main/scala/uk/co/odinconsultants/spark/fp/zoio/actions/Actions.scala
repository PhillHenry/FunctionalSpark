package uk.co.odinconsultants.spark.fp.zoio.actions

import org.apache.spark.rdd.RDD
import scalaz.{Kleisli, KleisliInstances, ReaderT, \/}
import scalaz.Scalaz._
import uk.co.odinconsultants.spark.fp.zoio.SparkOperation

package object Actions {

  type MyMonad[T] = \/[String, T]

  def toMonad[T](t: => T): MyMonad[T] = t.right[String]

  // see http://eed3si9n.com/learning-scalaz/Monad+transformers.html
  type ReaderTEither[A, B] = ReaderT[MyMonad, A, B]
  object ReaderTEither extends KleisliInstances {
    def apply[A, B](f: A => MyMonad[B]): ReaderTEither[A, B] = Kleisli(f)
  }

  type SparkMonadTransformer[T, U] =  ReaderTEither[SparkOperation[RDD[T]], SparkOperation[RDD[U]]]

  type SparkOpRdd[T] = SparkOperation[RDD[T]]
}
