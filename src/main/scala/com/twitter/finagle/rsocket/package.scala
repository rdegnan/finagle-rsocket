package com.twitter.finagle

import com.twitter.util.{Future, Promise}
import reactor.core.publisher.Mono

package object rsocket {
  implicit class MonoVoidToTwitterFuture[T](mono: Mono[Void]) {
    def toTwitterFuture: Future[Unit] = {
      val promise = Promise[Unit]()
      mono.toFuture.handle[Unit]((_, throwable) => {
        Option(throwable) match {
          case Some(_) => promise.setException(throwable)
          case None    => promise.setDone()
        }
        ()
      })
      promise
    }
  }

  implicit class MonoToTwitterFuture[T](mono: Mono[T]) {
    def toTwitterFuture: Future[T] = {
      val promise = Promise[T]()
      mono.toFuture.handle[Unit]((value, throwable) => {
        Option(throwable) match {
          case Some(_) => promise.setException(throwable)
          case None    => promise.setValue(value)
        }
        ()
      })
      promise
    }
  }
}
