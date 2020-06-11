package com.twitter.finagle

import com.twitter.util.{Future, Promise, Return, Throw}
import java.util.NoSuchElementException
import java.util.concurrent.atomic.AtomicReference
import org.reactivestreams.{Subscriber, Subscription}
import reactor.core.CoreSubscriber
import reactor.core.publisher.{Mono, Operators}

package object rsocket {
  private final class FutureUnitSubscriber extends Promise[Unit] with CoreSubscriber[Void] {
    final private val ref = new AtomicReference[Subscription]

    override def onSubscribe(s: Subscription): Unit = {
      if (Operators.validate(ref.getAndSet(s), s)) s.request(Long.MaxValue)
      else s.cancel()
    }

    override def onNext(t: Void): Unit = {
      if (ref.getAndSet(null) != null) setException(new IndexOutOfBoundsException)
    }

    override def onError(t: Throwable): Unit = {
      if (ref.getAndSet(null) != null) setException(t)
    }

    override def onComplete(): Unit = {
      if (ref.getAndSet(null) != null) setDone()
    }
  }

  private final class FutureSubscriber[T] extends Promise[T] with Subscriber[T] {
    final private val ref = new AtomicReference[Subscription]

    override def onSubscribe(s: Subscription): Unit = {
      if (Operators.validate(ref.getAndSet(s), s)) s.request(Long.MaxValue)
      else s.cancel()
    }

    override def onNext(t: T): Unit = {
      if (ref.getAndSet(null) != null) setValue(t)
    }

    override def onError(t: Throwable): Unit = {
      if (ref.getAndSet(null) != null) setException(t)
    }

    override def onComplete(): Unit = {
      if (ref.getAndSet(null) != null) setException(new NoSuchElementException)
    }
  }

  implicit class MonoVoidToFutureUnit(mono: Mono[Void]) {
    def toTwitterFuture: Future[Unit] = mono.subscribeWith(new FutureUnitSubscriber)
  }

  implicit class MonoToFuture[T](mono: Mono[T]) {
    def toTwitterFuture: Future[T] = mono.subscribeWith(new FutureSubscriber[T])
  }

  private final class MonoVoidFuture[T](val future: Future[Unit]) extends Mono[Void] {
    override def subscribe(actual: CoreSubscriber[_ >: Void]): Unit = {
      val sds = new Operators.MonoSubscriber[Void, Void](actual)
      actual.onSubscribe(sds)

      if (!sds.isCancelled) {
        future.respond { t =>
          if (!sds.isCancelled) {
            t match {
              case Return(_) => sds.onComplete()
              case Throw(e)  => sds.onError(e)
            }
          }
        }
      }
    }
  }

  private final class MonoFuture[T](val future: Future[T]) extends Mono[T] {
    override def subscribe(actual: CoreSubscriber[_ >: T]): Unit = {
      val sds = new Operators.MonoSubscriber[T, T](actual)
      actual.onSubscribe(sds)

      if (!sds.isCancelled) {
        future.respond { t =>
          if (!sds.isCancelled) {
            t match {
              case Return(v) => sds.complete(v)
              case Throw(e)  => sds.onError(e)
            }
          }
        }
      }
    }
  }

  implicit class FutureUnitToMonoVoid(future: Future[Unit]) {
    def toMono: Mono[Void] = new MonoVoidFuture(future)
  }

  implicit class FutureToMono[T](future: Future[T]) {
    def toMono: Mono[T] = new MonoFuture(future)
  }
}
