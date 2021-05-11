package com.twitter.finagle.rsocket

import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{RSocket, Service}
import com.twitter.util.{Await, Closable, Duration, Future, Futures, Promise}
import io.rsocket.Payload
import io.rsocket.util.DefaultPayload
import java.net.InetSocketAddress
import org.reactivestreams.Publisher
import org.scalatest.FunSuite
import reactor.core.publisher.{Flux, Mono}

class EndToEndTest extends FunSuite {
  import EndToEndTest._

  val echo: Service[io.rsocket.RSocket, io.rsocket.RSocket] = sendingSocket =>
    Future {
      new io.rsocket.RSocket {
        override def fireAndForget(payload: Payload): Mono[Void] = Mono.empty()

        override def requestResponse(payload: Payload): Mono[Payload] = Mono.just(payload.retain())

        override def requestStream(payload: Payload): Flux[Payload] =
          Flux.just(payload.retain(), payload.retain())

        override def requestChannel(payloads: Publisher[Payload]): Flux[Payload] =
          Flux.from(payloads).map(_.retain())
      }
  }

  test("FireAndForget") {
    connect(echo) { client =>
      val response = client.fireAndForget(DefaultPayload.create("foo"))
      for {
        output <- response.toTwitterFuture
      } yield output
    }
  }

  test("RequestResponse") {
    connect(echo) { client =>
      val response = client.requestResponse(DefaultPayload.create("foo"))
      for {
        output <- response.toTwitterFuture
      } yield assert(output.getDataUtf8 == "foo")
    }
  }

  test("RequestStream") {
    connect(echo) { client =>
      val response = client.requestStream(DefaultPayload.create("foo"))
      for {
        output <- response.collectList().toTwitterFuture
      } yield {
        assert(output.size() == 2)
        assert(output.get(0).getDataUtf8 == "foo")
        assert(output.get(1).getDataUtf8 == "foo")
      }
    }
  }

  test("RequestChannel") {
    connect(echo) { client =>
      val response =
        client.requestChannel(Flux.just(DefaultPayload.create("foo"), DefaultPayload.create("bar")))
      for {
        output <- response.collectList().toTwitterFuture
      } yield {
        assert(output.size() == 2)
        assert(output.get(0).getDataUtf8 == "foo")
        assert(output.get(1).getDataUtf8 == "bar")
      }
    }
  }
}

private object EndToEndTest {
  def connect(
      service: Service[io.rsocket.RSocket, io.rsocket.RSocket],
      stats: StatsReceiver = NullStatsReceiver
  )(run: io.rsocket.RSocket => Future[_]): Unit = {
    val runServer = Promise[Any]()
    val server = RSocket.server
      .withLabel("server")
      .configured(Stats(stats))
      .onConnect(client => run(client).proxyTo(runServer))
      .serve("localhost:*", service)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client = RSocket.client
      .configured(Stats(stats))
      .serve(service)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")
      .apply()

    Await
      .result(
          Futures.join(client.flatMap(run(_)), runServer).ensure(server.close()),
          Duration.fromSeconds(1))
  }
}
