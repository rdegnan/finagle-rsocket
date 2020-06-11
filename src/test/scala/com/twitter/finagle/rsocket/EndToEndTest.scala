package com.twitter.finagle.rsocket

import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{RSocket, Service, rsocket}
import com.twitter.util.{Await, Closable, Duration, Future}
import io.rsocket.util.DefaultPayload
import java.net.InetSocketAddress
import org.scalatest.FunSuite
import reactor.core.publisher.{Flux, Mono}

class EndToEndTest extends FunSuite {
  import EndToEndTest._

  val echo: Service[rsocket.Request, rsocket.Response] = request =>
    Future {
      request match {
        case Request.FireAndForget(_) =>
          Response.FireAndForget(Mono.empty())
        case Request.RequestResponse(payload) =>
          Response.RequestResponse(Mono.just(payload.retain()))
        case Request.RequestStream(payload) =>
          Response.RequestStream(Flux.just(payload.retain(), payload.retain()))
        case Request.RequestChannel(payloads) =>
          Response.RequestChannel(Flux.from(payloads).map(_.retain()))
      }
  }

  test("FireAndForget") {
    connect(echo) { client =>
      val request = Request.FireAndForget(DefaultPayload.create("foo"))
      for {
        Response.FireAndForget(response) <- client(request)
        output <- response.toTwitterFuture
      } yield output
    }
  }

  test("RequestResponse") {
    connect(echo) { client =>
      val request = Request.RequestResponse(DefaultPayload.create("foo"))
      for {
        Response.RequestResponse(response) <- client(request)
        output <- response.toTwitterFuture
      } yield assert(output.getDataUtf8 == "foo")
    }
  }

  test("RequestStream") {
    connect(echo) { client =>
      val request = Request.RequestStream(DefaultPayload.create("foo"))
      for {
        Response.RequestStream(response) <- client(request)
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
      val request =
        Request.RequestChannel(
            Flux.just(DefaultPayload.create("foo"), DefaultPayload.create("bar")))
      for {
        Response.RequestChannel(response) <- client(request)
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
      service: Service[rsocket.Request, rsocket.Response],
      stats: StatsReceiver = NullStatsReceiver
  )(run: Service[rsocket.Request, rsocket.Response] => Future[_]): Unit = {
    val server = RSocket.server
      .withLabel("server")
      .configured(Stats(stats))
      .serve("localhost:*", service)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client = RSocket.client
      .configured(Stats(stats))
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    Await.result(run(client).ensure(Closable.all(client, server).close()), Duration.fromSeconds(1))
  }
}
