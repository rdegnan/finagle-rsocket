package com.twitter.finagle.rsocket

import com.twitter.finagle.Service
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Await, Closable, Duration, Future}
import io.rsocket.Payload
import io.rsocket.util.DefaultPayload
import java.net.InetSocketAddress
import org.scalatest.FunSuite

class EndToEndTest extends FunSuite {
  import EndToEndTest._
  test("echo") {
    val echo = new Service[Payload, Payload] {
      def apply(req: Payload): Future[Payload] =
        Future.value(req.retain())
    }

    connect(echo) { client =>
      val payloads = List("hello", "world")
      Future.traverseSequentially(payloads) { payload =>
        for {
          response <- client(DefaultPayload.create(payload))
        } yield assert(response.getDataUtf8 == payload)
      }
    }
  }
}

private object EndToEndTest {
  def connect(
      service: Service[Payload, Payload],
      stats: StatsReceiver = NullStatsReceiver
  )(run: Service[Payload, Payload] => Future[_]): Unit = {
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
