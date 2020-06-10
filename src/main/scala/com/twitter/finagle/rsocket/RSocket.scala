package com.twitter.finagle.rsocket

import com.twitter.finagle.client.{EndpointerModule, EndpointerStackClient, StackClient}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param.{Label, ProtocolLibrary}
import com.twitter.finagle.server.{ListeningStackServer, StackServer}
import com.twitter.finagle._
import com.twitter.util.{CloseAwaitably, Future, Time}
import io.rsocket.core.{RSocketConnector, RSocketServer}
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.{CloseableChannel, TcpServerTransport}
import io.rsocket.{Payload, SocketAcceptor}
import java.net.SocketAddress
import reactor.core.publisher.Mono
import reactor.netty.tcp.{TcpClient, TcpServer}

private class TcpListeningServer(server: CloseableChannel)
    extends ListeningServer
    with CloseAwaitably {
  override def boundAddress: SocketAddress = server.address()

  override def closeServer(deadline: Time): Future[Unit] =
    closeAwaitably {
      server.dispose()
      server.onClose().toTwitterFuture
    }
}

object RSocket extends Client[Payload, Payload] with Server[Payload, Payload] {
  final case class TcpRSocketClient(
      stack: Stack[ServiceFactory[Payload, Payload]] = StackClient.newStack,
      params: Stack.Params = StackClient.defaultParams)
      extends EndpointerStackClient[Payload, Payload, TcpRSocketClient] {
    override def endpointer: Stackable[ServiceFactory[Payload, Payload]] =
      new EndpointerModule[Payload, Payload](
          Seq(implicitly[Stack.Param[ProtocolLibrary]], implicitly[Stack.Param[Label]]), {
            (prms: Stack.Params, sa: SocketAddress) =>
              //TODO: configure client based on params
              val tcpClient = TcpClient.create()
              val transport = TcpClientTransport.create(tcpClient.remoteAddress(() => sa))

              //TODO: what do we register here?
              //endpointClient.registerTransporter(connector.toString)

              // Note, this ensures that session establishment is lazy (i.e.,
              // on the service acquisition path).
              ServiceFactory.apply[Payload, Payload] { () =>
                // we do not want to capture and request specific Locals
                // that would live for the life of the session.
                Contexts.letClearAll {
                  RSocketConnector
                    .create()
                    .connect(transport)
                    .toTwitterFuture
                    .map(rSocket => payload => rSocket.requestResponse(payload).toTwitterFuture)
                }
              }
          }
      )

    override def copy1(
        stack: Stack[ServiceFactory[Payload, Payload]],
        params: Stack.Params
    ): TcpRSocketClient = copy(stack, params)
  }

  def client: TcpRSocketClient = TcpRSocketClient()

  override def newService(
      dest: Name,
      label: String
  ): Service[Payload, Payload] = client.newService(dest, label)

  override def newClient(
      dest: Name,
      label: String
  ): ServiceFactory[Payload, Payload] = client.newClient(dest, label)

  final case class TcpRSocketServer(
      stack: Stack[ServiceFactory[Payload, Payload]] = StackServer.newStack,
      params: Stack.Params = StackServer.defaultParams)
      extends ListeningStackServer[Payload, Payload, TcpRSocketServer] {
    //TODO: configure server based on params
    private val tcpServer = TcpServer.create()

    override def newListeningServer(
        serviceFactory: ServiceFactory[Payload, Payload],
        addr: SocketAddress)(trackSession: ClientConnection => Unit): ListeningServer = {
      //TODO: expose ClientConnection callback to track session
      val transport = TcpServerTransport.create(tcpServer.bindAddress(() => addr))
      val server = RSocketServer
        .create()
        .acceptor(
            SocketAcceptor.forRequestResponse { payload =>
              Mono.fromFuture(serviceFactory.toService.apply(payload).toCompletableFuture)
            }
        )
        .bindNow(transport)
      new TcpListeningServer(server)
    }

    override def copy1(
        stack: Stack[ServiceFactory[Payload, Payload]],
        params: Stack.Params
    ): TcpRSocketServer = copy(stack, params)
  }

  def server: TcpRSocketServer = TcpRSocketServer()

  override def serve(
      addr: SocketAddress,
      service: ServiceFactory[Payload, Payload]): ListeningServer = server.serve(addr, service)
}
