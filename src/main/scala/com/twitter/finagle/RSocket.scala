package com.twitter.finagle

import com.twitter.finagle.client.{EndpointerModule, EndpointerStackClient, StackClient}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param.{Label, ProtocolLibrary}
import com.twitter.finagle.rsocket._
import com.twitter.finagle.server.{ListeningStackServer, StackServer}
import com.twitter.util.{CloseAwaitably, Future, Time}
import io.rsocket.RSocket
import io.rsocket.core.{RSocketConnector, RSocketServer, Resume}
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.{CloseableChannel, TcpServerTransport}
import java.net.SocketAddress
import reactor.netty.tcp.{TcpClient, TcpServer}
import reactor.util.retry.Retry

private class TcpListeningServer(channel: CloseableChannel)
    extends ListeningServer
    with CloseAwaitably {
  override def boundAddress: SocketAddress = channel.address()

  override def closeServer(deadline: Time): Future[Unit] =
    closeAwaitably {
      channel.dispose()
      channel.onClose().toTwitterFuture
    }
}

object RSocket extends Client[Unit, RSocket] with Server[RSocket, RSocket] {
  final case class TcpRSocketClient(
      stack: Stack[ServiceFactory[Unit, RSocket]] = StackClient.newStack,
      params: Stack.Params = StackClient.defaultParams)
      extends EndpointerStackClient[Unit, RSocket, TcpRSocketClient] {
    override def endpointer: Stackable[ServiceFactory[Unit, RSocket]] =
      new EndpointerModule[Unit, RSocket](
          Seq(implicitly[Stack.Param[ProtocolLibrary]], implicitly[Stack.Param[Label]]), {
            (params: Stack.Params, sa: SocketAddress) =>
              //TODO: configure client based on params
              val tcpClient = TcpClient.create()
              val transport = TcpClientTransport.create(tcpClient.remoteAddress(() => sa))

              //TODO: what do we register here?
              //endpointClient.registerTransporter(connector.toString)

              // Note, this ensures that session establishment is lazy (i.e.,
              // on the service acquisition path).
              ServiceFactory[Unit, RSocket] {
                () =>
                  // we do not want to capture and request specific Locals
                  // that would live for the life of the session.
                  Contexts.letClearAll {
                    val connector = RSocketConnector.create()
                    params[rsocket.param.Fragmentation].mtu.foreach(connector.fragment)
                    params[rsocket.param.Reconnection].retry.foreach(connector.reconnect)
                    params[rsocket.param.Resumption].resume.foreach(connector.resume)
                    params[rsocket.param.ClientServiceFactory].factory.foreach { factory =>
                      connector.acceptor((setup, sendingSocket) =>
                        factory.toService(sendingSocket).toMono)
                    }

                    Future { _ =>
                      connector
                        .connect(transport)
                        .toTwitterFuture
                    }
                  }
              }
          }
      )

    override def copy1(
        stack: Stack[ServiceFactory[Unit, RSocket]],
        params: Stack.Params
    ): TcpRSocketClient = copy(stack, params)

    def fragment(mtu: Int): TcpRSocketClient =
      this.configured(rsocket.param.Fragmentation(Some(mtu)))

    def reconnect(retry: Retry): TcpRSocketClient =
      this.configured(rsocket.param.Reconnection(Some(retry)))

    def resume(resume: Resume): TcpRSocketClient =
      this.configured(rsocket.param.Resumption(Some(resume)))

    def serve(service: ServiceFactory[io.rsocket.RSocket, io.rsocket.RSocket]): TcpRSocketClient =
      this.configured(rsocket.param.ClientServiceFactory(Some(service)))

    def serve(service: Service[io.rsocket.RSocket, io.rsocket.RSocket]): TcpRSocketClient =
      this.configured(rsocket.param.ClientServiceFactory(Some(ServiceFactory.const(service))))
  }

  def client: TcpRSocketClient = TcpRSocketClient()

  override def newService(
      dest: Name,
      label: String
  ): Service[Unit, RSocket] = client.newService(dest, label)

  override def newClient(
      dest: Name,
      label: String
  ): ServiceFactory[Unit, RSocket] = client.newClient(dest, label)

  final case class TcpRSocketServer(
      stack: Stack[ServiceFactory[RSocket, RSocket]] = StackServer.newStack,
      params: Stack.Params = StackServer.defaultParams)
      extends ListeningStackServer[RSocket, RSocket, TcpRSocketServer] {
    //TODO: configure server based on params
    private val tcpServer = TcpServer.create()

    override def newListeningServer(factory: ServiceFactory[RSocket, RSocket], addr: SocketAddress)(
        trackSession: ClientConnection => Unit): ListeningServer = {
      //TODO: expose ClientConnection callback to track session
      val transport = TcpServerTransport.create(tcpServer.bindAddress(() => addr))
      val server = RSocketServer.create()
      params[rsocket.param.Fragmentation].mtu.foreach(server.fragment)
      params[rsocket.param.Resumption].resume.foreach(server.resume)

      val channel = server
        .acceptor((setup, sendingSocket) => {
          params[rsocket.param.ServerOnConnect].onConnect.foreach { onConnect =>
            onConnect(sendingSocket)
          }
          factory.toService(sendingSocket).toMono
        })
        .bindNow(transport)
      new TcpListeningServer(channel)
    }

    override def copy1(
        stack: Stack[ServiceFactory[RSocket, RSocket]],
        params: Stack.Params
    ): TcpRSocketServer = copy(stack, params)

    def fragment(mtu: Int): TcpRSocketServer =
      this.configured(rsocket.param.Fragmentation(Some(mtu)))

    def resume(resume: Resume): TcpRSocketServer =
      this.configured(rsocket.param.Resumption(Some(resume)))

    def onConnect(onConnect: RSocket => Unit): TcpRSocketServer =
      this.configured(rsocket.param.ServerOnConnect(Some(onConnect)))
  }

  def server: TcpRSocketServer = TcpRSocketServer()

  override def serve(
      addr: SocketAddress,
      service: ServiceFactory[RSocket, RSocket]): ListeningServer =
    server.serve(addr, service)
}
