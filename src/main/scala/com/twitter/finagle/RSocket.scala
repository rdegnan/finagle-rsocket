package com.twitter.finagle

import com.twitter.finagle.client.{EndpointerModule, EndpointerStackClient, StackClient}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param.{Label, ProtocolLibrary}
import com.twitter.finagle.rsocket._
import com.twitter.finagle.server.{ListeningStackServer, StackServer}
import com.twitter.util.{CloseAwaitably, Future, Time}
import io.rsocket.core.{RSocketConnector, RSocketServer, Resume}
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.{CloseableChannel, TcpServerTransport}
import io.rsocket.{Payload, RSocket}
import java.net.SocketAddress
import org.reactivestreams.Publisher
import reactor.core.publisher.{Flux, Mono}
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

private class ServiceFactoryRSocket(factory: ServiceFactory[rsocket.Request, rsocket.Response])
    extends RSocket {
  override def fireAndForget(payload: Payload): Mono[Void] =
    factory.toService(Request.FireAndForget(payload)).toMono.flatMap {
      case Response.FireAndForget(payload) => payload
    }

  override def requestResponse(payload: Payload): Mono[Payload] =
    factory.toService(Request.RequestResponse(payload)).toMono.flatMap {
      case Response.RequestResponse(payload) => payload
    }

  override def requestStream(payload: Payload): Flux[Payload] =
    factory.toService(Request.RequestStream(payload)).toMono.flatMapMany {
      case Response.RequestStream(payloads) => payloads
    }

  override def requestChannel(payloads: Publisher[Payload]): Flux[Payload] =
    factory.toService(Request.RequestChannel(payloads)).toMono.flatMapMany {
      case Response.RequestChannel(payloads) => payloads
    }
}

private class RSocketService(rSocket: RSocket) extends Service[rsocket.Request, rsocket.Response] {
  override def apply(request: Request): Future[Response] =
    Future {
      request match {
        case Request.FireAndForget(payload) =>
          Response.FireAndForget(rSocket.fireAndForget(payload))
        case Request.RequestResponse(payload) =>
          Response.RequestResponse(rSocket.requestResponse(payload))
        case Request.RequestStream(payload) =>
          Response.RequestStream(rSocket.requestStream(payload))
        case Request.RequestChannel(payloads) =>
          Response.RequestChannel(rSocket.requestChannel(payloads))
      }
    }
}

object RSocket
    extends Client[rsocket.Request, rsocket.Response]
    with Server[rsocket.Request, rsocket.Response] {
  final case class TcpRSocketClient(
      stack: Stack[ServiceFactory[rsocket.Request, rsocket.Response]] = StackClient.newStack,
      params: Stack.Params = StackClient.defaultParams)
      extends EndpointerStackClient[rsocket.Request, rsocket.Response, TcpRSocketClient] {
    override def endpointer: Stackable[ServiceFactory[rsocket.Request, rsocket.Response]] =
      new EndpointerModule[rsocket.Request, rsocket.Response](
          Seq(implicitly[Stack.Param[ProtocolLibrary]], implicitly[Stack.Param[Label]]), {
            (params: Stack.Params, sa: SocketAddress) =>
              //TODO: configure client based on params
              val tcpClient = TcpClient.create()
              val transport = TcpClientTransport.create(tcpClient.remoteAddress(() => sa))

              //TODO: what do we register here?
              //endpointClient.registerTransporter(connector.toString)

              // Note, this ensures that session establishment is lazy (i.e.,
              // on the service acquisition path).
              ServiceFactory[rsocket.Request, rsocket.Response] {
                () =>
                  // we do not want to capture and request specific Locals
                  // that would live for the life of the session.
                  Contexts.letClearAll {
                    val connector = RSocketConnector.create()
                    params[rsocket.param.Fragmentation].mtu.foreach(connector.fragment)
                    params[rsocket.param.Reconnection].retry.foreach(connector.reconnect)
                    params[rsocket.param.Resumption].resume.foreach(connector.resume)
                    params[rsocket.param.ClientServiceFactory].factory.foreach { factory =>
                      val rSocket = new ServiceFactoryRSocket(factory)
                      connector.acceptor((setup, sendingSocket) => Mono.just(rSocket))
                    }

                    connector
                      .connect(transport)
                      .toTwitterFuture
                      .map(new RSocketService(_))
                  }
              }
          }
      )

    override def copy1(
        stack: Stack[ServiceFactory[rsocket.Request, rsocket.Response]],
        params: Stack.Params
    ): TcpRSocketClient = copy(stack, params)

    def fragment(mtu: Int): TcpRSocketClient =
      this.configured(rsocket.param.Fragmentation(Some(mtu)))

    def reconnect(retry: Retry): TcpRSocketClient =
      this.configured(rsocket.param.Reconnection(Some(retry)))

    def resume(resume: Resume): TcpRSocketClient =
      this.configured(rsocket.param.Resumption(Some(resume)))

    def serve(service: ServiceFactory[rsocket.Request, rsocket.Response]): TcpRSocketClient =
      this.configured(rsocket.param.ClientServiceFactory(Some(service)))

    def serve(service: Service[rsocket.Request, rsocket.Response]): TcpRSocketClient =
      this.configured(rsocket.param.ClientServiceFactory(Some(ServiceFactory.const(service))))
  }

  def client: TcpRSocketClient = TcpRSocketClient()

  override def newService(
      dest: Name,
      label: String
  ): Service[rsocket.Request, rsocket.Response] = client.newService(dest, label)

  override def newClient(
      dest: Name,
      label: String
  ): ServiceFactory[rsocket.Request, rsocket.Response] = client.newClient(dest, label)

  final case class TcpRSocketServer(
      stack: Stack[ServiceFactory[rsocket.Request, rsocket.Response]] = StackServer.newStack,
      params: Stack.Params = StackServer.defaultParams)
      extends ListeningStackServer[rsocket.Request, rsocket.Response, TcpRSocketServer] {
    //TODO: configure server based on params
    private val tcpServer = TcpServer.create()

    override def newListeningServer(
        factory: ServiceFactory[rsocket.Request, rsocket.Response],
        addr: SocketAddress)(trackSession: ClientConnection => Unit): ListeningServer = {
      //TODO: expose ClientConnection callback to track session
      val transport = TcpServerTransport.create(tcpServer.bindAddress(() => addr))
      val server = RSocketServer.create()
      params[rsocket.param.Fragmentation].mtu.foreach(server.fragment)
      params[rsocket.param.Resumption].resume.foreach(server.resume)

      val rSocket = new ServiceFactoryRSocket(factory)
      val channel = server
        .acceptor((setup, sendingSocket) => {
          params[rsocket.param.ServerOnConnect].onConnect.foreach { onConnect =>
            val service = new RSocketService(sendingSocket)
            onConnect(service)
          }
          Mono.just(rSocket)
        })
        .bindNow(transport)
      new TcpListeningServer(channel)
    }

    override def copy1(
        stack: Stack[ServiceFactory[rsocket.Request, rsocket.Response]],
        params: Stack.Params
    ): TcpRSocketServer = copy(stack, params)

    def fragment(mtu: Int): TcpRSocketServer =
      this.configured(rsocket.param.Fragmentation(Some(mtu)))

    def resume(resume: Resume): TcpRSocketServer =
      this.configured(rsocket.param.Resumption(Some(resume)))

    def onConnect(onConnect: Service[rsocket.Request, rsocket.Response] => Unit): TcpRSocketServer =
      this.configured(rsocket.param.ServerOnConnect(Some(onConnect)))
  }

  def server: TcpRSocketServer = TcpRSocketServer()

  override def serve(
      addr: SocketAddress,
      service: ServiceFactory[rsocket.Request, rsocket.Response]): ListeningServer =
    server.serve(addr, service)
}
