package com.twitter.finagle

import com.twitter.finagle.client.{EndpointerModule, EndpointerStackClient, StackClient}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param.{Label, ProtocolLibrary}
import com.twitter.finagle.rsocket._
import com.twitter.finagle.server.{ListeningStackServer, StackServer}
import com.twitter.util.{CloseAwaitably, Future, Time}
import io.rsocket.core.{RSocketConnector, RSocketServer}
import io.rsocket.transport.netty.client.TcpClientTransport
import io.rsocket.transport.netty.server.{CloseableChannel, TcpServerTransport}
import io.rsocket.{Payload, RSocket}
import java.net.SocketAddress
import org.reactivestreams.Publisher
import reactor.core.publisher.{Flux, Mono}
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
            (prms: Stack.Params, sa: SocketAddress) =>
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
                    RSocketConnector
                      .create()
                      .connect(transport)
                      .toTwitterFuture
                      .map {
                        rSocket => request =>
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
                  }
              }
          }
      )

    override def copy1(
        stack: Stack[ServiceFactory[rsocket.Request, rsocket.Response]],
        params: Stack.Params
    ): TcpRSocketClient = copy(stack, params)
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
        serviceFactory: ServiceFactory[rsocket.Request, rsocket.Response],
        addr: SocketAddress)(trackSession: ClientConnection => Unit): ListeningServer = {
      //TODO: expose ClientConnection callback to track session
      val transport = TcpServerTransport.create(tcpServer.bindAddress(() => addr))
      val server = RSocketServer
        .create()
        .acceptor((setup, sendingSocket) => {
          Mono.just(new RSocket() {
            override def fireAndForget(payload: Payload): Mono[Void] =
              serviceFactory.toService(Request.FireAndForget(payload)).toMono.flatMap {
                case Response.FireAndForget(payload) => payload
              }

            override def requestResponse(payload: Payload): Mono[Payload] =
              serviceFactory.toService(Request.RequestResponse(payload)).toMono.flatMap {
                case Response.RequestResponse(payload) => payload
              }

            override def requestStream(payload: Payload): Flux[Payload] =
              serviceFactory.toService(Request.RequestStream(payload)).toMono.flatMapMany {
                case Response.RequestStream(payloads) => payloads
              }

            override def requestChannel(payloads: Publisher[Payload]): Flux[Payload] =
              serviceFactory.toService(Request.RequestChannel(payloads)).toMono.flatMapMany {
                case Response.RequestChannel(payloads) => payloads
              }
          })
        })
        .bindNow(transport)
      new TcpListeningServer(server)
    }

    override def copy1(
        stack: Stack[ServiceFactory[rsocket.Request, rsocket.Response]],
        params: Stack.Params
    ): TcpRSocketServer = copy(stack, params)
  }

  def server: TcpRSocketServer = TcpRSocketServer()

  override def serve(
      addr: SocketAddress,
      service: ServiceFactory[rsocket.Request, rsocket.Response]): ListeningServer =
    server.serve(addr, service)
}
