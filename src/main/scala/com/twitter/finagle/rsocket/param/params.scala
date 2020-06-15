package com.twitter.finagle.rsocket.param

import com.twitter.finagle.{Service, ServiceFactory, Stack, rsocket}
import io.rsocket.core.Resume
import reactor.util.retry.Retry

case class Fragmentation(mtu: Option[Int]) {
  def mk(): (Fragmentation, Stack.Param[Fragmentation]) =
    (this, Fragmentation.param)
}
object Fragmentation {
  implicit val param = Stack.Param(Fragmentation(None))
}

case class Reconnection(retry: Option[Retry]) {
  def mk(): (Reconnection, Stack.Param[Reconnection]) =
    (this, Reconnection.param)
}
object Reconnection {
  implicit val param = Stack.Param(Reconnection(None))
}

case class Resumption(resume: Option[Resume]) {
  def mk(): (Resumption, Stack.Param[Resumption]) =
    (this, Resumption.param)
}
object Resumption {
  implicit val param = Stack.Param(Resumption(None))
}

case class ClientServiceFactory(
    factory: Option[ServiceFactory[rsocket.Request, rsocket.Response]]) {
  def mk(): (ClientServiceFactory, Stack.Param[ClientServiceFactory]) =
    (this, ClientServiceFactory.param)
}
object ClientServiceFactory {
  implicit val param = Stack.Param(ClientServiceFactory(None))
}

case class ServerOnConnect(onConnect: Option[Service[rsocket.Request, rsocket.Response] => Unit]) {
  def mk(): (ServerOnConnect, Stack.Param[ServerOnConnect]) =
    (this, ServerOnConnect.param)
}
object ServerOnConnect {
  implicit val param = Stack.Param(ServerOnConnect(None))
}
