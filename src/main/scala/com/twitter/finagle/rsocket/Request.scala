package com.twitter.finagle.rsocket

import io.rsocket.Payload
import org.reactivestreams.Publisher

sealed trait Request extends Any

object Request {
  case class FireAndForget(request: Payload) extends AnyVal with Request

  case class RequestResponse(request: Payload) extends AnyVal with Request

  case class RequestStream(request: Payload) extends AnyVal with Request

  case class RequestChannel(request: Publisher[Payload]) extends AnyVal with Request
}
