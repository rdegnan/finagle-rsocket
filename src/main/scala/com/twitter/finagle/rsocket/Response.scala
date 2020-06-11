package com.twitter.finagle.rsocket

import io.rsocket.Payload
import reactor.core.publisher.{Flux, Mono}

sealed trait Response extends Any

object Response {
  case class FireAndForget(response: Mono[Void]) extends AnyVal with Response

  case class RequestResponse(response: Mono[Payload]) extends AnyVal with Response

  case class RequestStream(response: Flux[Payload]) extends AnyVal with Response

  case class RequestChannel(response: Flux[Payload]) extends AnyVal with Response
}
