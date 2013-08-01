package com.example

import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import akka.actor._
import spray.can.Http
import spray.can.server.Stats
import spray.util._
import spray.http._
import HttpMethods._
import MediaTypes._
import akka.io.IO
import spray.can.Http
import scala.concurrent._
import scala.concurrent.ExecutionContext

class MyServiceActor extends MyService with Controller {
  implicit val system = context.system
  val client: ActorRef = IO(Http)
}

trait IsAClient {
  implicit val timeout: Timeout
  implicit val executionContext: ExecutionContext
  val client: ActorRef
}

trait Service {
  def route: PartialFunction[HttpRequest, Future[OurResponse]]
}

trait OurResponse {}

case class EntityYY(spray: HttpResponse) extends OurResponse

case class Streaming(a: A) extends OurResponse

case class A(a: Int)(implicit val ex: ExecutionContext) {
  def get() = ("A" * 10000).getBytes()
  def next(): Future[Option[A]] = {
    if (a > 0) future {
      Thread.sleep(1000)
      Some(A(a - 1))
    }
    else Future.successful(None)
  }
}

trait Controller extends Service with IsAClient {

  def route: PartialFunction[HttpRequest, Future[OurResponse]] = {
    case r @ HttpRequest(GET, Uri.Path("/spray"), _, _, _) => spray(r)
    case r @ HttpRequest(GET, Uri.Path("/generate"), _, _, _) => generate()
  }

  private def http(request: HttpRequest) =
    (client ? request).mapTo[HttpResponse]

  def getRaw(uri: String): Future[HttpResponse] =
    http(HttpRequest(GET, Uri(uri)))

  def get[A](uri: String, deserializer: HttpEntity => A): Future[A] =
    http(HttpRequest(GET, Uri(uri))).map(body => deserializer(body.entity))

  def postRaw(uri: String, entity: HttpEntity): Future[HttpResponse] =
    http(HttpRequest(POST, Uri(uri), entity = entity))

  def post[A](uri: String, entity: A, serializer: A => HttpBody): Future[HttpResponse] =
    http(HttpRequest(POST, Uri(uri), entity = serializer(entity)))

  def spray(request: HttpRequest): Future[OurResponse] = {
    getRaw("http://spray.io/").map(x =>
      EntityYY(HttpResponse(entity = x.entity).withHeaders(x.headers)))
  }

  def generate(): Future[OurResponse] = {
    future {
      Thread.sleep(1000)
      Streaming(A(100))
    }
  }
}

trait MyService extends Actor with SprayActorLogging with Service {
  implicit val timeout: Timeout = 8.second // for the actor 'asks'
  implicit val executionContext = context.dispatcher // ExecutionContext for the futures and scheduler

  def receive = {
    // when a new connection comes in we register ourselves as the connection handler
    case _: Http.Connected => sender ! Http.Register(self)

    case r: HttpRequest =>
      val peer = sender
      route.lift(r)
        .getOrElse(Future.successful(EntityYY(HttpResponse(status = 404, entity = "Unknown resource!"))))
        .onSuccess {
          case x: EntityYY => peer ! x.spray
          case Streaming(a) => self ! Stream(peer, a)
        }

    case Timedout(HttpRequest(method, uri, _, _, _)) =>
      sender ! HttpResponse(
        status = 500,
        entity = "The " + method + " request to '" + uri + "' has timed out...")

    case Stream(peer: ActorRef, a: A) => context actorOf Props(new Streamer2(peer, a))
  }

  case class Stream(client: ActorRef, a: A)

  class Streamer2(client: ActorRef, a: A) extends Actor with SprayActorLogging {
    log.debug("Starting streaming response ...")

    client ! ChunkedResponseStart(HttpResponse(entity = a.get)).withAck(Ok(a.next))

    def receive = {

      case Chunk(None) =>
        log.info("Finalizing response stream ...")
        client ! MessageChunk("\nStopped...")
        client ! ChunkedMessageEnd
        context.stop(self)
      case Chunk(Some(a)) =>
        log.info("Sending response chunk ...")
        client ! MessageChunk(a.get).withAck(Ok(a.next))

      case Ok(f) =>
        f.onSuccess { case a => self ! Chunk(a) }

      case x: Http.ConnectionClosed =>
        log.info("Canceling response stream due to {} ...", x)
        context.stop(self)
    }

    case class Chunk(a: Option[A])
    // simple case class whose instances we use as send confirmation message for streaming chunks
    case class Ok(f: Future[Option[A]])
  }

}