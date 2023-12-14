package com.digibell.events.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.List;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) {
    vertx.deployVerticle(new KafkaProducerVerticle());
    vertx.deployVerticle(new KafkaConsumerVerticle());

    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());

    // Expose producer as an API endpoint
    router.post("/api/v1/events/produce").handler(ctx -> {
      String message = ctx.getBodyAsString();
      System.out.println("Message received: " + message);
      vertx.eventBus().request("produce-message", message, reply -> {
        System.out.println("inside Message Sending : " + reply.result().body().toString());
        if (reply.succeeded()) {
          ctx.response().end(reply.result().body().toString());
        } else {
          ctx.response().setStatusCode(500).end("Failed to produce message - Updated");
        }
      });
    });

    // Expose consumer data via API
    router.get("/api/v1/events/consume").handler(ctx -> {
      List<String> result = KafkaConsumerVerticle.getEventsList();
      ctx.response().end(result.toString());
    });

    vertx.createHttpServer()
      .requestHandler(router)
      .listen(8085, http -> {
        if (http.succeeded()) {
          startPromise.complete();
          System.out.println("HTTP server started on port 8080");
        } else {
          startPromise.fail(http.cause());
        }
      });
  }
}

