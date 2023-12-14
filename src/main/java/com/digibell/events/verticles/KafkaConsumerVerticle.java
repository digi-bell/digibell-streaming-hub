package com.digibell.events.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.*;

public class KafkaConsumerVerticle extends AbstractVerticle {

  private static final String GROUP_ID = "com.digibell.events.group";
  private static final String TOPIC = "digibell-events-topic";
  private KafkaConsumer<String, String> consumer;

  private static List<String> eventsList = new ArrayList<>();

  @Override
  public void start() {
    Properties config = new Properties();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", GROUP_ID);

    consumer = KafkaConsumer.create(vertx, config);

    EventBus eventBus = vertx.eventBus();
    consumer.handler(record -> {
      System.out.println("Processing key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());
      String value = record.value();
      eventsList.add(value);
      // Process the message

      // For example, publish the received message via event bus
      eventBus.publish("kafka-message", value);
    });
    consumer.subscribe(TOPIC);
  }

  @Override
  public void stop() {
    if (consumer != null) {
      consumer.close();
    }
  }

  public static List<String> getEventsList(){
    return eventsList;
  }
}
