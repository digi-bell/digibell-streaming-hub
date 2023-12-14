package com.digibell.events.verticles;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerVerticle extends AbstractVerticle {

  private KafkaProducer<String, String> producer;

  public static final String TOPIC = "digibell-events-topic";

  @Override
  public void start() {
    Properties config = new Properties();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", StringSerializer.class.getName());

    producer = KafkaProducer.create(vertx, config);

    EventBus eventBus = vertx.eventBus();
    eventBus.consumer("produce-message", message -> {
      String value = (String) message.body();
      // Produce message to Kafka topic
      KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(TOPIC, value);
      producer.send(record);
      message.reply("Message sent to Kafka topic");
    });
  }

  @Override
  public void stop() {
    if (producer != null) {
      producer.close();
    }
  }
}
