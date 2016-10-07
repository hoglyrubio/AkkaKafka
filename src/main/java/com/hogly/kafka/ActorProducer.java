package com.hogly.kafka;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.util.Properties;

public class ActorProducer extends AbstractLoggingActor {

  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create("actor-producer");
    ActorRef actorProducer = system.actorOf(ActorProducer.props(), "actorProducer");

    Patterns.ask(actorProducer, CreateTopic.of(TOPIC, 10, 1), TIMEOUT);
    //actorProducer.tell(50, ActorRef.noSender());
  }

  public final static int TIMEOUT = 10000;
  public final static String TOPIC = "my-topic";
  private KafkaProducer<String, String> kafkaProducer;

  public ActorProducer() {
    Config config = context().system().settings().config();
    Properties props = new Properties();
    props.put("bootstrap.servers", config.getString("kafka.bootstrap-servers"));
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    this.kafkaProducer = new KafkaProducer<>(props);
  }

  public static Props props() {
    return Props.create(ActorProducer.class);
  }

  @Override
  public PartialFunction<Object, BoxedUnit> receive() {
    return ReceiveBuilder
      .match(String.class, this::sendUnique)
      .match(Integer.class, this::sendMultiple)
      .match(CreateTopic.class, this::createTopic)
      .build();
  }

  private Config config() {
    return context().system().settings().config();
  }

  private void createTopic(CreateTopic command) throws IOException {
    KafkaAdmin kafkaAdmin = KafkaAdmin.create(config());
    if (!kafkaAdmin.topicExists(command.topicName())) {
      kafkaAdmin.createTopic(command.topicName(), command.partions(), command.replicas());
    }
    int partitions = kafkaAdmin.partitions(command.topicName());
    log().info("Topic: {} partitions: {} replicas: {}", command.topicName(), partitions, command.replicas());
    kafkaAdmin.close();

    TopicUtils topicUtils = TopicUtils.create(config().getString("kafka.bootstrap-servers"));
    topicUtils.lastOffsetByPartition(TOPIC, partitions);
  }

  private void sendMultiple(Integer qty) {
    for (int i=0; i<qty; i++) {
      publish(TOPIC, String.valueOf(i), "Message #" + i);
    }
  }

  private void sendUnique(String message) {
    publish(TOPIC, message, message);
  }

  private void publish(String topic, String key, String value) {
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
    kafkaProducer.send(record, sendCallback(sender()));
  }

  private Callback sendCallback(ActorRef _sender) {
    return ((recordMetadata, e) -> {
      if (e != null) {
        log().error(e, "Error publishing on Kafka");
        Patterns.pipe(Futures.failed(e), context().dispatcher()).to(_sender);
      } else {
        log().info("Sent. T: {} P: {} O: {} ", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        Patterns.pipe(Futures.successful(recordMetadata), context().dispatcher()).to(_sender);
      }
    });
  }

}
