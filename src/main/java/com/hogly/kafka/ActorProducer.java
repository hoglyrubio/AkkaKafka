package com.hogly.kafka;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;
import com.hogly.kafka.external.KafkaAdmin;
import com.hogly.kafka.messages.CreateTopic;
import com.hogly.kafka.messages.TestMessage;
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

    //Patterns.ask(actorProducer, CreateTopic.of(TOPIC, 10, 1), TIMEOUT);
    Patterns.ask(actorProducer, TestMessage.of(5000, "Hola"), TIMEOUT);
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
      .match(TestMessage.class, this::handleTestMessage)
      .match(CreateTopic.class, this::handleCreateTopic)
      .build();
  }

  private Config config() {
    return context().system().settings().config();
  }

  private void handleCreateTopic(CreateTopic command) throws IOException {
    KafkaAdmin kafkaAdmin = KafkaAdmin.create(config());
    if (!kafkaAdmin.topicExists(command.topicName())) {
      kafkaAdmin.createTopic(command.topicName(), command.partions(), command.replicas());
      sender().tell(new Status.Success("created"), self());
    } else {
      sender().tell(new Status.Success("already exists"), self());
    }

    /*
    List<PartitionMetadata> partitions = kafkaAdmin.partitions(command.topicName());
    log().info("Topic: {} partitions: {} replicas: {}", command.topicName(), partitions.size(), command.replicas());
    kafkaAdmin.close();

    TopicUtils topicUtils = TopicUtils.create(config().getString("kafka.bootstrap-servers"));
    topicUtils.lastOffsetByPartition(TOPIC, 1);*/
  }

  private void handleTestMessage(TestMessage msg) {
    for (int i=0; i < msg.quantity(); i++) {
      publish(TOPIC, String.valueOf(i), msg.payload() + "#" + i);
    }
  }

  private void publish(String topic, String key, String value) {
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
    kafkaProducer.send(record, sendCallback(sender()));
  }

  private Callback sendCallback(ActorRef _sender) {
    return ((recordMetadata, e) -> {
      if (e != null) {
        log().error(e, "Error publishing on Kafka");
        _sender.tell(new Status.Failure(e), self());
      } else {
        log().info("Sent. Topic: {} Partition: {} Offset: {} ", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        sender().tell(new Status.Success(recordMetadata), self());
      }
    });
  }

}
