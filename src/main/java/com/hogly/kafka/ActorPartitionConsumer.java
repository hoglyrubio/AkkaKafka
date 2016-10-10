package com.hogly.kafka;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import com.hogly.kafka.external.KafkaAdmin;
import com.hogly.kafka.messages.LastOffset;
import com.hogly.kafka.messages.ObtainLastOffset;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.api.PartitionMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

public class ActorPartitionConsumer extends AbstractLoggingActor {

  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create("kafka-tests");
    ActorRef actor = system.actorOf(ActorPartitionConsumer.props(), "actor-partition-consumer");
    Config config = ConfigFactory.load("application.conf");
    String bootstrapServers = config.getString("kafka.bootstrap-servers");

    KafkaAdmin kafkaAdmin = KafkaAdmin.create(config);
    List<PartitionMetadata> partitions = kafkaAdmin.partitions("my-topic");
    partitions
      .forEach(partitionMetadata -> {
        ObtainLastOffset request = ObtainLastOffset.of(bootstrapServers, "my-topic", partitionMetadata.partitionId());
        CompletionStage<Object> response = PatternsCS.ask(actor, request, 10000);

        response
          .thenAccept(lastOffset -> {
            System.out.println(lastOffset);
          })
          .exceptionally(throwable -> {
            throw new RuntimeException(throwable);
          });
      });
  }

  public static Props props() {
    return Props.create(ActorPartitionConsumer.class, () -> new ActorPartitionConsumer());
  }

  @Override public PartialFunction<Object, BoxedUnit> receive() {
    return ReceiveBuilder
      .match(ObtainLastOffset.class, this::handleObtainLastOffset)
      .build();
  }

  private void handleObtainLastOffset(ObtainLastOffset command) {
    try {
      String groupId = "actor-partition-consumer-" + UUID.randomUUID().toString();
      Properties props = properties(command.bootstrapServers(), groupId, "actor-partition-consumer");
      TopicPartition topicPartition = new TopicPartition(command.topic(), command.partition());
      KafkaConsumer<String, String> consumer = kafkaConsumerByPartition(props, topicPartition);
      consumer.seekToEnd(topicPartition);
      long nextOffset = consumer.position(topicPartition);
      consumer.close();
      sender().tell(LastOffset.of(command.topic(), command.partition(), nextOffset - 1), self());
    } catch (KafkaException ex) {
      log().error(ex, "Error obtaining last offset for {}", command);
      sender().tell(new Status.Failure(ex), self());
    }
  }

  public KafkaConsumer<String,String> kafkaConsumerByPartition(Properties props, TopicPartition topicPartition) {
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.assign(Arrays.asList(topicPartition));
    return consumer;
  }

  private Properties properties(String bootstrapServers, String groupId, String clientId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("group.id", groupId);
    props.put("client.id", clientId);
    props.put("session.timeout.ms", "30000");
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("auto.offset.reset", "earliest");
    return props;
  }


}
