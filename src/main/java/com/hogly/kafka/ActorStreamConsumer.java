package com.hogly.kafka;

import akka.Done;
import akka.actor.*;
import akka.japi.Pair;
import akka.japi.pf.ReceiveBuilder;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.javadsl.Consumer;
import akka.pattern.PatternsCS;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.hogly.kafka.external.KafkaAdmin;
import com.hogly.kafka.messages.LastOffset;
import com.hogly.kafka.messages.ObtainLastOffset;
import com.typesafe.config.Config;
import kafka.api.PartitionMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.PartialFunction;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.BoxedUnit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ActorStreamConsumer extends AbstractLoggingActor {

  private final String bootstrapServers;
  private Consumer.Control control;

  public static void main(String[] args) {
    ActorSystem system = ActorSystem.create("actor-consumer");
    system.actorOf(ActorStreamConsumer.props(), "actorConsumer-0");
    system.actorOf(ActorStreamConsumer.props(), "actorConsumer-1");
    system.actorOf(ActorStreamConsumer.props(), "actorConsumer-2");
    system.actorOf(ActorStreamConsumer.props(), "actorConsumer-3");
  }

  private static final String TOPIC = "my-topic";
  private static final String GROUP_ID = "testing-group-5";
  private static final String CLIENT_ID = "testing-client";

  private static final int COMMIT_GROUP_SIZE = 10;
  private static final FiniteDuration COMMIT_TIME_WINDOW = FiniteDuration.create(5, TimeUnit.SECONDS);
  private static final int PARALLELISM_COMMIT = 1;
  private Map<Integer, Long> lastOffsets;
  private Map<Integer, Long> lastOffsetsCommitted = new HashMap<>();

  public static Props props() {
    return Props.create(ActorStreamConsumer.class);
  }

  public ActorStreamConsumer() {
    this.bootstrapServers = config().getString("kafka.bootstrap-servers");
  }

  @Override public PartialFunction<Object, BoxedUnit> receive() {
    return ReceiveBuilder
      .matchAny(msg -> unhandled(msg))
      .build();
  }

  private Config config() {
    return context().system().settings().config();
  }

  @Override public void preStart() throws Exception {
    this.lastOffsets = lastOffsets(bootstrapServers, TOPIC);
    streaming(bootstrapServers, GROUP_ID, CLIENT_ID, TOPIC);
  }

  private Map<Integer, Long> lastOffsets(String bootstrapServers, String topic) {
    Map<Integer, Long> offsets = new HashMap<>();
    ActorRef partitionConsumer = context().actorOf(ActorPartitionConsumer.props(), "partition-consumer");
    KafkaAdmin kafkaAdmin = KafkaAdmin.create(config());
    List<PartitionMetadata> partitions = kafkaAdmin.partitions(topic);
    partitions.forEach(partitionMetadata -> {
      try {
        ObtainLastOffset request = ObtainLastOffset.of(bootstrapServers, topic, partitionMetadata.partitionId());
        Object response = PatternsCS.ask(partitionConsumer, request, 30000).toCompletableFuture().get();
        if (response instanceof LastOffset) {
          LastOffset lastOffset = (LastOffset) response;
          offsets.put(lastOffset.partition(), lastOffset.offset());
        } else if (response instanceof Status.Failure) {
          Status.Failure failure = (Status.Failure) response;
          throw new RuntimeException("Error requesting last offset for: " + request, failure.cause());
        }
      } catch (InterruptedException | ExecutionException ex) {
        log().error("Error requesting last offset for: {}", partitionMetadata);
        throw new RuntimeException("Error requesting last offset", ex);
      }
    });
    return offsets;
  }

  private void streaming(String bootstrapServers, String groupId, String clientId, String... topics) {
    ActorMaterializer materializer = ActorMaterializer.create(context());

    Pair<Consumer.Control, CompletionStage<Done>> response = source(bootstrapServers, groupId, clientId, topics)
      .via(consumingFlow())
      .via(commitFlow())
      .toMat(Sink.ignore(), Keep.both())
      .run(materializer);

    control = response.first();
    response.second()
      .thenAccept(done -> {
        log().info("Stream finished successfully");
        log().info("lastOffsetsCommitted: {}", lastOffsetsCommitted);
        log().info("lastOffsets {}", lastOffsets);
      })
      .exceptionally(throwable -> {
        control.shutdown();
        log().error(throwable, "Stream finished with errors");
        return null;
      });
  }

  private Source<ConsumerMessage.CommittableMessage<String, String>, Consumer.Control> source(String bootstrapServers, String groupId, String clientId, String... topics) {
    ConsumerSettings<String, String> settings = ConsumerSettings.create(context().system(), new StringDeserializer(), new StringDeserializer(),
      ConsumerSettings.asSet(topics))
      .withBootstrapServers(bootstrapServers)
      .withGroupId(groupId)
      .withClientId(clientId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return Consumer.committableSource(settings);
  }

  private Flow<ConsumerMessage.CommittableMessage<String, String>, ConsumerMessage.CommittableOffset, ?> consumingFlow() {
    return Flow
      .fromFunction((ConsumerMessage.CommittableMessage<String, String> commitableMessage) -> commitableMessage)
      .map(committableMessage -> {
        log().info("Element: {} consumed by: {}", committableMessage.committableOffset(), self().path());
        return committableMessage.committableOffset();
      });
  }

  private Flow<ConsumerMessage.CommittableOffset, Done, ?> commitFlow() {
    return Flow.of(ConsumerMessage.CommittableOffset.class)
      .groupedWithin(COMMIT_GROUP_SIZE, COMMIT_TIME_WINDOW)
      .map(committableOffsets -> foldLeft(committableOffsets))
      .mapAsync(PARALLELISM_COMMIT, committableOffsetBatch -> committableOffsetBatch.commitJavadsl()
        .thenApply(done -> {
          log().info("Committing in Kafka: {}", committableOffsetBatch);
          updateLastOffsetCommitted(committableOffsetBatch);
          if (wasProcessedLastMessageOnAllPartitions()) {
            log().info("Stoping kafka consumer and stoping stream");
            control.stop();
          }
          return done;
        }).exceptionally(throwable -> {
          log().error(throwable, "Error committing in Kafka, skipped failure and continuing the process: {}", committableOffsetBatch);
          return Done.getInstance();
        }));
  }

  private ConsumerMessage.CommittableOffsetBatch foldLeft(List<ConsumerMessage.CommittableOffset> group) {
    ConsumerMessage.CommittableOffsetBatch batch = ConsumerMessage.emptyCommittableOffsetBatch();
    for (ConsumerMessage.CommittableOffset elem: group) {
      batch = batch.updated(elem);
    }
    return batch;
  }

  private void updateLastOffsetCommitted(ConsumerMessage.CommittableOffsetBatch committableOffsetBatch) {
    lastOffsets.entrySet().stream()
      .forEach(entry -> {
        int partition = entry.getKey();
        ConsumerMessage.ClientTopicPartition clientTopicPartition = new ConsumerMessage.ClientTopicPartition(CLIENT_ID, TOPIC, partition);
        Optional<Object> optionalOffset = committableOffsetBatch.getOffset(clientTopicPartition);
        if (optionalOffset.isPresent()) {
          Long offset = (Long) optionalOffset.get();
          lastOffsetsCommitted.put(partition, offset);
        }
      });
  }

  private boolean wasProcessedLastMessageOnAllPartitions() {
    return lastOffsetsCommitted.entrySet().stream()
      .allMatch(entry -> {
        int partition = entry.getKey();
        long offset = entry.getValue();
        return lastOffsets.get(partition) == offset;
      });
  }


}
