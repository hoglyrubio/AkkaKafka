package com.hogly.kafka.external;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TopicUtils {

  private final static Logger LOG = LoggerFactory.getLogger(TopicUtils.class);

  private String bootstrapServers;

  public static TopicUtils create(String bootstrapServers) {
    return new TopicUtils(bootstrapServers);
  }

  private TopicUtils(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public Map<Integer, Long> lastOffsetByPartitions(String topic, int partitions) {
    Map<Integer, Long> offsetsByPartition = new HashMap<>(partitions);
    for(int partition = 0; partition < partitions; partition++) {
      Long offset = lastOffsetByPartition(topic, partition);
      offsetsByPartition.put(partition, offset);
    }
    LOG.debug("lastOffsetByPartitions: {}", offsetsByPartition);
    return offsetsByPartition;
  }

  public long lastOffsetByPartition(String topic, int partition) {
    LOG.debug("Obtaining last offset for topic: {} and partition: {}", topic, partition);
    String groupId = UUID.randomUUID().toString();
    Properties props = basicProperties(groupId, "topic-utils");
    props.setProperty("auto.offset.reset", "earliest");
    KafkaConsumer<String, String> consumer = kafkaConsumerByPartition(topic, props, partition);
    ConsumerRecords<String, String> records = consumer.poll(100);
    if (records.isEmpty()) {
      return -1;
    }
    ConsumerRecord<String, String> record = records.iterator().next();
    return record.offset();
  }

  public KafkaConsumer<String,String> kafkaConsumerByPartition(String topic, Properties props, int partition) {
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    consumer.subscribe(Arrays.asList(topic));
    consumer.assign(Arrays.asList(topicPartition));
    consumer.seekToEnd(topicPartition);
    return consumer;
  }

  private Properties basicProperties(String groupId, String clientId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("group.id", groupId);
    props.put("client.id", clientId);
    props.put("session.timeout.ms", "30000");
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    return props;
  }

}
