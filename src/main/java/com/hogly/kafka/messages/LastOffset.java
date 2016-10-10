package com.hogly.kafka.messages;

public class LastOffset {

  private String topic;
  private int partition;
  private long offset;

  LastOffset(String topic, int partition, long offset) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
  }

  public static LastOffset of(String topic, int partition, long offset) {
    return new LastOffset(topic, partition, offset);
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  public long offset() {
    return offset;
  }

  @Override public String toString() {
    return "LastOffset{" + "topic='" + topic + '\'' + ", partition=" + partition + ", offset=" + offset + '}';
  }
}
