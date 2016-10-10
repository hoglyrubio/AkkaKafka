package com.hogly.kafka.messages;

public class ObtainLastOffset {

  private String bootstrapServers;
  private String topic;
  private int partition;

  ObtainLastOffset(String bootstrapServers, String topic, int partition) {
    this.bootstrapServers = bootstrapServers;
    this.topic = topic;
    this.partition = partition;
  }

  public static ObtainLastOffset of(String bootstrapServers, String topic, int partition) {
    return new ObtainLastOffset(bootstrapServers, topic, partition);
  }

  public String bootstrapServers() {
    return bootstrapServers;
  }

  public String topic() {
    return topic;
  }

  public int partition() {
    return partition;
  }

  @Override public String toString() {
    return "ObtainLastOffset{" + "bootstrapServers='" + bootstrapServers + '\'' + ", topic='" + topic + '\'' + ", partition=" + partition + '}';
  }
}
