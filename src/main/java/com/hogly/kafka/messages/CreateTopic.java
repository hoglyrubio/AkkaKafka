package com.hogly.kafka.messages;

public class CreateTopic {

  private String topicName;
  private int partions;
  private int replicas;

  private CreateTopic(String topicName, int partions, int replicas) {
    this.topicName = topicName;
    this.partions = partions;
    this.replicas = replicas;
  }

  public static CreateTopic of(String topicName, int partions, int replicas) {
    return new CreateTopic(topicName, partions, replicas);
  }

  public String topicName() {
    return topicName;
  }

  public int partions() {
    return partions;
  }

  public int replicas() {
    return replicas;
  }

  @Override
  public String toString() {
    return "CreateTopic{" +
      "topicName='" + topicName + '\'' +
      ", partions=" + partions +
      ", replicas=" + replicas +
      '}';
  }
}
