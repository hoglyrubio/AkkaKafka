package com.hogly.kafka;

import com.typesafe.config.Config;
import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class KafkaAdmin implements Closeable {
  private final ZkClient zkClient;
  private final ZkUtils zkUtils;

  public static KafkaAdmin create(Config config) {
    String zookeeperHost = config.getString("kafka.zookeeper-host");
    int sessionTimeout = config.getInt("kafka.zookeeper-session-timeout-ms");
    int connectionTimeout = config.getInt("kafka.zookeeper-connection-timeout-ms");
    return new KafkaAdmin(zookeeperHost, sessionTimeout, connectionTimeout);
  }

  private KafkaAdmin(String zookeeperHosts, int sessionTimeOutInMs, int connectionTimeOutInMs) {
    zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
    zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);
  }

  public boolean topicExists(String topicName) {
    return AdminUtils.topicExists(zkUtils, topicName);
  }

  public void createTopic(String topicName, int noOfPartitions, int noOfReplicas) {
    createTopic(topicName, noOfPartitions, noOfReplicas, new Properties());
  }

  public void createTopic(String topicName, int noOfPartitions, int noOfReplicas, Properties topicConfiguration) {
    AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplicas, topicConfiguration);
  }

  public int partitions(String topicName) {
    TopicMetadata metadata = AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils);
    return metadata.partitionsMetadata().size();
  }

  @Override public void close() throws IOException {
    if (zkClient != null) {
      zkClient.close();
    }
  }

}
