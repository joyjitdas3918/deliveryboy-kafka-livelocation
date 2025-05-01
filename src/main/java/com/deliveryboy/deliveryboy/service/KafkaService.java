package com.deliveryboy.deliveryboy.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaService {

    private Logger logger = LoggerFactory.getLogger(KafkaService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ProducerFactory<String, String> producerFactory;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    public void describeCluster() {
        try (AdminClient adminClient = AdminClient.create(producerFactory.getConfigurationProperties())) {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            Collection<Node> nodes = clusterResult.nodes().get();
            String clusterId = clusterResult.clusterId().get();
            Node controller = clusterResult.controller().get();

            logger.info("Cluster ID: {}", clusterId);
            logger.info("Controller: {}", controller);
            for (Node node : nodes) {
                logger.info("Node: {}", node);
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error describing cluster", e);
        }
    }

    public void describeTopics() {
        try (AdminClient adminClient = AdminClient.create(producerFactory.getConfigurationProperties())) {
            ListTopicsResult topics = adminClient.listTopics();
            Map<String, TopicDescription> descriptions = adminClient.describeTopics(topics.names().get()).all().get();

            for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                logger.info("Topic: {}", entry.getKey());
                for (TopicPartitionInfo partition : entry.getValue().partitions()) {
                    logger.info("Partition: {}, Leader: {}, Replicas: {}",
                            partition.partition(), partition.leader(), partition.replicas());
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error describing topics", e);
        }
    }
}