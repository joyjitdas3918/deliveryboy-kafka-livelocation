package com.deliveryboy.deliveryboy.service;


import com.deliveryboy.deliveryboy.config.AppConstants;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class StatusProducer {

    private Logger logger = LoggerFactory.getLogger(StatusProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public boolean sendStatusUpdate(String deliveryBoyId, String status) {
        ProducerRecord<String, String> record = new ProducerRecord<>(AppConstants.STATUS_TOPIC_NAME, deliveryBoyId, status);
        try {
            RecordMetadata metadata = kafkaTemplate.send(record).get().getRecordMetadata();
            logger.info("Status update sent to topic: {} partition: {} offset: {}", metadata.topic(), metadata.partition(), metadata.offset());
            return true;
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error sending status update", e);
            return false;
        }
    }
}