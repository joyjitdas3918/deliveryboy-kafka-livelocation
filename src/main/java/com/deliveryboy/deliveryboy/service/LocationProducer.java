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
public class LocationProducer {

    private Logger logger = LoggerFactory.getLogger(LocationProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public boolean sendLocationUpdate(String deliveryBoyId, String location) {
        ProducerRecord<String, String> record = new ProducerRecord<>(AppConstants.LOCATION_TOPIC_NAME, deliveryBoyId, location);
        try {
            RecordMetadata metadata = kafkaTemplate.send(record).get().getRecordMetadata();
            logger.info("Location update sent to topic: {} partition: {} offset: {}", metadata.topic(), metadata.partition(), metadata.offset());
            return true;
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error sending location update", e);
            return false;
        }
    }
}
