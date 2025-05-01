package com.deliveryboy.deliveryboy.controller;

import com.deliveryboy.deliveryboy.service.KafkaService;
import com.deliveryboy.deliveryboy.service.LocationProducer;
import com.deliveryboy.deliveryboy.service.StatusProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/delivery-boy")
public class DeliveryBoyController {

    @Autowired
    private LocationProducer locationProducer;

    @Autowired
    private StatusProducer statusProducer;

    @Autowired
    private KafkaService kafkaService;
    @CrossOrigin(origins = "http://127.0.0.1:5500")
    @PostMapping("/update-location")
    public ResponseEntity<?> updateLocation(@RequestParam String deliveryBoyId) {
        String location = "(" + Math.random() * 100 + "," + Math.random() * 100 + ")";
        this.locationProducer.sendLocationUpdate(deliveryBoyId, location);
        return new ResponseEntity<>(Map.of("message", "Location Updated"), HttpStatus.OK);
    }

    @CrossOrigin(origins = "http://127.0.0.1:5500")
    @PostMapping("/update-status")
    public ResponseEntity<?> updateStatus(@RequestParam String deliveryBoyId, @RequestParam String status) {
        this.statusProducer.sendStatusUpdate(deliveryBoyId, status);
        return new ResponseEntity<>(Map.of("message", "Status Updated"), HttpStatus.OK);
    }

    @CrossOrigin(origins = "http://127.0.0.1:5500")
    @GetMapping("/describe-cluster")
    public ResponseEntity<?> describeCluster() {
        this.kafkaService.describeCluster();
        return new ResponseEntity<>(Map.of("message", "Cluster description logged"), HttpStatus.OK);
    }

    @CrossOrigin(origins = "http://127.0.0.1:5500")
    @GetMapping("/describe-topics")
    public ResponseEntity<?> describeTopics() {
        this.kafkaService.describeTopics();
        return new ResponseEntity<>(Map.of("message", "Topic descriptions logged"), HttpStatus.OK);
    }
}