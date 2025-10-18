package com.example.demo.producer;

import com.example.demo.model.JsonData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FlinkMangoProducer {

    private static final String TOPIC = "flinkmango-topic";

    @Autowired
    private KafkaTemplate<String, JsonData> kafkaTemplate;

    public void sendMessage(JsonData jsonData) {
        kafkaTemplate.send(TOPIC, jsonData);
    }
}
