package com.example.demo.producer;

import com.example.demo.model.JsonData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class JsonProducer {

    private static final String TOPIC = "json_topic";

    @Autowired
    private KafkaTemplate<String, JsonData> kafkaTemplate;

    public void sendToKafka(JsonData data) {
        kafkaTemplate.send(TOPIC, data);
        System.out.println("📤 Sent to Kafka -> " + data.getName());
    }
}
