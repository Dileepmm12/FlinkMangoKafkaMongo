package com.example.demo.consumer;

import com.example.demo.model.JsonData;
import com.example.demo.service.FlinkMangoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class FlinkMangoConsumer {

    @Autowired
    private FlinkMangoService flinkMangoService;

    @KafkaListener(topics = "flinkmango-topic", groupId = "json_group",
            containerFactory = "jsonKafkaListenerContainerFactory")
    public void consume(JsonData data) {
        System.out.println("✅ Consumed JSON message: " + data);

        JsonData enrichedData = flinkMangoService.parseAndEnrichJson(data);

        if (enrichedData != null) {
            flinkMangoService.saveData(enrichedData);
            System.out.println("✅ Enriched JSON saved: " + enrichedData);
        }
    }
}
