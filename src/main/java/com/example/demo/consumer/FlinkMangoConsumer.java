package com.example.demo.consumer;

import com.example.demo.flink.FlinkMangoKafkaMongo;
import com.example.demo.model.JsonData;
import com.example.demo.service.FlinkMangoService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class FlinkMangoConsumer {

    private final FlinkMangoService flinkMangoService;
    private final FlinkMangoKafkaMongo flinkMangoKafkaMongo;

    public FlinkMangoConsumer(FlinkMangoService flinkMangoService) {
        this.flinkMangoService = flinkMangoService;
        this.flinkMangoKafkaMongo = new FlinkMangoKafkaMongo();
    }

    @KafkaListener(topics = "flinkmango-topic", groupId = "json_group",
            containerFactory = "jsonKafkaListenerContainerFactory")
    public void consume(JsonData data) {
        System.out.println("✅ Consumed JSON message: " + data);

        // ✅ Call the enrichment method from FlinkMangoKafkaMongo
        JsonData enrichedData = flinkMangoKafkaMongo.parseAndEnrichJson(data);

        if (enrichedData != null) {
            flinkMangoService.saveData(enrichedData);
            System.out.println("✅ Enriched JSON saved: " + enrichedData);
        }
    }
}
