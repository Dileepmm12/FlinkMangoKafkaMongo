package com.example.demo.consumer;

import com.example.demo.job.WordCountJob;
import com.example.demo.model.JsonData;
import com.example.demo.service.JsonService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class JsonConsumer {

    private final JsonService jsonService;
    private final WordCountJob wordCountJob; // Inject the job class that has parseAndEnrichJson

    public JsonConsumer(JsonService jsonService, WordCountJob wordCountJob) {
        this.jsonService = jsonService;
        this.wordCountJob = wordCountJob;
    }

    @KafkaListener(topics = "json_topic", groupId = "json_group",
            containerFactory = "jsonKafkaListenerContainerFactory")
    public void consume(JsonData data) {
        System.out.println("✅ Consumed JSON message: " + data);

        // ✅ Call the parseAndEnrichJson method
        JsonData enrichedData = wordCountJob.parseAndEnrichJson(data);

        if (enrichedData != null) {
            // Save enriched data using JsonService
            jsonService.saveData(enrichedData);
            System.out.println("✅ Enriched JSON saved: " + enrichedData);
        }
    }
}
