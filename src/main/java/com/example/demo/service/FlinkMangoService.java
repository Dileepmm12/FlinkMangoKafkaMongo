package com.example.demo.service;

import com.example.demo.model.JsonData;
import com.example.demo.repository.JsonRepository;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class FlinkMangoService {

    private final JsonRepository jsonRepository;

    public FlinkMangoService(JsonRepository jsonRepository) {
        this.jsonRepository = jsonRepository;
    }

    /**
     * Save JsonData to MongoDB
     */
    public void saveData(JsonData data) {
        if (data != null) {
            jsonRepository.save(data);
            System.out.println("💾 Data saved to MongoDB: " + data);
        }
    }

    /**
     * Parse and enrich a JsonData object
     * Example enrichment:
     *  - Append "-enriched" to name
     *  - Increment age by 1
     *  - Generate a random TLOG ID
     */
    public JsonData parseAndEnrichJson(JsonData data) {
        if (data == null) return null;

        JsonData enriched = new JsonData();
        enriched.setId(data.getId()); // preserve existing ID
        enriched.setName(data.getName() != null ? data.getName() + "-enriched" : null);
        enriched.setAge(data.getAge() + 1);
        enriched.setTlogId("TLOG-" + UUID.randomUUID());

        return enriched;
    }
}
