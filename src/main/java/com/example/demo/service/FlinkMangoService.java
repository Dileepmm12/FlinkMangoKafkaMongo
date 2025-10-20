package com.example.demo.service;

import com.example.demo.model.JsonData;
import com.example.demo.repository.JsonRepository;
import org.springframework.stereotype.Service;

@Service
public class FlinkMangoService {

    private final JsonRepository jsonRepository;

    public FlinkMangoService(JsonRepository jsonRepository) {
        this.jsonRepository = jsonRepository;
    }

    // ✅ Save to MongoDB
    public void saveData(JsonData data) {
        if (data != null) {
            jsonRepository.save(data);
            System.out.println("💾 Data saved to MongoDB: " + data);
        }
    }
}
