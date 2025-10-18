package com.example.demo.service;

import com.example.demo.model.JsonData;
import com.example.demo.repository.JsonRepository;
import org.springframework.stereotype.Service;

@Service
public class JsonService {

    private final JsonRepository jsonRepository;

    public JsonService(JsonRepository jsonRepository) {
        this.jsonRepository = jsonRepository;
    }

    public void saveData(JsonData data) {
        jsonRepository.save(data);
        System.out.println("💾 Data saved to MongoDB: " + data);
    }
}
