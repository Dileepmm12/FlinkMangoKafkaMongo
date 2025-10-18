package com.example.demo.controller;

import com.example.demo.model.JsonData;
import com.example.demo.producer.JsonProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class JsonController {

    @Autowired
    private JsonProducer producer;

    @PostMapping("/publish")
    public String publishJson(@RequestBody JsonData jsonData) {
        producer.sendToKafka(jsonData);
        return "✅ JSON sent to Kafka successfully!";
    }
}
