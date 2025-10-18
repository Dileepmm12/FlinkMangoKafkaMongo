package com.example.demo.controller;

import com.example.demo.model.JsonData;
import com.example.demo.producer.FlinkMangoProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/flinkmango")
public class FlinkMangoController {

    @Autowired
    private FlinkMangoProducer producer;

    @PostMapping
    public String send(@RequestBody JsonData jsonData) {
        producer.sendMessage(jsonData);
        return "✅ JSON sent to FlinkMango Kafka topic -> " + jsonData.getName();
    }
}
