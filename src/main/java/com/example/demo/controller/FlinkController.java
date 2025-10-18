package com.example.demo.controller;



import com.example.demo.dto.WordCount;
import com.example.demo.service.FlinkJobService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/flink")
public class FlinkController {

    private final FlinkJobService flinkJobService;

    public FlinkController(FlinkJobService flinkJobService) {
        this.flinkJobService = flinkJobService;
    }

    @PostMapping("/wordcount")
    public List<WordCount> runWordCount() {
        return flinkJobService.runWordCountJob();
    }

    @PostMapping("/streaming/wordcount")
    public String runStreamingWordCount(@RequestParam String inputPath) {
        return flinkJobService.runStreamingWordCount(inputPath);
    }

    @GetMapping("/health")
    public String health() {
        return "Flink Spring Boot Application is running!";
    }
}
