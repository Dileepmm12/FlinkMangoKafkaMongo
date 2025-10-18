package com.example.demo.job;

import com.example.demo.dto.WordCount;
import com.example.demo.model.JsonData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class WordCountJob {

    private static final Logger log = LoggerFactory.getLogger(WordCountJob.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final MongoTemplate mongoTemplate;

    public WordCountJob(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    // ----------------- Batch Word Count -----------------
    public List<WordCount> executeBatchWordCount(StreamExecutionEnvironment env, List<String> words) throws Exception {
        List<WordCount> results = new ArrayList<>();

        DataStream<String> textLines = env.fromCollection(words);

        DataStream<WordCount> wordCounts = textLines
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1)
                .map((MapFunction<Tuple2<String, Integer>, WordCount>) value -> new WordCount(value.f0, value.f1));

        wordCounts.executeAndCollect().forEachRemaining(results::add);

        return results;
    }

    // ----------------- Streaming Word Count from File -----------------
    public void executeStreamingWordCount(StreamExecutionEnvironment env, String inputPath) throws Exception {
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
                .monitorContinuously(Duration.ofSeconds(1))
                .build();

        DataStream<String> textLines = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        );

        DataStream<WordCount> wordCounts = textLines
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .map((MapFunction<Tuple2<String, Integer>, WordCount>) value -> new WordCount(value.f0, value.f1));

        wordCounts.print("Streaming WordCount");
    }

    // ----------------- Kafka JSON Processing Job -----------------
    public void executeKafkaJsonJob(StreamExecutionEnvironment env, String topic) throws Exception {

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("flink-json-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> messages = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        DataStream<JsonData> enrichedStream = messages
                .map(this::parseAndEnrichJson)
                .filter(data -> data != null);

        enrichedStream.print("Kafka Enriched Message");

        env.execute("Flink Kafka JSON Processor");
    }

    // ----------------- Helper Method for JSON Parsing and Enrichment -----------------
    // Updated to accept JsonData object directly
    public JsonData parseAndEnrichJson(JsonData data) {
        if (data == null) return null;

        try {
            // ✅ Enrich the existing object
            data.setTlogId("TLOG-" + UUID.randomUUID());

            log.info("✅ Enriched JSON message: {}", data);

            return data;
        } catch (Exception e) {
            log.error("Failed to enrich JSON data: {}", data, e);
            return null;
        }
    }

    // Overloaded method to handle String input (keep for Kafka streaming)
    public JsonData parseAndEnrichJson(String message) {
        try {
            JsonData incoming = objectMapper.readValue(message, JsonData.class);
            return parseAndEnrichJson(incoming);
        } catch (Exception e) {
            log.error("Failed to parse JSON: {}", message, e);
            return null;
        }
    }

    // ----------------- Tokenizer for Word Count -----------------
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            if (value == null || value.isEmpty()) return;

            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (!token.isBlank()) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
