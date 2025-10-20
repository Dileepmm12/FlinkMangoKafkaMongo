package com.example.demo.flink;

import com.example.demo.model.JsonData;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import com.mongodb.client.model.InsertOneModel;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
public class FlinkMangoKafkaMongo {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        final String KAFKA_SERVERS = "localhost:9092";
        final String KAFKA_TOPIC = "flinkmango-topic";
        final String KAFKA_GROUP_ID = "flinkmango-flink-group";

        final String MONGO_URI = "mongodb://localhost:27017";
        final String MONGO_DB = "testdb";
        final String MONGO_COLLECTION = "json_data";

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.enableCheckpointing(60000);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

            // ---- Kafka Source ----
            KafkaSource<ConsumerRecord<byte[], byte[]>> kafkaSource = KafkaSource
                    .<ConsumerRecord<byte[], byte[]>>builder()
                    .setBootstrapServers(KAFKA_SERVERS)
                    .setTopics(KAFKA_TOPIC)
                    .setGroupId(KAFKA_GROUP_ID)
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setDeserializer(new PassThroughDeserializer())
                    .build();

            // ---- Mongo Sink ----
            MongoSerializationSchema<Document> serializationSchema = (input, context) ->
                    new InsertOneModel<>(input.toBsonDocument());

            MongoSink<Document> mongoSink = MongoSink.<Document>builder()
                    .setUri(MONGO_URI)
                    .setDatabase(MONGO_DB)
                    .setCollection(MONGO_COLLECTION)
                    .setSerializationSchema(serializationSchema)
                    .build();

            // ---- Flink Stream Processing ----
            DataStream<ConsumerRecord<byte[], byte[]>> kafkaStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.noWatermarks(),
                    "Kafka_Source"
            );

            DataStream<Document> mongoStream = kafkaStream.map(record -> {
                String jsonBody = new String(record.value(), StandardCharsets.UTF_8);
                JsonData enriched = parseAndEnrichJson(jsonBody);
                return Document.parse(objectMapper.writeValueAsString(enriched));
            });

            mongoStream.sinkTo(mongoSink);

            env.execute("FlinkMango Kafka to MongoDB Job");

        } catch (Exception e) {
            log.error("❌ Error executing FlinkMango job", e);
        }
    }

    // ---- Helper Methods ----

    public static JsonData parseAndEnrichJson(JsonData data) {
        if (data == null) return null;
        data.setTlogId("TLOG-" + UUID.randomUUID());
        if (data.getName() != null) data.setName(data.getName() + "-enriched");
        data.setAge(data.getAge() + 1);
        return data;
    }

    public static JsonData parseAndEnrichJson(String message) {
        try {
            JsonData incoming = objectMapper.readValue(message, JsonData.class);
            return parseAndEnrichJson(incoming);
        } catch (Exception e) {
            log.error("❌ Failed to parse JSON: {}", message, e);
            return null;
        }
    }

    public static class PassThroughDeserializer
            implements KafkaRecordDeserializationSchema<ConsumerRecord<byte[], byte[]>> {

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record,
                                Collector<ConsumerRecord<byte[], byte[]>> out) {
            out.collect(record);
        }

        @Override
        public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
            return TypeInformation.of(new TypeHint<ConsumerRecord<byte[], byte[]>>() {});
        }
    }
}
