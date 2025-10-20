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

    // Kafka configuration
    private static final String KAFKA_SERVERS = "localhost:9092";
    private static final String KAFKA_TOPIC = "flinkmango-topic";
    private static final String KAFKA_GROUP_ID = "flinkmango-flink-group";
    private static final String KAFKA_USERNAME = "your-username";
    private static final String KAFKA_PASSWORD = "your-password";
    private static final String KAFKA_HEADER_TLOGID = "tlogId";

    // MongoDB configuration
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String MONGO_DB = "testdb";
    private static final String MONGO_COLLECTION = "json_data";
    private static final int MONGO_BATCH_SIZE = 100;
    private static final int MONGO_BATCH_INTERVAL_MS = 1000;
    private static final int MONGO_MAX_RETRIES = 3;

    public static void main(String[] args) {

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
                    .setProperty("security.protocol", "SASL_SSL")
                    .setProperty("sasl.mechanism", "PLAIN")
                    .setProperty("sasl.jaas.config",
                            "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                    + "username=\"" + KAFKA_USERNAME + "\" "
                                    + "password=\"" + KAFKA_PASSWORD + "\";")
                    .build();

            // ---- Mongo Sink ----
            MongoSerializationSchema<Document> serializationSchema = (input, context) ->
                    new InsertOneModel<>(input.toBsonDocument());

            MongoSink<Document> mongoSink = MongoSink.<Document>builder()
                    .setUri(MONGO_URI)
                    .setDatabase(MONGO_DB)
                    .setCollection(MONGO_COLLECTION)
                    .setBatchSize(MONGO_BATCH_SIZE)
                    .setBatchIntervalMs(MONGO_BATCH_INTERVAL_MS)
                    .setMaxRetries(MONGO_MAX_RETRIES)
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

                // Parse and enrich JSON as POJO
                JsonData data = parseAndEnrichJson(jsonBody);

                if (data == null) return null;

                Document document = Document.parse(objectMapper.writeValueAsString(data));

                // Extract tlogId header if present
                byte[] tlogIdBytes = record.headers().lastHeader(KAFKA_HEADER_TLOGID) != null
                        ? record.headers().lastHeader(KAFKA_HEADER_TLOGID).value()
                        : null;

                if (tlogIdBytes != null && tlogIdBytes.length > 0) {
                    String tlogId = new String(tlogIdBytes, StandardCharsets.UTF_8);
                    document.put(KAFKA_HEADER_TLOGID, tlogId);
                }

                return document;
            }).name("Parse, Enrich & Extract Header");

            mongoStream.sinkTo(mongoSink).name("MongoDB_Sink");

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
        data.setAge(data.getAge());
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

    // ---- Custom Deserializer ----
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
