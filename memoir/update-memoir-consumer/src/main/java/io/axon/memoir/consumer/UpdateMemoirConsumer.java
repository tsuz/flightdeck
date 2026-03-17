package io.axon.memoir.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.axon.memoir.config.AppConfig;
import io.axon.memoir.model.MemoirSessionEnd;
import io.axon.memoir.service.ClaudeMemoirService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Kafka consumer that reads {@link MemoirSessionEnd} from {@code memoir-context-session-end},
 * calls Claude Sonnet to update the memoir, and produces the updated memoir to {@code memoir-context}.
 */
public class UpdateMemoirConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(UpdateMemoirConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper;
    private final ClaudeMemoirService memoirService;
    private volatile boolean running = true;

    public UpdateMemoirConsumer() {
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        this.consumer = createConsumer();
        this.producer = createProducer();
        this.memoirService = new ClaudeMemoirService(mapper);
    }

    /**
     * Main poll loop — consumes memoir-context-session-end, calls Claude, produces to memoir-context.
     */
    public void run() {
        consumer.subscribe(List.of(AppConfig.INPUT_TOPIC));
        log.info("Update Memoir Consumer started — listening on topic: {}", AppConfig.INPUT_TOPIC);

        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(
                    Duration.ofMillis(AppConfig.POLL_TIMEOUT_MS));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    processRecord(record);
                } catch (Exception e) {
                    log.error("[{}] Failed to process memoir update at offset {}: {}",
                            record.key(), record.offset(), e.getMessage(), e);
                }
            }

            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }

        log.info("Update Memoir Consumer shutting down");
    }

    /**
     * Processes a single memoir-context-session-end record:
     * 1. Deserialize the snapshot (previous memoir + last response)
     * 2. Call Claude Sonnet to generate updated memoir
     * 3. Produce the updated memoir to memoir-context
     */
    void processRecord(ConsumerRecord<String, String> record) throws Exception {
        String sessionId = record.key();

        // 1. Deserialize
        MemoirSessionEnd snapshot = mapper.readValue(record.value(), MemoirSessionEnd.class);

        log.info("[{}] Processing memoir update: has_previous_memoir={} has_response={}",
                sessionId,
                snapshot.memoirContext() != null && !snapshot.memoirContext().isBlank(),
                snapshot.thinkResponse() != null);

        // 2. Call Claude to update memoir
        String updatedMemoir = memoirService.updateMemoir(sessionId, snapshot);

        // 3. Produce updated memoir to memoir-context, keyed by userId
        //    so it persists across sessions for the same user
        String outputKey = snapshot.userId() != null ? snapshot.userId() : sessionId;
        ProducerRecord<String, String> outputRecord = new ProducerRecord<>(
                AppConfig.OUTPUT_TOPIC, outputKey, updatedMemoir);

        producer.send(outputRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("[{}] Failed to produce updated memoir: {}", sessionId, exception.getMessage());
            } else {
                log.info("[{}] Updated memoir produced: partition={} offset={} size={}",
                        sessionId, metadata.partition(), metadata.offset(), updatedMemoir.length());
            }
        });
        producer.flush();
    }

    public void shutdown() {
        running = false;
    }

    @Override
    public void close() {
        shutdown();
        consumer.close();
        producer.close();
    }

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfig.CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return new KafkaConsumer<>(props);
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return new KafkaProducer<>(props);
    }
}
