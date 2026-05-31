package io.flightdeck.api;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Produces reply-routing descriptors to the {@code {AGENT_NAME}-reply-to} topic,
 * keyed by session_id.
 *
 * <p>Used in two places:
 * <ul>
 *   <li>{@link ChatHandler} writes a descriptor when an inbound {@code /api/chat}
 *       request carries a {@code reply} object — establishing where this session's
 *       terminal response should be delivered.</li>
 *   <li>{@link OutputConsumer} writes a tombstone (null value) after it has
 *       successfully delivered a session's response, so a one-shot sub-agent call
 *       cannot be double-delivered.</li>
 * </ul>
 *
 * <p>The topic is compacted (latest-per-key) with a time-based retention, so the
 * descriptor lives only until it is tombstoned or until {@code REPLY_TO_STATE_TTL_MS}
 * elapses, whichever comes first.
 */
public class ReplyToProducer {

    private static final Logger log = LoggerFactory.getLogger(ReplyToProducer.class);

    private static final String AGENT_NAME = ChatApiApp.requireEnv("AGENT_NAME");
    private static final String TOPIC = AGENT_NAME + "-reply-to";

    private static final String BOOTSTRAP_SERVERS =
            ChatApiApp.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    private final KafkaProducer<String, String> producer;

    public ReplyToProducer() {
        Properties props = new Properties();
        KafkaEnvProps.apply(props);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
        log.info("Reply-to producer initialized — bootstrap={} topic={}", BOOTSTRAP_SERVERS, TOPIC);
    }

    /**
     * Writes (or overwrites) the reply-routing descriptor for a session.
     *
     * <p>TODO(multi-agent): keyed by sessionId, so only one outstanding reply
     * route per session is supported (a second write overwrites the first). For
     * multiple concurrent callbacks within one session, key by
     * {@code sessionId:requestId}. Deferred — see Topics.REPLY_TO.
     */
    public void send(String sessionId, String descriptorJson) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, sessionId, descriptorJson);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("[{}] Failed to produce reply-to descriptor to {}", sessionId, TOPIC, exception);
            } else {
                log.debug("[{}] Wrote reply-to descriptor to {}", sessionId, TOPIC);
            }
        });
    }

    /** Tombstones (null value) a session's reply-routing descriptor after delivery. */
    public void tombstone(String sessionId) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, sessionId, null);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("[{}] Failed to tombstone reply-to descriptor on {}", sessionId, TOPIC, exception);
            } else {
                log.debug("[{}] Tombstoned reply-to descriptor on {}", sessionId, TOPIC);
            }
        });
    }

    public void close() {
        producer.close();
        log.info("Reply-to producer closed");
    }
}
