package io.flightdeck.api;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * <p>Wraps the process-wide shared {@link Producer} (see
 * {@link KafkaProducerFactory}); it does not own the producer and so does not
 * close it.
 */
public class ReplyToProducer {

    private static final Logger log = LoggerFactory.getLogger(ReplyToProducer.class);

    private static final String AGENT_NAME = ChatApiApp.requireEnv("AGENT_NAME");
    private static final String TOPIC = AGENT_NAME + "-reply-to";

    private final Producer<String, String> producer;

    public ReplyToProducer(Producer<String, String> producer) {
        this.producer = producer;
        log.info("Reply-to producer wired to shared producer — topic={}", TOPIC);
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
}
