package io.flightdeck.api;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Builds the single {@link KafkaProducer} shared by the chat-api process.
 *
 * <p>A {@code KafkaProducer} is thread-safe and routes each record to the topic
 * named on its {@link org.apache.kafka.clients.producer.ProducerRecord}, so one
 * instance serves every producer wrapper (message-input, tool-use-result,
 * reply-to). Sharing it keeps the process to a single set of broker connections
 * instead of one set per topic — reducing the connection count billed by the
 * Kafka provider.
 */
public final class KafkaProducerFactory {

    private KafkaProducerFactory() {}

    public static KafkaProducer<String, String> create() {
        String bootstrapServers = ChatApiApp.env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

        Properties props = new Properties();
        KafkaEnvProps.apply(props);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        return new KafkaProducer<>(props);
    }
}
