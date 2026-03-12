package io.axon.streams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Generic Jackson-based Serde for any JSON-serialisable type.
 *
 * <pre>
 *   Serde&lt;ThinkResponse&gt; serde = JsonSerde.of(ThinkResponse.class);
 * </pre>
 */
public class JsonSerde<T> implements Serde<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Class<T> type;

    private JsonSerde(Class<T> type) {
        this.type = type;
    }

    public static <T> JsonSerde<T> of(Class<T> type) {
        return new JsonSerde<>(type);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("JSON serialization failed for " + type.getSimpleName(), e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.readValue(data, type);
            } catch (Exception e) {
                throw new RuntimeException("JSON deserialization failed for " + type.getSimpleName(), e);
            }
        };
    }

    /** Convenience accessor to the shared ObjectMapper (e.g. for processors). */
    public static ObjectMapper mapper() {
        return MAPPER;
    }
}