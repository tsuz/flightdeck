package io.flightdeck.think.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.flightdeck.think.model.FullSessionContext;
import io.flightdeck.think.model.MessageInput;
import io.flightdeck.think.model.ThinkResponse;
import io.flightdeck.think.service.LlmApiService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests that ThinkConsumer compaction logic works:
 * when user message count in history >= COMPACTION_USER_MESSAGE_THRESHOLD,
 * the consumer first calls LLM to summarize old history, then calls LLM
 * with the compacted history + latest input.
 *
 * Requires COMPACTION_USER_MESSAGE_TRIGGER=3, COMPACTION_USER_MESSAGE_UNTIL=2
 * (set via surefire env vars in pom.xml).
 */
class CompactionTest {

    private static final String TS = "2026-03-10T12:00:00Z";
    private static final String SESSION = "sess-compact";
    private static final String USER = "user-1";

    private LlmApiService mockLlm;
    private CapturingProducer capturingProducer;
    private ObjectMapper mapper;
    private ThinkConsumer thinkConsumer;

    @BeforeEach
    void setUp() {
        mockLlm = mock(LlmApiService.class);
        capturingProducer = new CapturingProducer();
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        thinkConsumer = new ThinkConsumer(mockLlm, capturingProducer, mapper);
    }

    @Test
    @DisplayName("Compaction includes assistant tool_use and tool results in the summarized portion")
    void compaction_includesToolUseAndToolResults() throws Exception {
        // Build a realistic history with tool interactions in the old (compacted) portion.
        // TRIGGER=3, START=2. 3 user messages → compaction fires, keeps last 2 user msgs.
        //
        // History:
        //   [0] user:      "Search for flights"        ← user msg #1 (compacted)
        //   [1] assistant:  tool_use content            ← tool_use block (compacted)
        //   [2] tool:       tool result                 ← tool result (compacted)
        //   [3] assistant:  "Found 3 flights..."        ← (compacted)
        //   ---- split here (index 4) ----
        //   [4] user:      "Book the cheapest"          ← user msg #2 (kept)
        //   [5] assistant:  "Booked flight AA123."       ← (kept)
        //   [6] user:      "Show confirmation"           ← user msg #3 (kept)
        //   [7] assistant:  "Here's your confirmation."  ← (kept)

        List<MessageInput> history = new ArrayList<>();
        // Old portion — includes tool_use + tool result
        history.add(userMsg("Search for flights"));                                     // 0
        history.add(new MessageInput(SESSION, USER, "assistant",                        // 1
                List.of(Map.of("type", "tool_use", "id", "t1", "name", "search_flights",
                        "input", Map.of("dest", "NYC"))),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",                             // 2
                List.of(Map.of("tool_use_id", "t1", "content",
                        "{\"flights\":[\"AA123\",\"UA456\",\"DL789\"]}")),
                TS, null));
        history.add(assistantMsg("Found 3 flights: AA123, UA456, DL789."));             // 3
        // Kept portion
        history.add(userMsg("Book the cheapest"));                                      // 4
        history.add(assistantMsg("Booked flight AA123."));                              // 5
        history.add(userMsg("Show confirmation"));                                      // 6
        history.add(assistantMsg("Here's your confirmation."));                         // 7

        MessageInput latestInput = userMsg("Thanks!");

        FullSessionContext context = new FullSessionContext(
                SESSION, USER, 0.10, history, latestInput, null, TS);

        // --- Mock: compaction call summarizes old messages (indices 0-3) ---
        List<MessageInput> expectedOld = history.subList(0, 4);
        List<Map<String, Object>> oldApiMessages = List.of(
                Map.of("role", "user", "content", "Search for flights"),
                Map.of("role", "assistant", "content", "[tool_use:search_flights]"),
                Map.of("role", "user", "content", "[tool_result]"),
                Map.of("role", "assistant", "content", "Found 3 flights: AA123, UA456, DL789."));
        when(mockLlm.toApiMessages(expectedOld, null))
                .thenReturn(oldApiMessages);

        ThinkResponse summaryResponse = new ThinkResponse(
                SESSION, USER, 0.002, null, 80, 40,
                List.of(assistantMsg("User searched flights to NYC. Found AA123, UA456, DL789.")),
                null, true, TS, null);
        when(mockLlm.call(
                eq(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT),
                eq(oldApiMessages),
                eq(SESSION),
                eq(USER)))
                .thenReturn(summaryResponse);

        // --- Mock: main LLM call uses compacted history ---
        // Compacted = [summary, msg4, msg5, msg6, msg7] (5 messages)
        List<Map<String, Object>> mainApiMessages = List.of(
                Map.of("role", "assistant", "content", "[Conversation Summary]\n..."));
        when(mockLlm.toApiMessages(anyList(), eq(latestInput)))
                .thenReturn(mainApiMessages);

        ThinkResponse mainResponse = new ThinkResponse(
                SESSION, USER, 0.01, null, 200, 100,
                List.of(assistantMsg("You're welcome!")),
                null, true, TS, null);
        when(mockLlm.call(anyString(), eq(mainApiMessages), eq(SESSION), eq(USER)))
                .thenReturn(mainResponse);

        // --- Execute ---
        String recordValue = mapper.writeValueAsString(context);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION, recordValue);
        thinkConsumer.processRecord(record);

        // --- Verify ---
        verify(mockLlm, times(2)).call(anyString(), anyList(), eq(SESSION), eq(USER));

        // The compaction call received all 4 old messages (user + tool_use + tool_result + assistant)
        ArgumentCaptor<List<MessageInput>> historyCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockLlm, times(2)).toApiMessages(historyCaptor.capture(), any());
        List<MessageInput> compactionInput = historyCaptor.getAllValues().get(0);
        assertThat(compactionInput).hasSize(4);
        assertThat(compactionInput.get(0).role()).isEqualTo("user");
        assertThat(compactionInput.get(1).role()).isEqualTo("assistant");
        assertThat(compactionInput.get(1).content()).isInstanceOf(List.class); // tool_use block
        assertThat(compactionInput.get(2).role()).isEqualTo("tool");           // tool result
        assertThat(compactionInput.get(3).role()).isEqualTo("assistant");

        // The main call received compacted history (summary + 4 kept messages)
        List<MessageInput> mainInput = historyCaptor.getAllValues().get(1);
        assertThat(mainInput).hasSize(5);
        assertThat(mainInput.get(0).contentAsString()).startsWith("[Conversation Summary]");
        assertThat(mainInput.get(1).contentAsString()).isEqualTo("Book the cheapest");
        assertThat(mainInput.get(4).contentAsString()).isEqualTo("Here's your confirmation.");

        // Produced response
        ThinkResponse produced = mapper.readValue(
                capturingProducer.records.get(0).value(), ThinkResponse.class);
        assertThat(produced.compactedHistory()).hasSize(5);
    }

    @Test
    @DisplayName("User messages < COMPACTION_USER_MESSAGE_TRIGGER → no compaction call, single LLM call")
    void noCompaction_userMessagesBelowTrigger() throws Exception {
        // 2 user messages in history, COMPACTION_USER_MESSAGE_TRIGGER=3 → no compaction
        List<MessageInput> history = new ArrayList<>();
        history.add(userMsg("Hello"));
        history.add(assistantMsg("Hi!"));
        history.add(userMsg("How are you?"));
        history.add(assistantMsg("I'm good."));

        MessageInput latestInput = userMsg("Cool");

        FullSessionContext context = new FullSessionContext(
                SESSION, USER, 0.02, history, latestInput, null, TS);

        List<Map<String, Object>> apiMessages = List.of(
                Map.of("role", "user", "content", "Hello"));
        when(mockLlm.toApiMessages(eq(history), eq(latestInput)))
                .thenReturn(apiMessages);

        ThinkResponse mainResponse = new ThinkResponse(
                SESSION, USER, 0.01, null, 100, 50,
                List.of(assistantMsg("Nice!")),
                null, true, TS, null);
        when(mockLlm.call(anyString(), eq(apiMessages), eq(SESSION), eq(USER)))
                .thenReturn(mainResponse);

        String recordValue = mapper.writeValueAsString(context);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION, recordValue);
        thinkConsumer.processRecord(record);

        // Only ONE LLM call — no compaction summarization call
        verify(mockLlm, times(1)).call(anyString(), anyList(), eq(SESSION), eq(USER));

        // The compaction prompt was never used
        ArgumentCaptor<String> promptCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockLlm).call(promptCaptor.capture(), anyList(), eq(SESSION), eq(USER));
        assertThat(promptCaptor.getValue())
                .isNotEqualTo(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT);

        // toApiMessages was called once with the full (uncompacted) history
        verify(mockLlm, times(1)).toApiMessages(eq(history), eq(latestInput));

        // Produced response has null compactedHistory
        assertThat(capturingProducer.records).hasSize(1);
        String producedJson = capturingProducer.records.get(0).value();
        ThinkResponse produced = mapper.readValue(producedJson, ThinkResponse.class);
        assertThat(produced.compactedHistory()).isNull();
    }

    @Test
    @DisplayName("Mid-tool-loop skips compaction even when threshold is exceeded")
    void noCompaction_midToolLoop() throws Exception {
        // 3 user messages but last message has tool role
        List<MessageInput> history = new ArrayList<>();
        history.add(userMsg("Search for X"));
        history.add(assistantMsg("I'll search for X."));
        history.add(userMsg("Also search Y"));
        history.add(assistantMsg("Searching..."));
        history.add(userMsg("And Z"));
        // Last two: a tool_use assistant msg + tool result
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t1", "name", "search", "input", Map.of())),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t1", "content", "result")),
                TS, null));

        MessageInput latestInput = null;

        FullSessionContext context = new FullSessionContext(
                SESSION, USER, 0.05, history, latestInput, null, TS);

        List<Map<String, Object>> apiMessages = List.of(
                Map.of("role", "user", "content", "test"));
        when(mockLlm.toApiMessages(eq(history), eq(latestInput)))
                .thenReturn(apiMessages);

        ThinkResponse mainResponse = new ThinkResponse(
                SESSION, USER, 0.01, null, 100, 50,
                List.of(assistantMsg("Here are the results.")),
                null, true, TS, null);
        when(mockLlm.call(anyString(), eq(apiMessages), eq(SESSION), eq(USER)))
                .thenReturn(mainResponse);

        String recordValue = mapper.writeValueAsString(context);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION, recordValue);
        thinkConsumer.processRecord(record);

        // Only ONE call — compaction was skipped due to mid-tool-loop
        verify(mockLlm, times(1)).call(anyString(), anyList(), eq(SESSION), eq(USER));
    }

    // ── COMPACTION_PROMPT ──────────────────────────────────────────────────

    @Test
    @DisplayName("COMPACTION_PROMPT is used as the system prompt for the summarization LLM call")
    void compactionPrompt_usedForSummarizationCall() throws Exception {
        // 3 user messages → triggers compaction (TRIGGER=3, UNTIL=2)
        List<MessageInput> history = new ArrayList<>();
        history.add(userMsg("msg1"));
        history.add(assistantMsg("reply1"));
        history.add(userMsg("msg2"));
        history.add(assistantMsg("reply2"));
        history.add(userMsg("msg3"));
        history.add(assistantMsg("reply3"));

        MessageInput latestInput = userMsg("msg4");
        FullSessionContext context = new FullSessionContext(
                SESSION, USER, 0.0, history, latestInput, null, TS);

        // Mock compaction call
        when(mockLlm.toApiMessages(history.subList(0, 2), null))
                .thenReturn(List.of(Map.of("role", "user", "content", "msg1")));
        when(mockLlm.call(eq(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT),
                anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.0, null, 10, 10,
                        List.of(assistantMsg("summary")), null, true, TS, null));

        // Mock main call
        when(mockLlm.toApiMessages(anyList(), eq(latestInput)))
                .thenReturn(List.of(Map.of("role", "user", "content", "msg4")));
        when(mockLlm.call(anyString(), anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.01, null, 50, 50,
                        List.of(assistantMsg("done")), null, true, TS, null));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION,
                mapper.writeValueAsString(context)));

        // Capture both LLM calls
        ArgumentCaptor<String> promptCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockLlm, times(2)).call(promptCaptor.capture(), anyList(), eq(SESSION), eq(USER));

        String compactionCallPrompt = promptCaptor.getAllValues().get(0);
        String mainCallPrompt = promptCaptor.getAllValues().get(1);

        // The compaction call uses exactly COMPACTION_PROMPT (controlled via env var)
        assertThat(compactionCallPrompt)
                .isEqualTo(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT);
        assertThat(compactionCallPrompt)
                .contains("Summarize the following conversation");

        // The main call uses the system prompt, NOT the compaction prompt
        assertThat(mainCallPrompt)
                .isNotEqualTo(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT);
    }

    // ── findCompactionSplitIndex unit tests ─────────────────────────────────

    @Test
    @DisplayName("findCompactionSplitIndex: 5 user msgs, keepLast=2 → split at 4th user msg")
    void splitIndex_fiveUsers_keepTwo() {
        // u0 a1 u2 a3 u4 a5 u6 a7 u8 a9
        // user msgs at indices: 0, 2, 4, 6, 8 (5 total)
        // keepLast=2 → keep user msgs #4 and #5 → split at index 6
        List<MessageInput> history = List.of(
                userMsg("u1"), assistantMsg("a1"),
                userMsg("u2"), assistantMsg("a2"),
                userMsg("u3"), assistantMsg("a3"),
                userMsg("u4"), assistantMsg("a4"),
                userMsg("u5"), assistantMsg("a5"));
        int idx = ThinkConsumer.findCompactionSplitIndex(history, 2);
        assertThat(idx).isEqualTo(6); // index of 4th user msg ("u4")
        // Messages kept: u4, a4, u5, a5
        assertThat(history.get(idx).contentAsString()).isEqualTo("u4");
    }

    @Test
    @DisplayName("findCompactionSplitIndex: 3 user msgs, keepLast=2 → split at 2nd user msg")
    void splitIndex_threeUsers_keepTwo() {
        List<MessageInput> history = List.of(
                userMsg("u1"), assistantMsg("a1"),
                userMsg("u2"), assistantMsg("a2"),
                userMsg("u3"), assistantMsg("a3"));
        int idx = ThinkConsumer.findCompactionSplitIndex(history, 2);
        assertThat(idx).isEqualTo(2); // index of 2nd user msg
        assertThat(history.get(idx).contentAsString()).isEqualTo("u2");
    }

    @Test
    @DisplayName("findCompactionSplitIndex: 3 user msgs, UNTIL=3 → returns -1 (nothing before first user msg)")
    void splitIndex_untilEqualsTotal_noCompaction() {
        List<MessageInput> history = List.of(
                userMsg("u1"), assistantMsg("a1"),
                userMsg("u2"), assistantMsg("a2"),
                userMsg("u3"), assistantMsg("a3"));
        // UNTIL=3 means keep last 3 user messages — that's all of them, nothing to compact
        assertThat(ThinkConsumer.findCompactionSplitIndex(history, 3)).isEqualTo(-1);
    }

    @Test
    @DisplayName("findCompactionSplitIndex: user msgs <= keepLast → returns -1")
    void splitIndex_notEnoughUsers() {
        List<MessageInput> history = List.of(
                userMsg("u1"), assistantMsg("a1"),
                userMsg("u2"), assistantMsg("a2"));
        assertThat(ThinkConsumer.findCompactionSplitIndex(history, 2)).isEqualTo(-1);
        assertThat(ThinkConsumer.findCompactionSplitIndex(history, 3)).isEqualTo(-1);
    }

    @Test
    @DisplayName("findCompactionSplitIndex: empty or null history → returns -1")
    void splitIndex_emptyHistory() {
        assertThat(ThinkConsumer.findCompactionSplitIndex(null, 2)).isEqualTo(-1);
        assertThat(ThinkConsumer.findCompactionSplitIndex(List.of(), 2)).isEqualTo(-1);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static MessageInput userMsg(String content) {
        return new MessageInput(SESSION, USER, "user", content, TS, null);
    }

    private static MessageInput assistantMsg(String content) {
        return new MessageInput(SESSION, USER, "assistant", content, TS, null);
    }

    /**
     * Minimal Producer implementation that captures sent records for assertions.
     * Avoids mocking issues with Kafka Producer interface on modern JVMs.
     */
    static class CapturingProducer implements Producer<String, String> {
        final List<ProducerRecord<String, String>> records = new ArrayList<>();

        @Override public Future<RecordMetadata> send(ProducerRecord<String, String> r) { records.add(r); return null; }
        @Override public Future<RecordMetadata> send(ProducerRecord<String, String> r, Callback cb) {
            records.add(r);
            if (cb != null) {
                RecordMetadata meta = new RecordMetadata(new TopicPartition(r.topic(), 0), 0, 0, 0, 0, 0);
                cb.onCompletion(meta, null);
            }
            return null;
        }
        @Override public void flush() {}
        @Override public List<PartitionInfo> partitionsFor(String topic) { return List.of(); }
        @Override public Map<MetricName, ? extends Metric> metrics() { return Map.of(); }
        @Override public void close() {}
        @Override public void close(Duration timeout) {}
        @Override public void initTransactions() {}
        @Override public void beginTransaction() throws ProducerFencedException {}
        @Override public void commitTransaction() throws ProducerFencedException {}
        @Override public void abortTransaction() throws ProducerFencedException {}
        @Override public void sendOffsetsToTransaction(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {}
        @Override public void sendOffsetsToTransaction(Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets, org.apache.kafka.clients.consumer.ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {}
        @Override public Uuid clientInstanceId(Duration timeout) { return null; }
    }
}
