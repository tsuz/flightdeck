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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
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
    @DisplayName("Exactly at trigger (3 user msgs): compacts old portion with tool interactions, keeps last 2 user msgs")
    void compaction_exactlyAtTrigger_withToolInteractions() throws Exception {
        // TRIGGER=3, UNTIL=2. Exactly 3 user messages in history.
        // Each user message triggers a tool call, so history includes tool_use + tool result pairs.
        //
        // History (12 messages):
        //   [0]  user:      "list topics"             ← user msg #1 (compacted)
        //   [1]  assistant:  tool_use(list_topics)     ← (compacted)
        //   [2]  tool:       tool result               ← (compacted)
        //   [3]  assistant:  "Found 5 topics."         ← (compacted)
        //   ---- split here (index 4) ----
        //   [4]  user:      "describe topic X"         ← user msg #2 (kept)
        //   [5]  assistant:  tool_use(describe_topic)  ← (kept)
        //   [6]  tool:       tool result               ← (kept)
        //   [7]  assistant:  "Topic X has 3 partitions" ← (kept)
        //   [8]  user:      "check consumer lag"       ← user msg #3 (kept)
        //   [9]  assistant:  tool_use(consumer_lag)    ← (kept)
        //   [10] tool:       tool result               ← (kept)
        //   [11] assistant:  "Lag is 0."               ← (kept)

        List<MessageInput> history = new ArrayList<>();
        // Turn 1 — compacted
        history.add(userMsg("list topics"));                                              // 0
        history.add(new MessageInput(SESSION, USER, "assistant",                          // 1
                List.of(Map.of("type", "tool_use", "id", "t1", "name", "list_topics",
                        "input", Map.of())),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",                               // 2
                List.of(Map.of("tool_use_id", "t1", "content",
                        "{\"topics\":[\"topicA\",\"topicB\"]}")),
                TS, null));
        history.add(assistantMsg("Found 5 topics."));                                     // 3
        // Turn 2 — kept
        history.add(userMsg("describe topic X"));                                         // 4
        history.add(new MessageInput(SESSION, USER, "assistant",                          // 5
                List.of(Map.of("type", "tool_use", "id", "t2", "name", "describe_topic",
                        "input", Map.of("topic", "X"))),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",                               // 6
                List.of(Map.of("tool_use_id", "t2", "content",
                        "{\"partitions\":3}")),
                TS, null));
        history.add(assistantMsg("Topic X has 3 partitions."));                           // 7
        // Turn 3 — kept
        history.add(userMsg("check consumer lag"));                                       // 8
        history.add(new MessageInput(SESSION, USER, "assistant",                          // 9
                List.of(Map.of("type", "tool_use", "id", "t3", "name", "consumer_lag",
                        "input", Map.of("group", "my-group"))),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",                               // 10
                List.of(Map.of("tool_use_id", "t3", "content",
                        "{\"lag\":0}")),
                TS, null));
        history.add(assistantMsg("Lag is 0."));                                           // 11

        MessageInput latestInput = userMsg("show broker health");

        FullSessionContext context = new FullSessionContext(
                SESSION, USER, 0.05, history, latestInput, null, TS);

        // Verify split index
        assertThat(ThinkConsumer.findCompactionSplitIndex(history, 2)).isEqualTo(4);

        // --- Mock: compaction call ---
        ThinkResponse summaryResponse = new ThinkResponse(
                SESSION, USER, 0.001, null, 50, 30,
                null, null,
                List.of(assistantMsg("User listed topics. Found 5 topics including topicA and topicB.")),
                null, true, false, 0, 0, 0.0, TS);
        when(mockLlm.callWithoutTools(
                eq(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT),
                anyList(), eq(SESSION), eq(USER)))
                .thenReturn(summaryResponse);

        // --- Mock: main LLM call ---
        List<Map<String, Object>> mainApiMessages = List.of(Map.of("role", "user", "content", "main"));
        when(mockLlm.toApiMessages(anyList(), eq(latestInput)))
                .thenReturn(mainApiMessages);
        ThinkResponse mainResponse = new ThinkResponse(
                SESSION, USER, 0.01, null, 200, 100,
                null, null,
                List.of(assistantMsg("All brokers healthy.")),
                null, true, false, 0, 0, 0.0, TS);
        when(mockLlm.call(anyString(), eq(mainApiMessages), eq(SESSION), eq(USER)))
                .thenReturn(mainResponse);

        // --- Execute ---
        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION,
                mapper.writeValueAsString(context)));

        // --- Verify: compaction + main call ---
        verify(mockLlm, times(1)).callWithoutTools(anyString(), anyList(), eq(SESSION), eq(USER));
        verify(mockLlm, times(1)).call(anyString(), anyList(), eq(SESSION), eq(USER));

        // Main call received: summary(1) + kept(8) = 9 messages
        ArgumentCaptor<List<MessageInput>> historyCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockLlm).toApiMessages(historyCaptor.capture(), eq(latestInput));
        List<MessageInput> mainInput = historyCaptor.getValue();
        assertThat(mainInput).hasSize(9);
        // First message is the compaction summary — contains the LLM's summarization output
        assertThat(mainInput.get(0).role()).isEqualTo("assistant");
        assertThat(mainInput.get(0).contentAsString()).isEqualTo(
                "[Conversation Summary]\nUser listed topics. Found 5 topics including topicA and topicB.");
        // Kept portion starts at user msg #2 with its tool interactions
        assertThat(mainInput.get(1).role()).isEqualTo("user");
        assertThat(mainInput.get(1).contentAsString()).isEqualTo("describe topic X");
        assertThat(mainInput.get(2).role()).isEqualTo("assistant");  // tool_use
        assertThat(mainInput.get(2).content()).isInstanceOf(List.class);
        assertThat(mainInput.get(3).role()).isEqualTo("tool");       // tool result
        assertThat(mainInput.get(4).contentAsString()).isEqualTo("Topic X has 3 partitions.");
        assertThat(mainInput.get(5).contentAsString()).isEqualTo("check consumer lag");
        assertThat(mainInput.get(8).contentAsString()).isEqualTo("Lag is 0.");

        // Produced response
        ThinkResponse produced = mapper.readValue(
                capturingProducer.records.get(0).value(), ThinkResponse.class);
        assertThat(produced.previousMessages()).hasSize(9);
        assertThat(produced.previousMessages().get(0).contentAsString()).isEqualTo(
                "[Conversation Summary]\nUser listed topics. Found 5 topics including topicA and topicB.");
        assertThat(produced.compaction()).isTrue();
        assertThat(produced.compactionInputTokens()).isEqualTo(50);
        assertThat(produced.compactionOutputTokens()).isEqualTo(30);
        assertThat(produced.compactionCost()).isGreaterThan(0.0);
        assertThat(produced.lastInputMessage().contentAsString()).isEqualTo("show broker health");
        assertThat(produced.lastInputResponse()).hasSize(1);
        assertThat(produced.lastInputResponse().get(0).contentAsString()).isEqualTo("All brokers healthy.");
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
                null, null,
                List.of(assistantMsg("User searched flights to NYC. Found AA123, UA456, DL789.")),
                null, true, false, 0, 0, 0.0, TS);
        when(mockLlm.callWithoutTools(
                eq(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT),
                anyList(),
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
                null, null,
                List.of(assistantMsg("You're welcome!")),
                null, true, false, 0, 0, 0.0, TS);
        when(mockLlm.call(anyString(), eq(mainApiMessages), eq(SESSION), eq(USER)))
                .thenReturn(mainResponse);

        // --- Execute ---
        String recordValue = mapper.writeValueAsString(context);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION, recordValue);
        thinkConsumer.processRecord(record);

        // --- Verify ---
        verify(mockLlm, times(1)).callWithoutTools(anyString(), anyList(), eq(SESSION), eq(USER));
        verify(mockLlm, times(1)).call(anyString(), anyList(), eq(SESSION), eq(USER));

        // Compaction uses toPlainTextMessages (not toApiMessages), so toApiMessages is called once for main
        ArgumentCaptor<List<MessageInput>> historyCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockLlm, times(1)).toApiMessages(historyCaptor.capture(), any());

        // The main call received compacted history (summary + 4 kept messages)
        List<MessageInput> mainInput = historyCaptor.getAllValues().get(0);
        assertThat(mainInput).hasSize(5);
        assertThat(mainInput.get(0).contentAsString()).startsWith("[Conversation Summary]");
        assertThat(mainInput.get(1).contentAsString()).isEqualTo("Book the cheapest");
        assertThat(mainInput.get(4).contentAsString()).isEqualTo("Here's your confirmation.");

        // Produced response — verify JSON serialization includes previous_messages key
        String producedJson = capturingProducer.records.get(0).value();
        assertThat(producedJson).contains("\"previous_messages\"");

        ThinkResponse produced = mapper.readValue(producedJson, ThinkResponse.class);
        assertThat(produced.previousMessages()).hasSize(5);
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
                null, null,
                List.of(assistantMsg("Nice!")),
                null, true, false, 0, 0, 0.0, TS);
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

        // Produced response has previousMessages equal to the original history (no compaction)
        assertThat(capturingProducer.records).hasSize(1);
        String producedJson = capturingProducer.records.get(0).value();
        ThinkResponse produced = mapper.readValue(producedJson, ThinkResponse.class);
        assertThat(produced.previousMessages()).hasSize(history.size());
    }

    @Test
    @DisplayName("Mid-tool-loop (latestInput is tool result) skips compaction even when trigger is exceeded")
    void noCompaction_midToolLoop() throws Exception {
        // 3 user messages in history, but latestInput is a tool result → skip compaction
        List<MessageInput> history = new ArrayList<>();
        history.add(userMsg("Search for X"));
        history.add(assistantMsg("I'll search for X."));
        history.add(userMsg("Also search Y"));
        history.add(assistantMsg("Searching..."));
        history.add(userMsg("And Z"));
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t1", "name", "search", "input", Map.of())),
                TS, null));

        // latestInput is a tool result → active tool loop
        MessageInput latestInput = new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t1", "content", "result")),
                TS, null);

        FullSessionContext context = new FullSessionContext(
                SESSION, USER, 0.05, history, latestInput, null, TS);

        List<Map<String, Object>> apiMessages = List.of(
                Map.of("role", "user", "content", "test"));
        when(mockLlm.toApiMessages(eq(history), eq(latestInput)))
                .thenReturn(apiMessages);

        ThinkResponse mainResponse = new ThinkResponse(
                SESSION, USER, 0.01, null, 100, 50,
                null, null,
                List.of(assistantMsg("Here are the results.")),
                null, true, false, 0, 0, 0.0, TS);
        when(mockLlm.call(anyString(), eq(apiMessages), eq(SESSION), eq(USER)))
                .thenReturn(mainResponse);

        String recordValue = mapper.writeValueAsString(context);
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION, recordValue);
        thinkConsumer.processRecord(record);

        // Only ONE call — compaction was skipped because latestInput is a tool result
        verify(mockLlm, times(1)).call(anyString(), anyList(), eq(SESSION), eq(USER));
    }

    // ── Real-world JSON input tests ─────────────────────────────────────────

    @Test
    @DisplayName("Real-world: user latestInput with tool interactions in history triggers compaction")
    void realWorld_userLatestInput_compactsWithToolHistory() throws Exception {
        // Load real-world context: 10 messages in history, 3 user messages (indices 0,2,6)
        // latestInput is role="user" ("describe group: think-consumer-group")
        // History includes completed tool_use + tool_result exchanges
        //
        // TRIGGER=3, UNTIL=2 → split at index 2 (2nd user msg "list topics")
        // Old: [0,1] = user("Hi,") + assistant("Hello!...")
        // Kept: [2..9] = user("list topics"), tool_use, tool_result, assistant,
        //                 user("lst consumer gruop"), tool_use, tool_result, assistant
        String json = new String(getClass().getResourceAsStream(
                "/compaction-test-context-user-input.json").readAllBytes());
        FullSessionContext context = mapper.readValue(json, FullSessionContext.class);

        assertThat(context.history()).hasSize(10);
        assertThat(context.latestInput().role()).isEqualTo("user");
        assertThat(context.latestInput().contentAsString()).isEqualTo("describe group: think-consumer-group");

        long userCount = context.history().stream()
                .filter(m -> "user".equals(m.role())).count();
        assertThat(userCount).isEqualTo(3);

        String sessionId = context.sessionId();
        String userId = context.userId();

        // Verify split point
        int splitIndex = ThinkConsumer.findCompactionSplitIndex(context.history(), 2);
        assertThat(splitIndex).isEqualTo(2);

        List<MessageInput> expectedOld = context.history().subList(0, 2);
        List<MessageInput> expectedKept = context.history().subList(2, 10);
        assertThat(expectedOld).hasSize(2);
        assertThat(expectedOld.get(0).contentAsString()).isEqualTo("Hi,");
        assertThat(expectedKept).hasSize(8);
        assertThat(expectedKept.get(0).contentAsString()).isEqualTo("list topics");

        // Mock: compaction call summarizes old portion [0,1]
        List<Map<String, Object>> oldApiMsgs = List.of(
                Map.of("role", "user", "content", "Hi,"),
                Map.of("role", "assistant", "content", "Hello!..."));
        when(mockLlm.toApiMessages(expectedOld, null)).thenReturn(oldApiMsgs);
        when(mockLlm.callWithoutTools(
                eq(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT),
                anyList(), eq(sessionId), eq(userId)))
                .thenReturn(new ThinkResponse(sessionId, userId, 0.001, null, 20, 15,
                        null, null,
                        List.of(new MessageInput(sessionId, userId, "assistant",
                                "User greeted the Kafka assistant.", TS, null)),
                        null, true, false, 0, 0, 0.0, TS));

        // Mock: main LLM call with compacted history (summary + 8 kept = 9) + latestInput
        List<Map<String, Object>> mainApiMsgs = List.of(Map.of("role", "user", "content", "main"));
        when(mockLlm.toApiMessages(anyList(), eq(context.latestInput()))).thenReturn(mainApiMsgs);
        when(mockLlm.call(anyString(), eq(mainApiMsgs), eq(sessionId), eq(userId)))
                .thenReturn(new ThinkResponse(sessionId, userId, 0.005, null, 200, 100,
                        null, null,
                        List.of(new MessageInput(sessionId, userId, "assistant",
                                "think-consumer-group has 0 lag.", TS, null)),
                        null, true, false, 0, 0, 0.0, TS));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, sessionId,
                mapper.writeValueAsString(context)));

        // --- Verify: compaction (callWithoutTools) + main (call) ---
        verify(mockLlm, times(1)).callWithoutTools(anyString(), anyList(), eq(sessionId), eq(userId));
        verify(mockLlm, times(1)).call(anyString(), anyList(), eq(sessionId), eq(userId));

        // Compaction call got the old portion (2 messages)
        // Compaction uses toPlainTextMessages, so toApiMessages called once for main
        ArgumentCaptor<List<MessageInput>> historyCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockLlm, times(1)).toApiMessages(historyCaptor.capture(), any());

        // Main call got compacted history: summary + 8 kept messages = 9
        List<MessageInput> mainInput = historyCaptor.getAllValues().get(0);
        assertThat(mainInput).hasSize(9);
        assertThat(mainInput.get(0).contentAsString()).startsWith("[Conversation Summary]");
        // Kept portion starts with "list topics" user msg and includes tool interactions
        assertThat(mainInput.get(1).contentAsString()).isEqualTo("list topics");
        assertThat(mainInput.get(2).role()).isEqualTo("assistant"); // tool_use block
        assertThat(mainInput.get(3).role()).isEqualTo("tool");     // tool result

        // Produced response has previousMessages with 9 messages
        String producedJson = capturingProducer.records.get(0).value();
        assertThat(producedJson).contains("\"previous_messages\"");
        ThinkResponse produced = mapper.readValue(producedJson, ThinkResponse.class);
        assertThat(produced.previousMessages()).hasSize(9);
        assertThat(produced.previousMessages().get(0).contentAsString())
                .startsWith("[Conversation Summary]");
    }

    @Test
    @DisplayName("Real-world: tool result as latestInput skips compaction (active tool loop)")
    void realWorld_toolLatestInput_skipsCompaction() throws Exception {
        // Same history as above but latestInput is a tool result → skip compaction
        String json = new String(getClass().getResourceAsStream(
                "/compaction-test-context-user-input.json").readAllBytes());
        FullSessionContext original = mapper.readValue(json, FullSessionContext.class);

        // Swap latestInput to a tool result
        MessageInput toolLatestInput = new MessageInput(
                original.sessionId(), original.userId(), "tool",
                List.of(Map.of("tool_use_id", "toolu_test", "name", "kafka_consumer_group_lag",
                        "result", Map.of("total_lag", 0), "status", "success")),
                TS, null);
        FullSessionContext context = new FullSessionContext(
                original.sessionId(), original.userId(), original.cost(),
                original.history(), toolLatestInput, null, original.timestamp());

        String sessionId = context.sessionId();
        String userId = context.userId();

        // Mock: single LLM call (no compaction)
        when(mockLlm.toApiMessages(eq(context.history()), eq(toolLatestInput)))
                .thenReturn(List.of(Map.of("role", "user", "content", "test")));
        when(mockLlm.call(anyString(), anyList(), eq(sessionId), eq(userId)))
                .thenReturn(new ThinkResponse(sessionId, userId, 0.01, null, 100, 50,
                        null, null,
                        List.of(new MessageInput(sessionId, userId, "assistant",
                                "The group has 0 lag.", TS, null)),
                        null, true, false, 0, 0, 0.0, TS));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, sessionId,
                mapper.writeValueAsString(context)));

        // Only 1 LLM call — compaction skipped because latestInput is a tool result
        verify(mockLlm, times(1)).call(anyString(), anyList(), eq(sessionId), eq(userId));

        ThinkResponse produced = mapper.readValue(
                capturingProducer.records.get(0).value(), ThinkResponse.class);
        assertThat(produced.previousMessages()).hasSize(original.history().size());
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
        when(mockLlm.callWithoutTools(eq(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT),
                anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.0, null, 10, 10,
                        null, null,
                        List.of(assistantMsg("summary")), null, true, false, 0, 0, 0.0, TS));

        // Mock main call
        when(mockLlm.toApiMessages(anyList(), eq(latestInput)))
                .thenReturn(List.of(Map.of("role", "user", "content", "msg4")));
        when(mockLlm.call(anyString(), anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.01, null, 50, 50,
                        null, null,
                        List.of(assistantMsg("done")), null, true, false, 0, 0, 0.0, TS));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION,
                mapper.writeValueAsString(context)));

        // Verify compaction used callWithoutTools with COMPACTION_PROMPT
        ArgumentCaptor<String> compactionPromptCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockLlm).callWithoutTools(compactionPromptCaptor.capture(), anyList(), eq(SESSION), eq(USER));
        assertThat(compactionPromptCaptor.getValue())
                .isEqualTo(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT);
        assertThat(compactionPromptCaptor.getValue())
                .contains("Summarize the following conversation");

        // Verify main call used call() with the system prompt (not compaction prompt)
        ArgumentCaptor<String> mainPromptCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockLlm).call(mainPromptCaptor.capture(), anyList(), eq(SESSION), eq(USER));
        assertThat(mainPromptCaptor.getValue())
                .isNotEqualTo(io.flightdeck.think.config.AppConfig.COMPACTION_PROMPT);
    }

    // ── Additional compaction scenarios ─────────────────────────────────────

    @Test
    @DisplayName("Scenario 2: user msgs == UNTIL (2 user msgs, TRIGGER=2) → no compaction, nothing to compact")
    void noCompaction_userMsgsEqualUntil() throws Exception {
        // TRIGGER=3 in env, but this has only 2 user messages — below trigger, no compaction.
        // Even if trigger were 2, UNTIL=2 means keep all 2 → splitIndex=-1 → no compaction.
        // History: user + tool_use + tool + assistant + user + tool_use + tool + assistant
        List<MessageInput> history = new ArrayList<>();
        history.add(userMsg("list topics"));
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t1", "name", "list_topics", "input", Map.of())),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t1", "content", "{\"topics\":[\"A\"]}")),
                TS, null));
        history.add(assistantMsg("Found 1 topic."));
        history.add(userMsg("describe topic A"));
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t2", "name", "describe_topic", "input", Map.of("topic", "A"))),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t2", "content", "{\"partitions\":1}")),
                TS, null));
        history.add(assistantMsg("Topic A has 1 partition."));

        MessageInput latestInput = userMsg("thanks");
        FullSessionContext context = new FullSessionContext(SESSION, USER, 0.02, history, latestInput, null, TS);

        // Only 2 user messages < TRIGGER=3 → no compaction
        when(mockLlm.toApiMessages(eq(history), eq(latestInput)))
                .thenReturn(List.of(Map.of("role", "user", "content", "test")));
        when(mockLlm.call(anyString(), anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.01, null, 100, 50,
                        null, null, List.of(assistantMsg("You're welcome!")),
                        null, true, false, 0, 0, 0.0, TS));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION,
                mapper.writeValueAsString(context)));

        // No compaction call
        verify(mockLlm, never()).callWithoutTools(anyString(), anyList(), anyString(), anyString());
        verify(mockLlm, times(1)).call(anyString(), anyList(), eq(SESSION), eq(USER));

        ThinkResponse produced = mapper.readValue(
                capturingProducer.records.get(0).value(), ThinkResponse.class);
        assertThat(produced.compaction()).isFalse();
        assertThat(produced.compactionInputTokens()).isEqualTo(0);
        assertThat(produced.compactionOutputTokens()).isEqualTo(0);
        assertThat(produced.compactionCost()).isEqualTo(0.0);
        // previousMessages is the full uncompacted history
        assertThat(produced.previousMessages()).hasSize(8);
    }

    @Test
    @DisplayName("Scenario 3: Many user msgs (8 user msgs) → compacts 6, keeps last 2 with tool interactions")
    void compaction_manyUserMessages_keepsLastTwo() throws Exception {
        // 8 user messages, each with tool_use + tool + assistant response = 32 messages
        // TRIGGER=3, UNTIL=2 → split keeps last 2 user msgs (user #7 and #8)
        // Old: users #1-#6 (24 msgs), Kept: users #7-#8 (8 msgs)
        List<MessageInput> history = new ArrayList<>();
        for (int i = 1; i <= 8; i++) {
            history.add(userMsg("question " + i));
            history.add(new MessageInput(SESSION, USER, "assistant",
                    List.of(Map.of("type", "tool_use", "id", "t" + i, "name", "tool_" + i,
                            "input", Map.of("q", i))),
                    TS, null));
            history.add(new MessageInput(SESSION, USER, "tool",
                    List.of(Map.of("tool_use_id", "t" + i, "content", "{\"result\":" + i + "}")),
                    TS, null));
            history.add(assistantMsg("answer " + i));
        }
        assertThat(history).hasSize(32);

        // Split at user #7 (index 24)
        assertThat(ThinkConsumer.findCompactionSplitIndex(history, 2)).isEqualTo(24);

        MessageInput latestInput = userMsg("one more question");
        FullSessionContext context = new FullSessionContext(SESSION, USER, 0.1, history, latestInput, null, TS);

        String summaryText = "User asked 6 questions (q1-q6) using various tools. All returned results.";
        when(mockLlm.callWithoutTools(anyString(), anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.005, null, 500, 100,
                        null, null, List.of(assistantMsg(summaryText)),
                        null, true, false, 0, 0, 0.0, TS));

        List<Map<String, Object>> mainApiMsgs = List.of(Map.of("role", "user", "content", "main"));
        when(mockLlm.toApiMessages(anyList(), eq(latestInput))).thenReturn(mainApiMsgs);
        when(mockLlm.call(anyString(), eq(mainApiMsgs), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.01, null, 200, 50,
                        null, null, List.of(assistantMsg("answer 9")),
                        null, true, false, 0, 0, 0.0, TS));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION,
                mapper.writeValueAsString(context)));

        verify(mockLlm, times(1)).callWithoutTools(anyString(), anyList(), eq(SESSION), eq(USER));

        // Main call: summary(1) + kept(8 msgs for users #7-#8) = 9
        ArgumentCaptor<List<MessageInput>> histCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockLlm).toApiMessages(histCaptor.capture(), eq(latestInput));
        List<MessageInput> mainInput = histCaptor.getValue();
        assertThat(mainInput).hasSize(9);
        assertThat(mainInput.get(0).contentAsString()).isEqualTo(
                "[Conversation Summary]\n" + summaryText);
        assertThat(mainInput.get(1).contentAsString()).isEqualTo("question 7");
        assertThat(mainInput.get(5).contentAsString()).isEqualTo("question 8");
        assertThat(mainInput.get(8).contentAsString()).isEqualTo("answer 8");

        ThinkResponse produced = mapper.readValue(
                capturingProducer.records.get(0).value(), ThinkResponse.class);
        assertThat(produced.previousMessages()).hasSize(9);
        assertThat(produced.previousMessages().get(0).contentAsString()).isEqualTo(
                "[Conversation Summary]\n" + summaryText);
        assertThat(produced.compaction()).isTrue();
        assertThat(produced.compactionInputTokens()).isEqualTo(500);
        assertThat(produced.compactionOutputTokens()).isEqualTo(100);
    }

    @Test
    @DisplayName("Scenario 4: Second compaction round — old summary is re-compacted with new messages")
    void compaction_secondRound_recompactsOldSummary() throws Exception {
        // History starts with a [Conversation Summary] from a previous compaction,
        // followed by enough new user messages to trigger compaction again.
        // TRIGGER=3, UNTIL=2.
        //
        // History:
        //   [0]  assistant:  "[Conversation Summary]\nUser asked about topics."  ← old summary
        //   [1]  user:       "check broker health"        ← user msg #1 (compacted with summary)
        //   [2]  assistant:   tool_use(broker_health)
        //   [3]  tool:        tool result
        //   [4]  assistant:  "All brokers healthy."
        //   ---- split (index 5) ----
        //   [5]  user:       "check lag"                  ← user msg #2 (kept)
        //   [6]  assistant:   tool_use(consumer_lag)
        //   [7]  tool:        tool result
        //   [8]  assistant:  "Lag is 0."
        //   [9]  user:       "describe topic X"           ← user msg #3 (kept)
        //   [10] assistant:   tool_use(describe_topic)
        //   [11] tool:        tool result
        //   [12] assistant:  "Topic X: 3 partitions."

        List<MessageInput> history = new ArrayList<>();
        history.add(assistantMsg("[Conversation Summary]\nUser asked about topics."));     // 0
        history.add(userMsg("check broker health"));                                       // 1
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t1", "name", "broker_health", "input", Map.of())),
                TS, null));                                                                // 2
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t1", "content", "{\"status\":\"ok\"}")),
                TS, null));                                                                // 3
        history.add(assistantMsg("All brokers healthy."));                                 // 4
        history.add(userMsg("check lag"));                                                 // 5
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t2", "name", "consumer_lag", "input", Map.of("g", "grp"))),
                TS, null));                                                                // 6
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t2", "content", "{\"lag\":0}")),
                TS, null));                                                                // 7
        history.add(assistantMsg("Lag is 0."));                                            // 8
        history.add(userMsg("describe topic X"));                                          // 9
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t3", "name", "describe_topic", "input", Map.of("t", "X"))),
                TS, null));                                                                // 10
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t3", "content", "{\"partitions\":3}")),
                TS, null));                                                                // 11
        history.add(assistantMsg("Topic X: 3 partitions."));                               // 12

        assertThat(ThinkConsumer.findCompactionSplitIndex(history, 2)).isEqualTo(5);

        MessageInput latestInput = userMsg("anything else?");
        FullSessionContext context = new FullSessionContext(SESSION, USER, 0.05, history, latestInput, null, TS);

        // The summary should incorporate the old summary + new info
        String newSummary = "User asked about topics. Then checked broker health (all OK) and it was healthy.";
        when(mockLlm.callWithoutTools(anyString(), anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.002, null, 80, 40,
                        null, null, List.of(assistantMsg(newSummary)),
                        null, true, false, 0, 0, 0.0, TS));

        List<Map<String, Object>> mainApiMsgs = List.of(Map.of("role", "user", "content", "main"));
        when(mockLlm.toApiMessages(anyList(), eq(latestInput))).thenReturn(mainApiMsgs);
        when(mockLlm.call(anyString(), eq(mainApiMsgs), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.01, null, 200, 80,
                        null, null, List.of(assistantMsg("Nothing else to report.")),
                        null, true, false, 0, 0, 0.0, TS));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION,
                mapper.writeValueAsString(context)));

        // Compaction fired
        verify(mockLlm, times(1)).callWithoutTools(anyString(), anyList(), eq(SESSION), eq(USER));

        // Main call: new summary(1) + kept(8: users #2,#3 with tool interactions) = 9
        ArgumentCaptor<List<MessageInput>> histCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockLlm).toApiMessages(histCaptor.capture(), eq(latestInput));
        List<MessageInput> mainInput = histCaptor.getValue();
        assertThat(mainInput).hasSize(9);
        assertThat(mainInput.get(0).contentAsString()).isEqualTo(
                "[Conversation Summary]\n" + newSummary);
        assertThat(mainInput.get(1).contentAsString()).isEqualTo("check lag");
        assertThat(mainInput.get(2).role()).isEqualTo("assistant"); // tool_use
        assertThat(mainInput.get(3).role()).isEqualTo("tool");

        ThinkResponse produced = mapper.readValue(
                capturingProducer.records.get(0).value(), ThinkResponse.class);
        assertThat(produced.previousMessages()).hasSize(9);
        assertThat(produced.previousMessages().get(0).contentAsString()).isEqualTo(
                "[Conversation Summary]\n" + newSummary);
        assertThat(produced.compaction()).isTrue();
    }

    @Test
    @DisplayName("Scenario 5: No compaction → previousMessages equals original history, compaction fields zero")
    void noCompaction_previousMessagesEqualsHistory_fieldsZero() throws Exception {
        // 2 user messages (below trigger=3), with tool interactions
        List<MessageInput> history = new ArrayList<>();
        history.add(userMsg("list topics"));
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t1", "name", "list_topics", "input", Map.of())),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t1", "content", "{\"topics\":[\"A\"]}")),
                TS, null));
        history.add(assistantMsg("Found topic A."));
        history.add(userMsg("describe A"));
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t2", "name", "describe", "input", Map.of("t", "A"))),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t2", "content", "{\"p\":1}")),
                TS, null));
        history.add(assistantMsg("Topic A: 1 partition."));

        MessageInput latestInput = userMsg("ok thanks");
        FullSessionContext context = new FullSessionContext(SESSION, USER, 0.03, history, latestInput, null, TS);

        when(mockLlm.toApiMessages(eq(history), eq(latestInput)))
                .thenReturn(List.of(Map.of("role", "user", "content", "test")));
        when(mockLlm.call(anyString(), anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.005, null, 100, 30,
                        null, null, List.of(assistantMsg("No problem!")),
                        null, true, false, 0, 0, 0.0, TS));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION,
                mapper.writeValueAsString(context)));

        verify(mockLlm, never()).callWithoutTools(anyString(), anyList(), anyString(), anyString());

        ThinkResponse produced = mapper.readValue(
                capturingProducer.records.get(0).value(), ThinkResponse.class);
        // previousMessages is the full original history (no compaction)
        assertThat(produced.previousMessages()).hasSize(8);
        assertThat(produced.previousMessages().get(0).contentAsString()).isEqualTo("list topics");
        assertThat(produced.previousMessages().get(7).contentAsString()).isEqualTo("Topic A: 1 partition.");
        // All compaction fields are zero/false
        assertThat(produced.compaction()).isFalse();
        assertThat(produced.compactionInputTokens()).isEqualTo(0);
        assertThat(produced.compactionOutputTokens()).isEqualTo(0);
        assertThat(produced.compactionCost()).isEqualTo(0.0);
        // lastInputMessage and lastInputResponse correct
        assertThat(produced.lastInputMessage().contentAsString()).isEqualTo("ok thanks");
        assertThat(produced.lastInputResponse().get(0).contentAsString()).isEqualTo("No problem!");
    }

    @Test
    @DisplayName("Scenario 6: All user msgs within UNTIL range → splitIndex=-1, no compaction")
    void noCompaction_allUserMsgsWithinUntilRange() throws Exception {
        // 3 user messages, TRIGGER=3, but UNTIL=3 → keep all 3 → nothing to compact
        // Note: env has UNTIL=2, but findCompactionSplitIndex is called with AppConfig value.
        // Here we test splitIndex directly — with 3 users and keepLast=3, returns -1.
        // With UNTIL=2 (from env), 3 users would compact. So we use 2 user messages + TRIGGER=3.
        // Actually the env has TRIGGER=3, so 2 users < 3 → no compaction at all.
        // Let's test: exactly 3 user msgs but UNTIL=2, first user msg has no content before it.
        // Split at index 0 → old portion empty → splitIndex > 0 check fails.
        //
        // Actually: with 3 users at indices 0,4,8 and UNTIL=2, split at index 4 (>0).
        // So this scenario is: TRIGGER=3, UNTIL=3, 3 user msgs → splitIndex=-1.
        // But UNTIL from env is 2. We test via findCompactionSplitIndex directly.
        List<MessageInput> history = List.of(
                userMsg("q1"),
                new MessageInput(SESSION, USER, "assistant",
                        List.of(Map.of("type", "tool_use", "id", "t1", "name", "tool1", "input", Map.of())),
                        TS, null),
                new MessageInput(SESSION, USER, "tool",
                        List.of(Map.of("tool_use_id", "t1", "content", "{}")), TS, null),
                assistantMsg("a1"),
                userMsg("q2"),
                new MessageInput(SESSION, USER, "assistant",
                        List.of(Map.of("type", "tool_use", "id", "t2", "name", "tool2", "input", Map.of())),
                        TS, null),
                new MessageInput(SESSION, USER, "tool",
                        List.of(Map.of("tool_use_id", "t2", "content", "{}")), TS, null),
                assistantMsg("a2"),
                userMsg("q3"),
                new MessageInput(SESSION, USER, "assistant",
                        List.of(Map.of("type", "tool_use", "id", "t3", "name", "tool3", "input", Map.of())),
                        TS, null),
                new MessageInput(SESSION, USER, "tool",
                        List.of(Map.of("tool_use_id", "t3", "content", "{}")), TS, null),
                assistantMsg("a3"));

        // With keepLast=3 (UNTIL=3), all 3 user msgs are kept → nothing to compact
        assertThat(ThinkConsumer.findCompactionSplitIndex(history, 3)).isEqualTo(-1);
        // With keepLast=2 (UNTIL=2), user #1 compacted, users #2,#3 kept → split at index 4
        assertThat(ThinkConsumer.findCompactionSplitIndex(history, 2)).isEqualTo(4);
        // With keepLast=1 (UNTIL=1), users #1,#2 compacted, user #3 kept → split at index 8
        assertThat(ThinkConsumer.findCompactionSplitIndex(history, 1)).isEqualTo(8);
    }

    @Test
    @DisplayName("Scenario 7: Compaction fields in response — verify exact values when compacted vs not")
    void compactionFields_exactValues() throws Exception {
        // 3 user messages with tool interactions → compaction fires
        List<MessageInput> history = new ArrayList<>();
        history.add(userMsg("q1"));
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t1", "name", "tool1", "input", Map.of())),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t1", "content", "{\"r\":1}")), TS, null));
        history.add(assistantMsg("a1"));
        history.add(userMsg("q2"));
        history.add(assistantMsg("a2"));
        history.add(userMsg("q3"));
        history.add(assistantMsg("a3"));

        MessageInput latestInput = userMsg("q4");
        FullSessionContext context = new FullSessionContext(SESSION, USER, 0.05, history, latestInput, null, TS);

        // Compaction returns specific token counts and cost
        when(mockLlm.callWithoutTools(anyString(), anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.00234, null, 150, 42,
                        null, null, List.of(assistantMsg("Summary of q1.")),
                        null, true, false, 0, 0, 0.0, TS));

        when(mockLlm.toApiMessages(anyList(), eq(latestInput)))
                .thenReturn(List.of(Map.of("role", "user", "content", "test")));
        when(mockLlm.call(anyString(), anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.008, null, 300, 80,
                        null, null, List.of(assistantMsg("a4")),
                        null, true, false, 0, 0, 0.0, TS));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION,
                mapper.writeValueAsString(context)));

        ThinkResponse produced = mapper.readValue(
                capturingProducer.records.get(0).value(), ThinkResponse.class);

        // Compaction fields reflect the summarization call
        assertThat(produced.compaction()).isTrue();
        assertThat(produced.compactionInputTokens()).isEqualTo(150);
        assertThat(produced.compactionOutputTokens()).isEqualTo(42);
        assertThat(produced.compactionCost()).isEqualTo(0.00234);

        // Main response fields
        assertThat(produced.cost()).isEqualTo(0.008);
        assertThat(produced.inputTokens()).isEqualTo(300);
        assertThat(produced.outputTokens()).isEqualTo(80);

        // Summary is correct
        assertThat(produced.previousMessages().get(0).contentAsString()).isEqualTo(
                "[Conversation Summary]\nSummary of q1.");
    }

    @Test
    @DisplayName("Scenario 8: History with only assistant and tool messages (no user) → no compaction")
    void noCompaction_noUserMessages() throws Exception {
        // Edge case: history has tool interactions but zero user messages
        // This can happen if tool results are being processed in a loop
        List<MessageInput> history = new ArrayList<>();
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t1", "name", "tool1", "input", Map.of())),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t1", "content", "{\"r\":1}")), TS, null));
        history.add(assistantMsg("Processed result."));
        history.add(new MessageInput(SESSION, USER, "assistant",
                List.of(Map.of("type", "tool_use", "id", "t2", "name", "tool2", "input", Map.of())),
                TS, null));
        history.add(new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t2", "content", "{\"r\":2}")), TS, null));
        history.add(assistantMsg("Done."));

        MessageInput latestInput = new MessageInput(SESSION, USER, "tool",
                List.of(Map.of("tool_use_id", "t3", "content", "{\"r\":3}")), TS, null);
        FullSessionContext context = new FullSessionContext(SESSION, USER, 0.01, history, latestInput, null, TS);

        when(mockLlm.toApiMessages(eq(history), eq(latestInput)))
                .thenReturn(List.of(Map.of("role", "user", "content", "test")));
        when(mockLlm.call(anyString(), anyList(), eq(SESSION), eq(USER)))
                .thenReturn(new ThinkResponse(SESSION, USER, 0.005, null, 100, 30,
                        null, null, List.of(assistantMsg("Final result.")),
                        null, true, false, 0, 0, 0.0, TS));

        thinkConsumer.processRecord(new ConsumerRecord<>(
                "test-enriched-message-input", 0, 0, SESSION,
                mapper.writeValueAsString(context)));

        // No compaction — zero user messages in history
        verify(mockLlm, never()).callWithoutTools(anyString(), anyList(), anyString(), anyString());

        ThinkResponse produced = mapper.readValue(
                capturingProducer.records.get(0).value(), ThinkResponse.class);
        assertThat(produced.compaction()).isFalse();
        assertThat(produced.compactionInputTokens()).isEqualTo(0);
        assertThat(produced.compactionOutputTokens()).isEqualTo(0);
        assertThat(produced.compactionCost()).isEqualTo(0.0);
        assertThat(produced.previousMessages()).hasSize(6);
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
     * Captures sent records for assertions using a real KafkaProducer subclass.
     * Overrides only the methods ThinkConsumer actually calls (send, flush, close)
     * to avoid Kafka Producer interface compatibility issues across versions.
     */
    static class CapturingProducer extends KafkaProducer<String, String> {
        final List<ProducerRecord<String, String>> records = new ArrayList<>();

        CapturingProducer() {
            super(Map.of(
                    "bootstrap.servers", "localhost:0",
                    "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                    "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
        }

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
        @Override public void close() {}
        @Override public void close(Duration timeout) {}
    }
}
