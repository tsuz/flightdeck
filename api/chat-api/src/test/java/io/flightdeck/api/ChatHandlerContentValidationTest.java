package io.flightdeck.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates {@link ChatHandler#parseContent}: plain-text content keeps working,
 * image-by-reference block lists are accepted, and anything that would put bytes
 * or unsupported blocks into Kafka is rejected with an IllegalArgumentException
 * (HTTP 400).
 */
class ChatHandlerContentValidationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode body(String json) throws Exception {
        return MAPPER.readTree(json);
    }

    @Test
    void acceptsPlainString() throws Exception {
        JsonNode content = ChatHandler.parseContent(body("{\"content\":\"hello\"}"));
        assertTrue(content.isTextual());
        assertEquals("hello", content.asText());
    }

    @Test
    void rejectsBlankStringAndMissingField() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> ChatHandler.parseContent(body("{\"content\":\"   \"}")));
        assertThrows(IllegalArgumentException.class,
                () -> ChatHandler.parseContent(body("{\"session_id\":\"s\"}")));
        assertThrows(IllegalArgumentException.class,
                () -> ChatHandler.parseContent(body("{\"content\":null}")));
    }

    @Test
    void acceptsImageUrlPlusText() throws Exception {
        JsonNode content = ChatHandler.parseContent(body("""
                {"content":[
                  {"type":"image","source":{"type":"url","url":"https://storage.example.com/shot.png"}},
                  {"type":"text","text":"this is what I see"}
                ]}"""));
        assertTrue(content.isArray());
        assertEquals(2, content.size());
        assertEquals("image", content.get(0).get("type").asText());
        assertEquals("https://storage.example.com/shot.png",
                content.get(0).get("source").get("url").asText());
        assertEquals("[image] this is what I see", ChatHandler.summarizeContent(content));
    }

    @Test
    void acceptsImageOnlyAndFileSource() throws Exception {
        JsonNode urlOnly = ChatHandler.parseContent(body("""
                {"content":[{"type":"image","source":{"type":"url","url":"https://x.test/a.jpg"}}]}"""));
        assertEquals(1, urlOnly.size());
        JsonNode file = ChatHandler.parseContent(body("""
                {"content":[{"type":"image","source":{"type":"file","file_id":"file_abc"}}]}"""));
        assertEquals("file_abc", file.get(0).get("source").get("file_id").asText());
    }

    @Test
    void rejectsBase64ImageSource() throws Exception {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ChatHandler.parseContent(body("""
                        {"content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"iVBOR"}}]}""")));
        assertTrue(e.getMessage().contains("base64"));
    }

    @Test
    void rejectsNonHttpsImageUrl() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ChatHandler.parseContent(body("""
                {"content":[{"type":"image","source":{"type":"url","url":"http://x.test/a.jpg"}}]}""")));
        assertThrows(IllegalArgumentException.class, () -> ChatHandler.parseContent(body("""
                {"content":[{"type":"image","source":{"type":"url","url":"file:///etc/passwd"}}]}""")));
        assertThrows(IllegalArgumentException.class, () -> ChatHandler.parseContent(body("""
                {"content":[{"type":"image","source":{"type":"url"}}]}""")));
    }

    @Test
    void rejectsUnsupportedShapes() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> ChatHandler.parseContent(body("{\"content\":[]}")));
        assertThrows(IllegalArgumentException.class, () -> ChatHandler.parseContent(body("{\"content\":{\"a\":1}}")));
        assertThrows(IllegalArgumentException.class, () -> ChatHandler.parseContent(body("{\"content\":[\"x\"]}")));
        assertThrows(IllegalArgumentException.class, () -> ChatHandler.parseContent(body(
                "{\"content\":[{\"type\":\"document\",\"source\":{\"type\":\"url\",\"url\":\"https://x.test/a.pdf\"}}]}")));
        assertThrows(IllegalArgumentException.class, () -> ChatHandler.parseContent(body(
                "{\"content\":[{\"type\":\"text\",\"text\":\"\"}]}")));
        assertThrows(IllegalArgumentException.class, () -> ChatHandler.parseContent(body(
                "{\"content\":[{\"type\":\"image\"}]}")));
    }

    @Test
    void stripsCallerSuppliedExtrasFromBlocks() throws Exception {
        JsonNode content = ChatHandler.parseContent(body("""
                {"content":[
                  {"type":"text","text":"x","cache_control":{"type":"ephemeral"}},
                  {"type":"image","source":{"type":"url","url":"https://x.test/a.png","foo":1},"bar":2}
                ]}"""));
        assertEquals("{\"type\":\"text\",\"text\":\"x\"}", content.get(0).toString());
        assertEquals("{\"type\":\"image\",\"source\":{\"type\":\"url\",\"url\":\"https://x.test/a.png\"}}",
                content.get(1).toString());
    }

    @Test
    void rejectsMalformedHttpsStrings() {
        assertFalse(ChatHandler.isHttpsUrl("https://"));
        assertFalse(ChatHandler.isHttpsUrl("https:// evil"));
        assertFalse(ChatHandler.isHttpsUrl("https://[::1"));
        assertFalse(ChatHandler.isHttpsUrl("http://x.test/a.png"));
        assertTrue(ChatHandler.isHttpsUrl("https://storage.crisp.chat/users/upload/x/a.png?x=1"));
        assertTrue(ChatHandler.isHttpsUrl("HTTPS://x.test/a.png"));
    }

    @Test
    void noSizeOrCountCapOnReferences() throws Exception {
        // Size is policed by the Kafka record limit and the model, not here.
        StringBuilder sb = new StringBuilder("{\"content\":[");
        for (int i = 0; i < 50; i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"type\":\"image\",\"source\":{\"type\":\"url\",\"url\":\"https://x.test/").append(i).append(".png\"}}");
        }
        sb.append(",{\"type\":\"text\",\"text\":\"").append("x".repeat(200_000)).append("\"}]}");
        JsonNode content = ChatHandler.parseContent(body(sb.toString()));
        assertEquals(51, content.size());
    }
}
