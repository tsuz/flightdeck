package io.flightdeck.memoir.service;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FlattenContentTest {

    @Test
    void stringPassesThrough() {
        assertEquals("hello", ClaudeMemoirService.flattenContent("hello"));
        assertNull(ClaudeMemoirService.flattenContent(null));
    }

    @Test
    void imageAndTextBlocksFlattenToReadableText() {
        String out = ClaudeMemoirService.flattenContent(List.of(
                Map.of("type", "image", "source", Map.of("type", "url", "url", "https://x.test/a.png")),
                Map.of("type", "text", "text", "this is the error")));
        assertEquals("[image attached]\nthis is the error", out);
    }

    @Test
    void assistantToolBlocksFlatten() {
        String out = ClaudeMemoirService.flattenContent(List.of(
                Map.of("type", "text", "text", "Checking."),
                Map.of("type", "tool_use", "name", "get_invoice", "input", Map.of())));
        assertEquals("Checking.\n[Called tool: get_invoice]", out);
    }
}
