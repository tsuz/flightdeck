package io.flightdeck.api;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit-tests the pure mapping logic of {@link CallbackRegistry} — the SSRF-relevant
 * parts: parsing {@code ALLOWED_HOST_MAPPING}, splitting each entry on its first
 * colon only (so {@code scheme://host:port} survives), and composing the fixed
 * callback URL. The env-backed {@code MAPPING}/{@code resolve}/{@code isKnown}
 * surface is exercised against the (empty) default config to assert fail-closed.
 */
class CallbackRegistryTest {

    @Test
    void parsesSingleEntry() {
        Map<String, String> m = CallbackRegistry.parse("my-agent-a:https://hosta.local");
        assertEquals("https://hosta.local", m.get("my-agent-a"));
        assertEquals(1, m.size());
    }

    @Test
    void parsesMultipleEntriesAndPreservesSchemeAndPort() {
        Map<String, String> m = CallbackRegistry.parse(
                "my-agent-a:https://hosta.local,orchestrator:http://orchestrator-api:8000");
        assertEquals("https://hosta.local", m.get("my-agent-a"));
        // First-colon-only split must keep the http:// and the :8000 port intact.
        assertEquals("http://orchestrator-api:8000", m.get("orchestrator"));
    }

    @Test
    void parseToleratesWhitespaceAndEmptyInput() {
        assertEquals("https://x.local",
                CallbackRegistry.parse("  a : https://x.local ").get("a"));
        assertEquals(0, CallbackRegistry.parse("").size());
        assertEquals(0, CallbackRegistry.parse("   ").size());
    }

    @Test
    void parseRejectsMalformedEntry() {
        assertThrows(IllegalArgumentException.class, () -> CallbackRegistry.parse("no-url-here"));
        assertThrows(IllegalArgumentException.class, () -> CallbackRegistry.parse(":https://x"));
        assertThrows(IllegalArgumentException.class, () -> CallbackRegistry.parse("name:"));
    }

    @Test
    void toCallbackUrlAppendsFixedPathAndNormalizesTrailingSlash() {
        assertEquals("https://hosta.local/api/tools/response",
                CallbackRegistry.toCallbackUrl("https://hosta.local"));
        assertEquals("https://hosta.local/api/tools/response",
                CallbackRegistry.toCallbackUrl("https://hosta.local/"));
        assertEquals("http://orchestrator-api:8000/api/tools/response",
                CallbackRegistry.toCallbackUrl("http://orchestrator-api:8000"));
    }

    @Test
    void resolveFailsClosedForUnknownService() {
        // Default config (no ALLOWED_HOST_MAPPING in the test env) → nothing is known.
        assertFalse(CallbackRegistry.isKnown("anything"));
        assertFalse(CallbackRegistry.isKnown(null));
        assertThrows(IllegalArgumentException.class, () -> CallbackRegistry.resolve("anything"));
        assertThrows(IllegalArgumentException.class, () -> CallbackRegistry.resolve(null));
    }
}
