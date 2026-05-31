package io.flightdeck.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves a logical callback-service name to the trusted base URL configured for
 * it, then appends the fixed callback path ({@value #CALLBACK_PATH}).
 *
 * <p>The mapping is loaded once from the {@code ALLOWED_HOST_MAPPING} environment
 * variable: a comma-separated list of {@code name:baseUrl} entries. Only the first
 * colon of each entry separates the name from the URL, so the base URL keeps its
 * own {@code scheme://host[:port]} colons:
 *
 * <pre>
 *   ALLOWED_HOST_MAPPING=my-agent-a:https://hosta.local,my-agent-c:http://hostc.local
 * </pre>
 *
 * <p><b>Why this exists.</b> A caller (a peer agent) supplies only the service
 * <em>name</em> in its {@code reply} descriptor; the destination URL is chosen
 * here from operator-controlled config, never from caller input. An untrusted
 * caller therefore cannot steer the server-side callback at an arbitrary host —
 * the SSRF primitive that an attacker-controlled {@code endpoint} would create is
 * structurally removed. Unknown names fail closed.
 */
final class CallbackRegistry {

    /** Fixed path appended to every resolved base URL. */
    static final String CALLBACK_PATH = "/api/tools/response";

    private static final Map<String, String> MAPPING =
            parse(ChatApiApp.env("ALLOWED_HOST_MAPPING", ""));

    private CallbackRegistry() {}

    /** True if {@code service} resolves to a configured base URL. */
    static boolean isKnown(String service) {
        return service != null && MAPPING.containsKey(service);
    }

    /**
     * Resolves the full callback URL for a service name.
     *
     * @throws IllegalArgumentException if the name is not configured (fail closed)
     */
    static String resolve(String service) {
        String base = service == null ? null : MAPPING.get(service);
        if (base == null) {
            throw new IllegalArgumentException("unknown callbackService: " + service);
        }
        return toCallbackUrl(base);
    }

    /** Strips any trailing slash from the base URL and appends the fixed callback path. */
    static String toCallbackUrl(String base) {
        String trimmed = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        return trimmed + CALLBACK_PATH;
    }

    /** Parses {@code name:baseUrl} entries, splitting each on its FIRST colon only. */
    static Map<String, String> parse(String raw) {
        Map<String, String> mapping = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) {
            return Collections.unmodifiableMap(mapping);
        }
        for (String entry : raw.split(",")) {
            String e = entry.trim();
            if (e.isEmpty()) continue;
            int sep = e.indexOf(':');
            String name = sep > 0 ? e.substring(0, sep).trim() : "";
            String url = sep > 0 ? e.substring(sep + 1).trim() : "";
            if (name.isEmpty() || url.isEmpty()) {
                throw new IllegalArgumentException(
                        "Malformed ALLOWED_HOST_MAPPING entry (expected name:baseUrl): " + entry);
            }
            mapping.put(name, url);
        }
        return Collections.unmodifiableMap(mapping);
    }
}
