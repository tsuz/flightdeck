package io.flightdeck.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;

/**
 * Verifies and decodes the HMAC-signed callback token minted by the tool
 * consumer SDK when it dispatches an async tool.
 *
 * <p>Token layout (must match the SDK's {@code signCallbackToken}):
 * <pre>
 *   token = base64url(payloadJson) "." base64url(HMAC_SHA256(secret, payloadJson))
 * </pre>
 *
 * <p>The HMAC is recomputed over the exact decoded payload bytes, so JSON key
 * ordering is irrelevant — we never re-serialise during verification.
 */
public final class ToolCallbackToken {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Base64.Decoder URL_DECODER = Base64.getUrlDecoder();

    private ToolCallbackToken() {}

    /** Decoded token payload. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(
            @JsonProperty("session_id")  String sessionId,
            @JsonProperty("tool_use_id") String toolUseId,
            @JsonProperty("tool_id")     String toolId,
            @JsonProperty("name")        String name,
            @JsonProperty("total_tools") int totalTools,
            @JsonProperty("agent")       String agent,
            @JsonProperty("iat")         long iat,
            @JsonProperty("exp")         long exp
    ) {}

    /** Raised when a token is malformed, has a bad signature, or has expired. */
    public static class InvalidTokenException extends Exception {
        public InvalidTokenException(String message) { super(message); }
    }

    /**
     * Verifies the token signature against {@code secret} and its expiry against
     * the current time, then returns the decoded payload.
     *
     * @throws InvalidTokenException if the token is malformed, the signature
     *         does not match, or the token has expired.
     */
    public static Payload verify(String token, String secret) throws InvalidTokenException {
        if (token == null || token.isBlank()) {
            throw new InvalidTokenException("Missing token");
        }
        if (secret == null || secret.isBlank()) {
            throw new InvalidTokenException("Server is not configured to verify callback tokens");
        }

        int dot = token.indexOf('.');
        if (dot <= 0 || dot == token.length() - 1) {
            throw new InvalidTokenException("Malformed token");
        }

        byte[] payloadBytes;
        byte[] providedSig;
        try {
            payloadBytes = URL_DECODER.decode(token.substring(0, dot));
            providedSig  = URL_DECODER.decode(token.substring(dot + 1));
        } catch (IllegalArgumentException e) {
            throw new InvalidTokenException("Token is not valid base64url");
        }

        byte[] expectedSig = hmacSha256(secret, payloadBytes);
        // Constant-time comparison to avoid signature timing oracles.
        if (!MessageDigest.isEqual(expectedSig, providedSig)) {
            throw new InvalidTokenException("Bad token signature");
        }

        Payload payload;
        try {
            payload = MAPPER.readValue(payloadBytes, Payload.class);
        } catch (Exception e) {
            throw new InvalidTokenException("Token payload is not valid JSON");
        }

        if (payload.exp() > 0 && Instant.now().getEpochSecond() > payload.exp()) {
            throw new InvalidTokenException("Token has expired");
        }
        if (payload.sessionId() == null || payload.toolUseId() == null) {
            throw new InvalidTokenException("Token missing session_id or tool_use_id");
        }
        return payload;
    }

    private static byte[] hmacSha256(String secret, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException("HMAC computation failed", e);
        }
    }
}
