package io.flightdeck.streams.processors;

/**
 * AccumulateSessionContextProcessor has been removed from the topology.
 * Session history is now reconstructed from ThinkResponse fields
 * (previousMessages, lastInputMessage, lastInputResponse) in EnrichInputMessageProcessor.
 */
class AccumulateSessionContextProcessorTest {
    // intentionally empty — processor removed
}
