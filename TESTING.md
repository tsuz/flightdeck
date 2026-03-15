
Test message goes into the system

> kafka-topics --bootstrap-server localhost:9092 --list
__consumer_offsets
axon-streams-enrich-message-context-store-changelog
message-input
session-context

> kafka-console-producer --bootstrap-server localhost:9092 --topic message-input 

> kafka-topics --bootstrap-server localhost:9092 --create --topic enriched-message-input
Created topic enriched-message-input.


> kafka-topics --bootstrap-server localhost:9092 --create --topic think-request-response
Created topic think-request-response.

{ "session_id": "sess_abc123", "user_id": "user_42", "role": "user", "content": "What is the weather like today in Tokyo?", "timestamp": "2026-03-14T10:30:00Z", "metadata": { "locale": "en-US", "client": "web", "ip_address": "192.168.1.1" } }

