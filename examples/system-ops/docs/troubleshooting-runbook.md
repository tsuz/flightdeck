# Kafka Troubleshooting Runbook

## 1. High Consumer Lag

**Symptoms**: Consumer group lag exceeds 1000 messages per partition.

**Diagnosis**:
1. Use `kafka_consumer_group_lag` tool to check lag for the affected consumer group.
2. Use `kafka_describe_topic` to verify partition count and check if partitions are balanced.
3. Check if any consumer instances are down (fewer instances than expected).

**Resolution**:
- If lag is growing steadily: scale up consumer instances to match partition count.
- If lag spiked suddenly: check for a burst in producer traffic. Lag should recover once the burst passes.
- If one partition has much higher lag than others: check for a hot key causing partition skew. Consider repartitioning.
- If all consumers are healthy but lag persists: the consumers may be too slow. Profile the consumer processing logic or increase `max.poll.records`.

**Escalation**: If lag exceeds 10,000 messages per partition for more than 15 minutes, page the on-call engineer.

---

## 2. Under-Replicated Partitions

**Symptoms**: Partitions have ISR count less than the replication factor.

**Diagnosis**:
1. Use `kafka_describe_topic` to check ISR list for the affected topic.
2. Use `kafka_broker_health` to verify all brokers are online.
3. Check if a broker recently restarted (it needs time to catch up).

**Resolution**:
- If a broker is offline: restart the broker. Partitions will re-replicate once it rejoins.
- If a broker is online but not in ISR: it may be falling behind. Check disk I/O and network throughput on that broker.
- If the broker is consistently slow: check for disk full conditions or noisy-neighbor issues on the host.

**Escalation**: If any topic has ISR < min.insync.replicas for more than 5 minutes, producers will start failing. This is a P1 incident.

---

## 3. Topic Not Found or Missing

**Symptoms**: Producers or consumers report "unknown topic" errors.

**Diagnosis**:
1. Use `kafka_list_topics` to verify the topic exists.
2. Check if `auto.create.topics.enable` is set to true on the cluster.

**Resolution**:
- If the topic was accidentally deleted: use `kafka_create_topic` to recreate it with the correct partition count and replication factor. Refer to the architecture document for the expected configuration.
- If the topic never existed: create it with appropriate settings.
- After recreation: restart affected consumers to force a rebalance.

**Warning**: Recreating a compacted topic (like `audit-log`) after deletion means historical data is lost. Check backups first.

---

## 4. Broker Disk Full

**Symptoms**: Broker logs show "No space left on device". Producers receive errors.

**Diagnosis**:
1. Use `kafka_broker_health` to check broker status.
2. Check which topics are consuming the most disk space.

**Resolution**:
- Identify topics with excessive retention. Reduce `retention.ms` or `retention.bytes` for non-critical topics.
- Delete old consumer group offsets for inactive groups using cluster admin tools.
- If a single topic is disproportionately large: check if the retention policy is correct per the architecture document.

**Preventive**: Set up disk usage alerts at 70% and 85% thresholds.

---

## 5. Slow Producer Throughput

**Symptoms**: Producers report high latency or timeouts when sending messages.

**Diagnosis**:
1. Use `kafka_broker_health` to check broker load.
2. Use `kafka_describe_topic` to check if the topic has enough partitions for the throughput.
3. Check producer `acks` setting — `acks=all` is slower but required for critical topics.

**Resolution**:
- If broker CPU/IO is high: the cluster may need more brokers.
- If a single topic is the bottleneck: increase partition count to allow more parallelism.
- If `acks=all` and `min.insync.replicas=2`: this is expected behavior. Latency is bounded by the slowest in-sync replica.
- Check for large message sizes — messages over 1MB require `max.message.bytes` to be increased on both broker and topic.

---

## 6. Consumer Rebalancing Storms

**Symptoms**: Consumers frequently rejoin the group, causing processing pauses.

**Diagnosis**:
1. Use `kafka_consumer_group_lag` to check if lag spikes correlate with rebalances.
2. Check consumer logs for `session.timeout.ms` or `max.poll.interval.ms` violations.

**Resolution**:
- Increase `session.timeout.ms` (default 45s) if consumers are healthy but slow to heartbeat.
- Increase `max.poll.interval.ms` if processing takes longer than the default 5 minutes.
- Use static group membership (`group.instance.id`) to avoid rebalances during rolling restarts.
- Ensure consumer instances have stable network connectivity.

---

## 7. Message Ordering Issues

**Symptoms**: Messages appear out of order within a partition.

**Diagnosis**:
1. Verify that the producer is using a consistent partition key.
2. Check if `enable.idempotence=true` and `max.in.flight.requests.per.connection <= 5` on the producer.

**Resolution**:
- If ordering matters, ensure a single partition key is used per entity (e.g., order ID).
- Enable idempotent producers to prevent duplicate messages on retry.
- If using transactions, ensure `transactional.id` is set correctly.

---

## General Troubleshooting Steps

For any unknown issue:
1. First, check broker health with `kafka_broker_health`.
2. Then, list topics with `kafka_list_topics` to verify the cluster state.
3. Check consumer group lag with `kafka_consumer_group_lag` for affected groups.
4. Describe specific topics with `kafka_describe_topic` for partition and ISR details.
5. Cross-reference findings with the architecture document for expected configuration.
