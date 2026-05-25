import { KafkaConsumer, Producer, Message, TopicPartition } from "@confluentinc/kafka-javascript";

export interface ThinkConsumerConfig {
  agentName: string;
  brokers: string;
  claudeApiKey: string;
  systemPrompt: string;
  tools?: Record<string, unknown>[];
  claudeModel?: string;
  claudeMaxTokens?: number;
  claudeApiUrl?: string;
  /** Enable Anthropic prompt caching (cache_control breakpoints). Defaults to false. */
  promptCaching?: boolean;
  pollTimeoutMs?: number;
  systemPromptBuilder?: (basePrompt: string, context: Record<string, unknown>) => string;
  llmProvider?: string;
  geminiApiKey?: string;
  geminiModel?: string;
  geminiMaxTokens?: number;
  geminiApiUrl?: string;
  compactionUserMessageTrigger?: number;
  compactionUserMessageUntil?: number;
  compactionPrompt?: string;
}

function deriveThinkNames(agentName: string) {
  return {
    groupId: `${agentName}-think-consumer`,
    input: `${agentName}-enriched-message-input`,
    output: `${agentName}-think-request-response`,
    dlq: `${agentName}-think-dlq`,
  };
}
// Token pricing from environment variables (per-token, not per-million)
const INPUT_TOKEN_PRICE = process.env.INPUT_TOKEN_PRICE
  ? parseFloat(process.env.INPUT_TOKEN_PRICE)
  : null;
const OUTPUT_TOKEN_PRICE = process.env.OUTPUT_TOKEN_PRICE
  ? parseFloat(process.env.OUTPUT_TOKEN_PRICE)
  : null;

if (INPUT_TOKEN_PRICE == null || OUTPUT_TOKEN_PRICE == null) {
  console.warn(
    "WARN: INPUT_TOKEN_PRICE and/or OUTPUT_TOKEN_PRICE not set — cost will not be calculated"
  );
}

export class ThinkConsumerRunner {
  private consumer: KafkaConsumer;
  private producer: Producer;
  private config: ThinkConsumerConfig;
  private names: ReturnType<typeof deriveThinkNames>;
  private running = false;

  constructor(config: ThinkConsumerConfig) {
    this.config = {
      claudeModel: "claude-haiku-4-5-20251001",
      claudeMaxTokens: 4096,
      claudeApiUrl: "https://api.anthropic.com/v1/messages",
      promptCaching: process.env.PROMPT_CACHING?.toLowerCase() === "true",
      tools: [],
      llmProvider: "claude",
      geminiApiKey: "",
      geminiModel: "gemini-2.5-flash",
      geminiMaxTokens: 4096,
      geminiApiUrl: "https://generativelanguage.googleapis.com/v1beta",
      compactionUserMessageTrigger: -1,
      compactionUserMessageUntil: 2,
      compactionPrompt:
        "Summarize the following conversation concisely. " +
        "If the conversation starts with a previous summary, incorporate and extend it " +
        "rather than re-summarizing it. " +
        "Preserve key facts, decisions, user preferences, and any context needed " +
        "to continue the conversation naturally. Output only the summary.",
      ...config,
    };
    this.names = deriveThinkNames(config.agentName);

    const provider = (this.config.llmProvider || "claude").toLowerCase();
    if (provider === "gemini") {
      if (!this.config.geminiApiKey) {
        throw new Error("geminiApiKey is required when llmProvider='gemini'");
      }
      console.log(`Using Gemini LLM provider (model=${this.config.geminiModel})`);
    } else {
      if (!this.config.claudeApiKey) {
        throw new Error("claudeApiKey is required when llmProvider='claude'");
      }
      console.log(`Using Claude LLM provider (model=${this.config.claudeModel})`);
    }

    this.consumer = new KafkaConsumer(
      {
        "bootstrap.servers": config.brokers,
        "group.id": this.names.groupId,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": false,
      },
      {}
    );

    this.producer = new Producer(
      {
        "bootstrap.servers": config.brokers,
        acks: "all",
        "enable.idempotence": true,
      },
      {}
    );
  }

  async start(): Promise<void> {
    await this.connectConsumer();
    await this.connectProducer();

    this.consumer.subscribe([this.names.input]);
    this.running = true;

    console.log(`ThinkConsumerRunner started — listening on [${this.names.input}]`);

    process.on("SIGINT", () => this.stop());
    process.on("SIGTERM", () => this.stop());

    this.poll();
  }

  stop(): void {
    console.log("Shutting down ThinkConsumerRunner...");
    this.running = false;
    this.consumer.unsubscribe();
    this.consumer.disconnect();
    this.producer.disconnect();
  }

  private poll(): void {
    if (!this.running) return;

    this.consumer.consume(
      1,
      async (err: Error | null, messages: Message[]) => {
        if (err) {
          console.error("Consumer error:", err.message);
          setTimeout(() => this.poll(), this.config.pollTimeoutMs ?? 1000);
          return;
        }

        for (const msg of messages) {
          const key = msg.key ? msg.key.toString() : null;
          const value = msg.value ? msg.value.toString() : null;

          try {
            await this.processRecord(key, value, msg.topic, msg.partition, msg.offset);
          } catch (e: unknown) {
            const reason = e instanceof Error ? e.message : String(e);
            console.error(`Error processing offset ${msg.offset}:`, reason);
            await this.emitErrorResponse(key, value, reason, msg.topic, msg.partition, msg.offset);
            await this.sendToDlq(key, value, reason, msg.topic, msg.partition, msg.offset);
          }
        }

        if (this.running) {
          setImmediate(() => this.poll());
        }
      }
    );
  }

  private async processRecord(
    key: string | null,
    value: string | null,
    topic: string,
    partition: number,
    offset: number
  ): Promise<void> {
    const context = value ? JSON.parse(value) : {};

    const sessionId = key || context.sessionId || "";
    const userId = context.userId || "";
    const history: Record<string, unknown>[] = context.history || [];
    const latestInput: Record<string, unknown> = context.latestInput || {};
    const memoirContext: string = context.memoirContext || "";
    const cumulativeCost: number | null = context.cost != null ? Number(context.cost) : null;

    // Check session budget
    const budgetStr = process.env.BUDGET_PRICE_PER_SESSION;
    if (budgetStr && cumulativeCost != null) {
      const budget = parseFloat(budgetStr);
      if (cumulativeCost >= budget) {
        console.warn(
          `[${sessionId}] Session budget exceeded: $${cumulativeCost.toFixed(6)} >= $${budget.toFixed(2)}`
        );
        const budgetResponse = {
          sessionId,
          userId,
          cost: null,
          prevSessionCost: cumulativeCost,
          inputTokens: 0,
          outputTokens: 0,
          previousMessages: history,
          lastInputMessage: latestInput,
          lastInputResponse: [
            {
              sessionId,
              userId,
              role: "assistant",
              content: `You have used too many tokens. Session budget of $${budget.toFixed(2)} has been reached.`,
              timestamp: new Date().toISOString(),
            },
          ],
          toolUses: [],
          endTurn: true,
          timestamp: new Date().toISOString(),
        };
        await this.produce(this.names.output, key, JSON.stringify(budgetResponse));
        const tp: TopicPartition = { topic, partition, offset: offset + 1 };
        this.consumer.commitAsync([tp], (commitErr: Error | null) => {
          if (commitErr) console.error("Commit failed:", commitErr.message);
        });
        return;
      }
    }

    // Compact history if user message count exceeds trigger
    let effectiveHistory = history;
    let compactedHistory: Record<string, unknown>[] | null = null;
    const trigger = this.config.compactionUserMessageTrigger ?? -1;
    const keepLast = this.config.compactionUserMessageUntil ?? 2;

    if (trigger > 0 && effectiveHistory.length > 0) {
      const userMsgCount = effectiveHistory.filter((m) => m.role === "user").length;
      if (userMsgCount >= trigger) {
        const splitIdx = this.findCompactionSplitIndex(effectiveHistory, keepLast);
        if (splitIdx > 0) {
          const recentMessages = effectiveHistory.slice(splitIdx);
          // Skip compaction if we're in an active tool loop
          // (latestInput is a tool result)
          const midToolLoop = latestInput && latestInput.role === "tool";

          if (!midToolLoop) {
            console.log(
              `[${sessionId}] Compacting history: ${userMsgCount} user messages >= trigger ${trigger}, keeping from index ${splitIdx}`
            );
            const oldMessages = effectiveHistory.slice(0, splitIdx);

            const prov = (this.config.llmProvider || "claude").toLowerCase();
            let summaryText: string;
            if (prov === "gemini") {
              const summaryInput = this.toGeminiMessages(oldMessages, {});
              const summaryResp = await this.callGemini(this.config.compactionPrompt!, summaryInput, false);
              summaryText = this.extractGeminiText(summaryResp);
            } else {
              const summaryInput = this.toClaudeMessages(oldMessages, {});
              const summaryResp = await this.callClaude(this.config.compactionPrompt!, summaryInput, false);
              summaryText = this.extractClaudeText(summaryResp);
            }

            const summaryMsg: Record<string, unknown> = {
              sessionId,
              userId,
              role: "assistant",
              content: `[Conversation Summary]\n${summaryText}`,
              timestamp: new Date().toISOString(),
            };
            compactedHistory = [summaryMsg, ...recentMessages];
            effectiveHistory = compactedHistory;
            console.log(
              `[${sessionId}] History compacted: ${history.length} messages → ${compactedHistory.length}`
            );
          }
        }
      }
    }

    // Build system prompt
    const systemPrompt = this.buildSystemPrompt(memoirContext, context);

    const provider = (this.config.llmProvider || "claude").toLowerCase();
    let thinkResponse: Record<string, unknown>;

    if (provider === "gemini") {
      const geminiMessages = this.toGeminiMessages(effectiveHistory, latestInput);
      const response = await this.callGemini(systemPrompt, geminiMessages);
      thinkResponse = this.parseGeminiResponse(response, sessionId, userId, latestInput, effectiveHistory);
    } else {
      const messages = this.toClaudeMessages(effectiveHistory, latestInput);
      const response = await this.callClaude(systemPrompt, messages);
      thinkResponse = this.parseResponse(response, sessionId, userId, latestInput);
    }

    thinkResponse.prevSessionCost = cumulativeCost;
    thinkResponse.previousMessages = effectiveHistory;
    thinkResponse.lastInputMessage = latestInput;
    thinkResponse.lastInputResponse = thinkResponse.messages;
    delete thinkResponse.messages;

    // Produce to output topic
    await this.produce(this.names.output, key, JSON.stringify(thinkResponse));

    // Commit offset
    const tp: TopicPartition = { topic, partition, offset: offset + 1 };
    this.consumer.commitAsync([tp], (commitErr: Error | null) => {
      if (commitErr) console.error("Commit failed:", commitErr.message);
    });
  }

  private buildSystemPrompt(memoirContext: string, fullContext: Record<string, unknown>): string {
    let prompt: string;

    if (this.config.systemPromptBuilder) {
      prompt = this.config.systemPromptBuilder(this.config.systemPrompt, fullContext);
    } else {
      prompt = this.config.systemPrompt;
    }

    if (memoirContext) {
      prompt +=
        "\n\nUser memoir (known facts about this user from previous sessions):\n" +
        memoirContext +
        "\nUse the memoir to personalize your responses.";
    }

    return prompt;
  }

  private toClaudeMessages(
    history: Record<string, unknown>[],
    latestInput: Record<string, unknown>
  ): Record<string, unknown>[] {
    const messages: Record<string, unknown>[] = [];

    for (const msg of history) {
      const role = (msg.role as string) || "user";
      const content = msg.content;

      if (role === "tool") {
        // Convert tool results to user role with tool_result blocks
        const toolResults = Array.isArray(content) ? content : [content];
        const blocks: Record<string, unknown>[] = [];
        for (const result of toolResults) {
          if (typeof result === "object" && result !== null && "tool_use_id" in result) {
            blocks.push({
              type: "tool_result",
              tool_use_id: (result as Record<string, unknown>).tool_use_id,
              content: (result as Record<string, unknown>).content || "",
            });
          } else {
            blocks.push({ type: "text", text: String(result) });
          }
        }
        messages.push({ role: "user", content: blocks });
      } else if (role === "assistant") {
        if (Array.isArray(content)) {
          messages.push({ role: "assistant", content });
        } else {
          this.appendOrMerge(messages, "assistant", String(content));
        }
      } else {
        this.appendOrMerge(messages, "user", String(content));
      }
    }

    // Add latest input
    if (latestInput && latestInput.content) {
      this.appendOrMerge(messages, "user", String(latestInput.content));
    }

    return messages;
  }

  private appendOrMerge(messages: Record<string, unknown>[], role: string, text: string): void {
    const last = messages[messages.length - 1];
    if (last && last.role === role && typeof last.content === "string") {
      last.content = last.content + "\n" + text;
    } else {
      messages.push({ role, content: text });
    }
  }

  private async callClaude(
    systemPrompt: string,
    messages: Record<string, unknown>[],
    includeTools = true
  ): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = {
      model: this.config.claudeModel,
      max_tokens: this.config.claudeMaxTokens,
      system: this.config.promptCaching
        ? [
            {
              type: "text",
              text: systemPrompt,
              cache_control: { type: "ephemeral" },
            },
          ]
        : systemPrompt,
      messages,
    };

    if (includeTools && this.config.tools && this.config.tools.length > 0) {
      body.tools = this.config.tools;
    }

    const resp = await fetch(this.config.claudeApiUrl!, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": this.config.claudeApiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(120000),
    });

    if (!resp.ok) {
      const errorBody = await resp.text();
      throw new Error(`Claude API error ${resp.status}: ${errorBody}`);
    }

    return (await resp.json()) as Record<string, unknown>;
  }

  private parseResponse(
    response: Record<string, unknown>,
    sessionId: string,
    userId: string,
    latestInput: Record<string, unknown>
  ): Record<string, unknown> {
    const usage = (response.usage as Record<string, number>) || {};
    const inputTokens = usage.input_tokens || 0;
    const outputTokens = usage.output_tokens || 0;
    const cost =
      INPUT_TOKEN_PRICE != null && OUTPUT_TOKEN_PRICE != null
        ? (inputTokens / 1_000_000) * INPUT_TOKEN_PRICE + (outputTokens / 1_000_000) * OUTPUT_TOKEN_PRICE
        : null;

    const stopReason = response.stop_reason as string;
    const endTurn = stopReason !== "tool_use";

    const contentBlocks = (response.content as Record<string, unknown>[]) || [];
    const messages: Record<string, unknown>[] = [];
    const toolUses: Record<string, unknown>[] = [];
    const now = new Date().toISOString();

    const hasToolUse = contentBlocks.some((b) => b.type === "tool_use");

    if (hasToolUse) {
      messages.push({
        sessionId,
        userId,
        role: "assistant",
        content: contentBlocks,
        timestamp: now,
      });
    }

    for (const block of contentBlocks) {
      if (block.type === "text") {
        if (!hasToolUse) {
          messages.push({
            sessionId,
            userId,
            role: "assistant",
            content: block.text,
            timestamp: now,
          });
        }
      } else if (block.type === "tool_use") {
        toolUses.push({
          toolUseId: block.id,
          toolId: block.name,
          name: block.name,
          input: block.input,
          sessionId,
          totalTools: contentBlocks.filter((b) => b.type === "tool_use").length,
          timestamp: now,
        });
      }
    }

    return {
      sessionId,
      userId,
      cost: cost != null ? Math.round(cost * 1_000_000) / 1_000_000 : null,
      inputTokens,
      outputTokens,
      messages,
      toolUses,
      endTurn,
      timestamp: now,
    };
  }

  // ── Gemini helpers ──────────────────────────────────────────────────────

  private toGeminiMessages(
    history: Record<string, unknown>[],
    latestInput: Record<string, unknown>
  ): Record<string, unknown>[] {
    // Build tool_use_id → name mapping from assistant messages
    const toolIdToName: Record<string, string> = {};
    for (const msg of history) {
      if (msg.role === "assistant" && Array.isArray(msg.content)) {
        for (const block of msg.content as Record<string, unknown>[]) {
          if (block.type === "tool_use" && block.id && block.name) {
            toolIdToName[block.id as string] = block.name as string;
          }
        }
      }
    }

    const contents: Record<string, unknown>[] = [];

    for (const msg of history) {
      const role = (msg.role as string) || "user";
      const content = msg.content;

      if (role === "tool") {
        const parts = this.buildFunctionResponseParts(content, toolIdToName);
        if (parts.length > 0) {
          contents.push({ role: "user", parts });
        }
      } else if (role === "assistant") {
        if (Array.isArray(content)) {
          const parts: Record<string, unknown>[] = [];
          for (const block of content as Record<string, unknown>[]) {
            if (block.type === "text") {
              const text = block.text as string;
              if (text) parts.push({ text });
            } else if (block.type === "tool_use") {
              parts.push({
                functionCall: { name: block.name, args: block.input || {} },
              });
            }
          }
          if (parts.length > 0) {
            contents.push({ role: "model", parts });
          }
        } else {
          this.appendOrMergeGemini(contents, "model", String(content));
        }
      } else {
        this.appendOrMergeGemini(contents, "user", String(content));
      }
    }

    if (latestInput && latestInput.content) {
      this.appendOrMergeGemini(contents, "user", String(latestInput.content));
    }

    return contents;
  }

  private appendOrMergeGemini(
    contents: Record<string, unknown>[],
    role: string,
    text: string
  ): void {
    const last = contents[contents.length - 1];
    if (last && last.role === role) {
      const parts = last.parts as Record<string, unknown>[];
      if (parts.length > 0 && "text" in parts[parts.length - 1]) {
        parts.push({ text });
        return;
      }
    }
    contents.push({ role, parts: [{ text }] });
  }

  private buildFunctionResponseParts(
    content: unknown,
    toolIdToName: Record<string, string>
  ): Record<string, unknown>[] {
    const parts: Record<string, unknown>[] = [];
    const results = Array.isArray(content) ? content : [content];

    for (const result of results) {
      if (typeof result === "object" && result !== null && "tool_use_id" in result) {
        const r = result as Record<string, unknown>;
        const name = toolIdToName[r.tool_use_id as string] || "unknown";
        let resData = r.result ?? r.content ?? "{}";
        if (typeof resData === "string") {
          try {
            resData = JSON.parse(resData);
          } catch {
            resData = { result: resData };
          }
        }
        parts.push({ functionResponse: { name, response: resData } });
      }
    }
    return parts;
  }

  private async callGemini(
    systemPrompt: string,
    contents: Record<string, unknown>[],
    includeTools = true
  ): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = {
      system_instruction: { parts: [{ text: systemPrompt }] },
      contents,
      generationConfig: { maxOutputTokens: this.config.geminiMaxTokens },
    };

    if (includeTools && this.config.tools && this.config.tools.length > 0) {
      const funcDecls = this.config.tools.map((tool) => ({
        name: tool.name,
        description: tool.description || "",
        ...(tool.input_schema ? { parameters: tool.input_schema } : {}),
      }));
      body.tools = [{ function_declarations: funcDecls }];
    }

    const url = `${this.config.geminiApiUrl}/models/${this.config.geminiModel}:generateContent?key=${this.config.geminiApiKey}`;

    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(120000),
    });

    if (!resp.ok) {
      const errorBody = await resp.text();
      throw new Error(`Gemini API error ${resp.status}: ${errorBody}`);
    }

    return (await resp.json()) as Record<string, unknown>;
  }

  private parseGeminiResponse(
    response: Record<string, unknown>,
    sessionId: string,
    userId: string,
    latestInput: Record<string, unknown>,
    _history: Record<string, unknown>[]
  ): Record<string, unknown> {
    const usage = (response.usageMetadata as Record<string, number>) || {};
    const inputTokens = usage.promptTokenCount || 0;
    const outputTokens = usage.candidatesTokenCount || 0;
    const cost =
      INPUT_TOKEN_PRICE != null && OUTPUT_TOKEN_PRICE != null
        ? (inputTokens / 1_000_000) * INPUT_TOKEN_PRICE + (outputTokens / 1_000_000) * OUTPUT_TOKEN_PRICE
        : null;

    const candidates = (response.candidates as Record<string, unknown>[]) || [];
    if (candidates.length === 0) throw new Error("No candidates in Gemini response");

    const candidate = candidates[0];
    const candidateContent = (candidate.content as Record<string, unknown>) || {};
    const parts = (candidateContent.parts as Record<string, unknown>[]) || [];

    const hasFunctionCall = parts.some((p) => "functionCall" in p);
    const endTurn = !hasFunctionCall;

    const messages: Record<string, unknown>[] = [];
    const toolUses: Record<string, unknown>[] = [];
    const now = new Date().toISOString();

    // Build content blocks in Claude-compatible format
    const contentBlocks: Record<string, unknown>[] = [];

    for (const part of parts) {
      if ("text" in part) {
        contentBlocks.push({ type: "text", text: part.text });
      } else if ("functionCall" in part) {
        const fc = part.functionCall as Record<string, unknown>;
        const toolUseId = "toolu_" + crypto.randomUUID().replace(/-/g, "").substring(0, 20);
        contentBlocks.push({
          type: "tool_use",
          id: toolUseId,
          name: fc.name,
          input: fc.args || {},
        });
        toolUses.push({
          toolUseId,
          toolId: fc.name,
          name: fc.name,
          input: fc.args || {},
          sessionId,
          totalTools: parts.filter((p) => "functionCall" in p).length,
          timestamp: now,
        });
      }
    }

    if (hasFunctionCall) {
      messages.push({ sessionId, userId, role: "assistant", content: contentBlocks, timestamp: now });
    } else {
      for (const block of contentBlocks) {
        if (block.type === "text") {
          messages.push({ sessionId, userId, role: "assistant", content: block.text, timestamp: now });
        }
      }
    }

    return {
      sessionId,
      userId,
      cost: cost != null ? Math.round(cost * 1_000_000) / 1_000_000 : null,
      inputTokens,
      outputTokens,
      messages,
      toolUses,
      endTurn,
      timestamp: now,
    };
  }

  private findCompactionSplitIndex(history: Record<string, unknown>[], keepLast: number): number {
    if (!history || keepLast <= 0) return -1;
    const totalUser = history.filter((m) => m.role === "user").length;
    if (totalUser <= keepLast) return -1;
    const target = totalUser - keepLast;
    let seen = 0;
    for (let i = 0; i < history.length; i++) {
      if (history[i].role === "user") {
        seen++;
        if (seen > target) return i;
      }
    }
    return -1;
  }

  private hasToolUseContent(msg: Record<string, unknown>): boolean {
    const content = msg.content;
    if (Array.isArray(content)) {
      return content.some(
        (b) => typeof b === "object" && b !== null && (b as Record<string, unknown>).type === "tool_use"
      );
    }
    return false;
  }

  private extractClaudeText(response: Record<string, unknown>): string {
    const blocks = (response.content as Record<string, unknown>[]) || [];
    return blocks
      .filter((b) => b.type === "text")
      .map((b) => (b.text as string) || "")
      .join("\n");
  }

  private extractGeminiText(response: Record<string, unknown>): string {
    const candidates = (response.candidates as Record<string, unknown>[]) || [];
    if (candidates.length === 0) return "";
    const content = (candidates[0].content as Record<string, unknown>) || {};
    const parts = (content.parts as Record<string, unknown>[]) || [];
    return parts
      .filter((p) => "text" in p)
      .map((p) => (p.text as string) || "")
      .join("\n");
  }

  private produce(topic: string, key: string | null, value: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this.producer.produce(
        topic,
        null,
        Buffer.from(value),
        key ?? undefined,
        Date.now(),
        undefined,
        undefined,
        (prodErr: Error | null) => {
          if (prodErr) reject(prodErr);
          else resolve();
        }
      );
      this.producer.flush(5000, (flushErr: Error | null) => {
        if (flushErr) console.error("Flush error:", flushErr.message);
      });
    });
  }

  private async emitErrorResponse(
    key: string | null,
    value: string | null,
    reason: string,
    topic: string,
    partition: number,
    offset: number
  ): Promise<void> {
    try {
      const sessionId = key || "";
      let userId = "";
      if (value) {
        try {
          const ctx = JSON.parse(value);
          userId = ctx.userId || "";
        } catch {}
      }

      const now = new Date().toISOString();
      const errorResponse = {
        sessionId,
        userId,
        cost: null,
        prevSessionCost: null,
        inputTokens: 0,
        outputTokens: 0,
        messages: [
          {
            sessionId,
            userId,
            role: "assistant",
            content: `Sorry, an error occurred while processing your request: ${reason}`,
            timestamp: now,
          },
        ],
        toolUses: [],
        endTurn: true,
        timestamp: now,
      };

      await this.produce(this.names.output, key, JSON.stringify(errorResponse));
      console.log(`[${sessionId}] Emitted error response to ${this.names.output}`);
    } catch (ex) {
      console.error("Failed to emit error response:", ex);
    }
  }

  private async sendToDlq(
    key: string | null,
    value: string | null,
    reason: string,
    topic: string,
    partition: number,
    offset: number
  ): Promise<void> {
    await this.produce(
      this.names.dlq,
      key,
      JSON.stringify({ originalValue: value, error: reason })
    );

    const tp: TopicPartition = { topic, partition, offset: offset + 1 };
    this.consumer.commitAsync([tp], (err: Error | null) => {
      if (err) console.error("DLQ commit failed:", err.message);
    });
  }

  private connectConsumer(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.consumer.connect({}, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private connectProducer(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.producer.connect({}, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
}
