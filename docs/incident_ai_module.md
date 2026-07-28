# AI-assisted incident module

The incident collector normalizes exceptions and pipeline metadata: topic, batch, failed count, quarantine rate, retry attempts, checkpoint, and dependency health. The default `MockAIProvider` deterministically identifies a likely component, assigns severity, and returns recovery and prevention advice. This makes development, tests, and offline operations independent of a paid service.

With `AI_PROVIDER=openai` and `OPENAI_API_KEY`, the adapter sends only the collected incident context and asks for strict JSON. The response must contain the documented fields. Missing fields, network errors, invalid JSON, or provider failures automatically fall back to the mock implementation and preserve the fallback reason. API keys are read from environment only.

AI output is advisory. It must not automatically delete checkpoints, alter Kafka offsets, or mutate infrastructure. Evidence is attached to every report so operators can distinguish observed facts from the provider's likely-cause inference.
