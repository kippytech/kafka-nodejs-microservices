import assert from "node:assert/strict";
import test from "node:test";
import { CircuitBreaker, CircuitOpenError } from "../packages/shared/src/resilience/circuitBreaker";

test("circuit breaker opens after consecutive failures and closes after a successful probe", async () => {
  const breaker = new CircuitBreaker({ name: "test-service", failureThreshold: 2, openMs: 20 });

  breaker.acquire()!.failure();
  breaker.acquire()!.success();
  breaker.acquire()!.failure();
  breaker.acquire()!.failure();
  assert.equal(breaker.getState(), "open");
  await assert.rejects(() => breaker.execute(async () => "unreachable"), CircuitOpenError);

  await new Promise((resolve) => setTimeout(resolve, 25));
  const probe = breaker.acquire();
  assert.ok(probe);
  assert.equal(breaker.getState(), "half-open");
  probe.success();
  assert.equal(breaker.getState(), "closed");
});

test("circuit breaker can exclude expected application errors from its failure count", async () => {
  const breaker = new CircuitBreaker({ name: "test-service", failureThreshold: 1 });
  await assert.rejects(() => breaker.execute(async () => { throw new Error("expected denial"); }, { shouldTrip: () => false }));
  assert.equal(breaker.getState(), "closed");
});
