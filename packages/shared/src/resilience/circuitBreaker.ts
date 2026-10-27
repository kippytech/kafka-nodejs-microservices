export type CircuitState = "closed" | "open" | "half-open";

export type CircuitBreakerOptions = {
  name: string;
  failureThreshold?: number;
  openMs?: number;
  halfOpenMaxCalls?: number;
  onStateChange?: (previous: CircuitState, next: CircuitState) => void;
};

export type CircuitPermit = {
  success(): void;
  failure(): void;
  ignore(): void;
};

export class CircuitOpenError extends Error {
  constructor(readonly breakerName: string, readonly retryAfterMs: number) {
    super(`Circuit breaker "${breakerName}" is ${retryAfterMs > 0 ? "open" : "testing recovery"}`);
    this.name = "CircuitOpenError";
  }
}

/** A process-local breaker for a single synchronous dependency. */
export class CircuitBreaker {
  private state: CircuitState = "closed";
  private failures = 0;
  private openedAt = 0;
  private halfOpenInFlight = 0;
  private readonly failureThreshold: number;
  private readonly openMs: number;
  private readonly halfOpenMaxCalls: number;

  constructor(private readonly options: CircuitBreakerOptions) {
    this.failureThreshold = positiveInteger(options.failureThreshold, 5);
    this.openMs = positiveInteger(options.openMs, 10_000);
    this.halfOpenMaxCalls = positiveInteger(options.halfOpenMaxCalls, 1);
  }

  get name() { return this.options.name; }

  getState(now = Date.now()): CircuitState {
    this.refreshState(now);
    return this.state;
  }

  getRetryAfterMs(now = Date.now()): number {
    this.refreshState(now);
    if (this.state === "open") return Math.max(1, this.openMs - (now - this.openedAt));
    if (this.state === "half-open") return 1_000;
    return 0;
  }

  acquire(now = Date.now()): CircuitPermit | undefined {
    this.refreshState(now);
    if (this.state === "open") return undefined;
    if (this.state === "half-open") {
      if (this.halfOpenInFlight >= this.halfOpenMaxCalls) return undefined;
      this.halfOpenInFlight++;
    }

    const acquiredIn = this.state;
    let completed = false;
    const finish = (outcome: "success" | "failure" | "ignore") => {
      if (completed) return;
      completed = true;
      this.complete(acquiredIn, outcome);
    };
    return {
      success: () => finish("success"),
      failure: () => finish("failure"),
      ignore: () => finish("ignore"),
    };
  }

  async execute<T>(
    operation: () => Promise<T>,
    options: { isFailure?: (value: T) => boolean; shouldTrip?: (error: unknown) => boolean } = {},
  ): Promise<T> {
    const permit = this.acquire();
    if (!permit) throw new CircuitOpenError(this.name, this.getRetryAfterMs());
    try {
      const result = await operation();
      if (options.isFailure?.(result)) permit.failure();
      else permit.success();
      return result;
    } catch (error) {
      if (options.shouldTrip && !options.shouldTrip(error)) permit.success();
      else permit.failure();
      throw error;
    }
  }

  private refreshState(now: number) {
    if (this.state === "open" && now - this.openedAt >= this.openMs) {
      this.transition("half-open");
      this.halfOpenInFlight = 0;
    }
  }

  private complete(acquiredIn: CircuitState, outcome: "success" | "failure" | "ignore") {
    if (acquiredIn === "half-open" && this.state === "half-open") {
      this.halfOpenInFlight = Math.max(0, this.halfOpenInFlight - 1);
      if (outcome === "failure") this.open();
      else if (outcome === "success") {
        this.transition("closed");
        this.failures = 0;
        this.halfOpenInFlight = 0;
      }
      return;
    }
    if (acquiredIn !== "closed" || this.state !== "closed" || outcome === "ignore") return;
    if (outcome === "success") this.failures = 0;
    else if (++this.failures >= this.failureThreshold) this.open();
  }

  private open() {
    this.transition("open");
    this.openedAt = Date.now();
    this.halfOpenInFlight = 0;
  }

  private transition(next: CircuitState) {
    if (this.state === next) return;
    const previous = this.state;
    this.state = next;
    this.options.onStateChange?.(previous, next);
  }
}

function positiveInteger(value: number | undefined, fallback: number): number {
  return Number.isInteger(value) && value! > 0 ? value! : fallback;
}
