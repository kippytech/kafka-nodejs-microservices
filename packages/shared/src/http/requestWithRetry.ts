export async function requestWithRetry(
  url: string | URL,
  init: RequestInit,
  options: { timeoutMs?: number; retries?: number; baseDelayMs?: number } = {},
): Promise<Response> {
  const method = (init.method ?? "GET").toUpperCase();
  const retries = method === "GET" ? Math.max(0, options.retries ?? 1) : 0;
  const timeoutMs = options.timeoutMs ?? 1_500;
  let lastError: unknown;

  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("Upstream request timed out")), timeoutMs);
    try {
      const response = await fetch(url, { ...init, signal: controller.signal });
      if (![502, 503, 504].includes(response.status) || attempt === retries) return response;
      await response.body?.cancel();
    } catch (err) {
      lastError = err;
      if (attempt === retries) throw err;
    } finally {
      clearTimeout(timer);
    }
    const delay = (options.baseDelayMs ?? 100) * 2 ** attempt + Math.floor(Math.random() * 50);
    await new Promise((resolve) => setTimeout(resolve, delay));
  }
  throw lastError instanceof Error ? lastError : new Error("Upstream request failed");
}
