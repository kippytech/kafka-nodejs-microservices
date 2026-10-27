import { Pool, type Pool as PgPool } from "pg";
import { logger } from "../logger/logger";

const pools = new Map<string, Pool>();

export async function verifyDatabase(pool: PgPool, service: string, retries = 4): Promise<void> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      await pool.query("SELECT 1");
      return;
    } catch (err) {
      if (attempt === retries) throw err;
      const delayMs = Math.min(4_000, 250 * 2 ** attempt) + Math.floor(Math.random() * 100);
      logger.warn({ err, service, attempt: attempt + 1, delayMs }, "Database readiness check failed; retrying");
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

export function getPool(envName: string): Pool {
  let pool = pools.get(envName);
  if (!pool) {
    const connectionString = process.env[envName];

    if (!connectionString) {
      throw new Error(`${envName} is not set`);
    }

    pool = new Pool({
      connectionString,
      max: Number(process.env.PG_POOL_MAX ?? 10),
      connectionTimeoutMillis: Number(process.env.PG_CONNECT_TIMEOUT_MS ?? 5_000),
      idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS ?? 30_000),
      statement_timeout: Number(process.env.PG_STATEMENT_TIMEOUT_MS ?? 15_000),
      query_timeout: Number(process.env.PG_QUERY_TIMEOUT_MS ?? 20_000),
      keepAlive: true,
      keepAliveInitialDelayMillis: 10_000,
    });
    pool.on("error", (err) => logger.error({ err, database: envName }, "Idle PostgreSQL client error"));
    pools.set(envName, pool);
  }

  return pool;
}

export async function closePool(): Promise<void> {
  const results = await Promise.allSettled([...pools.values()].map((pool) => pool.end()));
  pools.clear();
  const failed = results.find((result): result is PromiseRejectedResult => result.status === "rejected");
  if (failed) throw failed.reason;
}
