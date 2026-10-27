import { config } from "dotenv";
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Pool, type PoolClient } from "pg";

config({ path: resolve(process.cwd(), ".env") });

const migrations: Record<string, { env: string; directory: string }> = {
  auth: { env: "USER_DATABASE_URL", directory: "sql/auth" },
  task: { env: "TASK_DATABASE_URL", directory: "sql/task" },
  media: { env: "MEDIA_DATABASE_URL", directory: "sql/media" },
  workflow: { env: "WORKFLOW_DATABASE_URL", directory: "sql/workflow" },
};

type Migration = {
  name: string;
  sql: string;
  checksum: string;
};

async function loadMigrations(directory: string): Promise<Migration[]> {
  const files = (await readdir(resolve(process.cwd(), directory)))
    .filter((name) => name.endsWith(".sql"))
    .sort();

  return Promise.all(
    files.map(async (name) => {
      const sql = await readFile(
        resolve(process.cwd(), directory, name),
        "utf8",
      );

      const checksum = createHash("sha256")
        .update(sql)
        .digest("hex");

      return {
        name,
        sql,
        checksum,
      };
    }),
  );
}

async function createMigrationTable(client: PoolClient) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      name TEXT PRIMARY KEY,
      checksum TEXT NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
}

async function acquireMigrationLock(client: PoolClient) {
  await client.query(`
    SELECT pg_advisory_lock(
      hashtextextended('nodejs-microservices:migrations', 0)
    )
  `);
}

async function releaseMigrationLock(client: PoolClient) {
  await client.query(`
    SELECT pg_advisory_unlock(
      hashtextextended('nodejs-microservices:migrations', 0)
    )
  `);
}

async function migrate(service: string) {
  const target = migrations[service];

  if (!target) {
    throw new Error(
      `Unknown service '${service}'. Choose ${Object.keys(migrations).join(", ")}`,
    );
  }

  const url = process.env[target.env];

  if (!url) {
    throw new Error(`${target.env} is not set`);
  }

  const pool = new Pool({
    connectionString: url,
  });

  const client = await pool.connect();

  try {
    await acquireMigrationLock(client);

    await createMigrationTable(client);

    const migrationFiles = await loadMigrations(target.directory);

    const { rows: applied } = await client.query<{
      name: string;
      checksum: string;
    }>(`
      SELECT name, checksum
      FROM schema_migrations
      ORDER BY name
    `);

    const appliedMigrations = new Map(
      applied.map((migration) => [
        migration.name,
        migration.checksum,
      ]),
    );

    for (const migration of migrationFiles) {
      const appliedChecksum = appliedMigrations.get(migration.name);

      if (appliedChecksum) {
        if (appliedChecksum !== migration.checksum) {
          throw new Error(
            `Migration '${migration.name}' was modified after it was applied`,
          );
        }

        console.log(`Already migrated ${service}: ${migration.name}`);
        continue;
      }

      await client.query("BEGIN");

      try {
        await client.query(migration.sql);

        await client.query(
          `
            INSERT INTO schema_migrations (name, checksum)
            VALUES ($1, $2)
          `,
          [migration.name, migration.checksum],
        );

        await client.query("COMMIT");

        console.log(`Migrated ${service}: ${migration.name}`);
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    }
  } finally {
    try {
      await releaseMigrationLock(client);
    } finally {
      client.release();
      await pool.end();
    }
  }
}

const services = process.argv.slice(2);

Promise.all(
  (services.length ? services : Object.keys(migrations)).map(migrate),
).catch((err) => {
  console.error(err);
  process.exitCode = 1;
});



// import { config } from "dotenv";
// import { readdirSync, readFileSync } from "node:fs";
// import { resolve } from "node:path";
// import { Pool } from "pg";

// config({ path: resolve(process.cwd(), ".env") });

// const migrations: Record<string, { env: string; directory: string }> = {
//   auth: { env: "USER_DATABASE_URL", directory: "sql/auth" },
//   task: { env: "TASK_DATABASE_URL", directory: "sql/task" },
//   media: { env: "MEDIA_DATABASE_URL", directory: "sql/media" },
//   workflow: { env: "WORKFLOW_DATABASE_URL", directory: "sql/workflow" },
// };

// async function migrate(service: string) {
//   const target = migrations[service];
//   if (!target) throw new Error(`Unknown service '${service}'. Choose ${Object.keys(migrations).join(", ")}`);
//   const url = process.env[target.env];
//   if (!url) throw new Error(`${target.env} is not set`);
//   const pool = new Pool({ connectionString: url });
//   try {
//     for (const file of readdirSync(resolve(process.cwd(), target.directory)).filter((name) => name.endsWith(".sql")).sort()) {
//       await pool.query(readFileSync(resolve(process.cwd(), target.directory, file), "utf8"));
//       console.log(`Migrated ${service}: ${file}`);
//     }
//   } finally { await pool.end(); }
// }

// const services = process.argv.slice(2);
// Promise.all((services.length ? services : Object.keys(migrations)).map(migrate)).catch((err) => { console.error(err); process.exitCode = 1; });
