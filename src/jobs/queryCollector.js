const cron = require("node-cron");
const prisma = require("../config/db");
const { decrypt } = require("../utils/encryption");
const { Client } = require("pg");

async function collectLogs() {
  console.log(
    `[QueryCollector] collectLogs started at ${new Date().toISOString()}`
  );
  const dbs = await prisma.userDB.findMany({
    where: { monitoringEnabled: true },
  });
  console.log(`[QueryCollector] ${dbs.length} DB(s) with monitoring enabled`);

  for (const db of dbs) {
    console.log(
      `[QueryCollector] Processing DB id=${db.id} name=${db.dbName} host=${db.host} port=${db.port}`
    );
    const password = decrypt(db.passwordEncrypted);
    const client = new Client({
      host: db.host,
      port: db.port,
      database: db.dbName,
      user: db.username,
      password,
    });

    try {
      await client.connect();
      console.log(`[QueryCollector] Connected to ${db.dbName}`);
      const { rows } = await client.query(`
        SELECT query, calls, total_exec_time, mean_exec_time, rows
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT 50;
      `);
      console.log(
        `[QueryCollector] Retrieved ${rows.length} rows from pg_stat_statements for db=${db.dbName}`
      );

      let successfulUpserts = 0;
      for (const row of rows) {
        try {
          await prisma.queryLog.upsert({
            where: {
              userDbId_query: {
                userDbId: db.id,
                query: row.query || "",
              },
            },
            update: {
              calls: parseInt(row.calls) || 0,
              totalTimeMs: parseFloat(row.total_exec_time) || 0,
              meanTimeMs: parseFloat(row.mean_exec_time) || 0,
              rowsReturned: parseInt(row.rows) || 0,
              collectedAt: new Date(), // Update timestamp on update
            },
            create: {
              userDbId: db.id,
              query: row.query || "",
              calls: parseInt(row.calls) || 0,
              totalTimeMs: parseFloat(row.total_exec_time) || 0,
              meanTimeMs: parseFloat(row.mean_exec_time) || 0,
              rowsReturned: parseInt(row.rows) || 0,
            },
          });
          successfulUpserts += 1;
        } catch (logError) {
          console.error(
            `Failed to create log entry for query: ${row.query}`,
            logError.message
          );
        }
      }
      console.log(
        `[QueryCollector] Upserted ${successfulUpserts}/${rows.length} rows for db=${db.dbName}`
      );
    } catch (err) {
      console.error(`Failed to collect logs for DB ${db.dbName}:`, err.message);
    } finally {
      await client.end();
      console.log(`[QueryCollector] Disconnected from ${db.dbName}`);
    }
  }
  console.log(
    `[QueryCollector] collectLogs finished at ${new Date().toISOString()}`
  );
}

function startCron() {
  console.log(
    '[QueryCollector] Scheduling cron job to run every minute ("* * * * *")'
  );
  cron.schedule("* * * * *", () => {
    console.log(`[QueryCollector] Cron tick at ${new Date().toISOString()}`);
    collectLogs();
  });

  // Trigger one immediate run on startup for visibility while debugging
  setTimeout(() => {
    console.log("[QueryCollector] Initial run on startup");
    collectLogs();
  }, 1000);
}

module.exports = { startCron, collectLogs };
