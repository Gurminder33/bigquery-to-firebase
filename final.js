// syncBigQueryToFirestore.js

const admin      = require('firebase-admin');
const { BigQuery } = require('@google-cloud/bigquery');
const path       = require('path');

// ── 1) Load service account & init SDKs ────────────────────────────────
const serviceAccount = require(path.join(__dirname, 'bigquery-firestore-sync.json'));

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  projectId: serviceAccount.project_id
});

const db       = admin.firestore();
const bq       = new BigQuery({
  projectId: serviceAccount.project_id,
  credentials: {
    client_email: serviceAccount.client_email,
    private_key: serviceAccount.private_key
  }
});

// ── 2) Parameters ───────────────────────────────────────────────────────
const DATASET       = 'testdataset';
const TABLE         = 'nps_data_final';
const COLLECTION    = 'nps-data';
const CHUNK_SIZE    = 5000;   // number of docs per BulkWriter chunk
const DELETE_BATCH  = 500;    // Firestore batch-delete limit
const PROJECT_QUERY = `\`${serviceAccount.project_id}.${DATASET}.${TABLE}\``;

// ── 3) Main sync function ──────────────────────────────────────────────
async function syncData() {
  try {
    console.log(`🗑️  Clearing all documents from "${COLLECTION}"…`);
    // 3A) Delete existing docs in 500-doc batches
    const allDocs = await db.collection(COLLECTION).listDocuments();
    for (let i = 0; i < allDocs.length; i += DELETE_BATCH) {
      const batch = db.batch();
      allDocs.slice(i, i + DELETE_BATCH).forEach(docRef => batch.delete(docRef));
      await batch.commit();
      console.log(`  • Deleted batch ${Math.floor(i / DELETE_BATCH) + 1}`);
    }
    console.log(`✅ Cleared ${allDocs.length} documents.`);

    // 3B) Fetch rows from BigQuery
    console.log(`▶️  Querying BigQuery: SELECT * FROM ${PROJECT_QUERY}`);
    const [job]  = await bq.createQueryJob({ query: `SELECT * FROM ${PROJECT_QUERY}` });
    const [rows] = await job.getQueryResults();
    console.log(`📊 Retrieved ${rows.length} rows from BigQuery`);

    if (!rows.length) {
      console.log('ℹ️  No data to write.');
      return;
    }

    // 3C) Write in CHUNK_SIZE-doc chunks using BulkWriter
    console.log(`✍️  Writing to Firestore in chunks of ${CHUNK_SIZE}…`);
    for (let offset = 0; offset < rows.length; offset += CHUNK_SIZE) {
      const chunk = rows.slice(offset, offset + CHUNK_SIZE);
      const writer = db.bulkWriter();

      // Optional: exponential backoff on failures
      writer.onWriteError((err) => {
        console.error('BulkWriter error:', err);
        return true; // retry
      });

      chunk.forEach((row, idx) => {
        const docId = row.id != null ? String(row.id) : `doc-${offset + idx}`;
        writer.set(db.collection(COLLECTION).doc(docId), row);
      });

      await writer.close();
      console.log(`  • Chunk ${Math.floor(offset / CHUNK_SIZE) + 1} (${chunk.length} docs) written`);
    }

    console.log('✅ All data synced to Firestore successfully!');
  } catch (err) {
    console.error('❌ Sync failed:', err);
    process.exit(1);
  }
}

// ── 4) Run the sync ─────────────────────────────────────────────────────
syncData();
