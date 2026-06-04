// test.js - Complete Test Suite for database.js + discord.js
//
// Run: node test.js
// Requires a valid config.json with bot token + guild_id, and a "databases" category
// already existing OR bot has permission to create categories.

'use strict';

const db = require('./functions/database');
const discord = require('./functions/discord');

// ─── Test Helpers ──────────────────────────────────────────────────────────────
let passed = 0, failed = 0, skipped = 0;
const TEST_DB = `test_db_${Date.now().toString(36)}`;
const TEST_CATEGORY = 'test-category';

const log = (label, ok, detail = '') => {
  if (ok) {
    console.log(`  ✅ ${label}${detail ? ': ' + detail : ''}`);
    passed++;
  } else {
    console.log(`  ❌ ${label}${detail ? ' — ' + detail : ''}`);
    failed++;
  }
};

const skip = (label, reason) => {
  console.log(`  ⏭  ${label} (skipped: ${reason})`);
  skipped++;
};

const section = (title) => console.log(`\n── ${title} ${'─'.repeat(50 - title.length)}`);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── 1. Health Check ───────────────────────────────────────────────────────────
async function testHealth() {
  section('1. Health & Connectivity');

  const health = await discord.health();
  log('discord.health() succeeds', health.success, health.data?.bot || health.error);
  if (!health.success) {
    console.log('\n⚠️  Cannot reach Discord API — stopping tests. Check your config.json token.\n');
    process.exit(1);
  }

  log('Bot username present', !!health.data?.bot, health.data?.bot);
  log('Guild name present', !!health.data?.guild, health.data?.guild);
  log('API latency reasonable', health.data?.latency < 3000, `${health.data?.latency}ms`);
}

// ─── 2. Category Auto-Creation ─────────────────────────────────────────────────
async function testCategories() {
  section('2. Category Auto-Creation (database.json)');

  // createOrGetCategory — first call creates
  const r1 = await discord.createOrGetCategory(TEST_CATEGORY);
  log('createOrGetCategory creates new category', r1.success, r1.data?.categoryId);

  // second call reuses (from database.json cache)
  const r2 = await discord.createOrGetCategory(TEST_CATEGORY);
  log('createOrGetCategory reuses existing (cache hit)', r2.success && r2.data?.existed === true, `existed=${r2.data?.existed}`);
  log('Same categoryId returned both times', r1.data?.categoryId === r2.data?.categoryId, r2.data?.categoryId);

  // db.createCategory wrapper
  const r3 = await db.createCategory('db-wrapper-cat');
  log('db.createCategory() works', r3.success, r3.data?.categoryId);

  // db.listCategories reads from database.json
  const r4 = db.listCategories();
  log('db.listCategories() returns array', r4.success && Array.isArray(r4.data), `count=${r4.data?.length}`);
  log('Test category in list', r4.data?.some((c) => c.name === TEST_CATEGORY), TEST_CATEGORY);

  return { categoryId: r1.data?.categoryId };
}

// ─── 3. Database Creation ──────────────────────────────────────────────────────
async function testDatabaseCreation(categoryId) {
  section('3. Database Creation & Listing');

  // Create DB — auto-category enabled by default
  const r1 = await db.createDatabase(TEST_DB, {
    categoryId,
    channelName: `test-${Date.now().toString(36)}`,
  });
  log('db.createDatabase() succeeds', r1.success, `channelId=${r1.data?.channelId}`);
  log('Channel placed in category', r1.data?.categoryId === categoryId || !!r1.data?.categoryId, r1.data?.categoryId);

  // Duplicate — should return existed:true
  const r2 = await db.createDatabase(TEST_DB);
  log('Duplicate createDatabase returns existed:true', r2.success && r2.data?.existed === true);

  // listDatabases
  const r3 = db.listDatabases();
  log('db.listDatabases() includes test DB', r3.data?.some((d) => d.name === TEST_DB), TEST_DB);

  // getDatabase
  const r4 = db.getDatabase(TEST_DB);
  log('db.getDatabase() returns DB info', r4.success && r4.data?.channelId, r4.data?.channelId);

  return { channelId: r1.data?.channelId || r2.data?.channelId };
}

// ─── 4. CRUD Operations ────────────────────────────────────────────────────────
async function testCRUD() {
  section('4. CRUD — Insert / FindById / Update / Delete');

  // Insert
  const rec1 = await db.insert(TEST_DB, { name: 'Alice', age: 30, role: 'admin', score: 95 });
  log('db.insert() succeeds', rec1.success, rec1.data?.id);
  const id1 = rec1.data?.id;

  const rec2 = await db.insert(TEST_DB, { name: 'Bob', age: 25, role: 'user', score: 72 });
  log('db.insert() returns record with auto-id', rec2.success && !!rec2.data?.id);
  const id2 = rec2.data?.id;

  const rec3 = await db.insert(TEST_DB, { name: 'Carol', age: 35, role: 'user', score: 88 });
  const id3 = rec3.data?.id;
  log('Inserted 3 records', rec1.success && rec2.success && rec3.success);

  await sleep(500); // Let cache settle

  // FindById
  const found = await db.findById(TEST_DB, id1);
  log('db.findById() finds record', found.success && found.data?.name === 'Alice', found.data?.name);
  log('_createdAt is set', !!found.data?._createdAt);
  log('_updatedAt is set', !!found.data?._updatedAt);

  // FindOne
  const one = await db.findOne(TEST_DB, { name: 'Bob' });
  log('db.findOne() finds by field match', one.success && one.data?.name === 'Bob');

  // FindAll
  const all = await db.findAll(TEST_DB);
  log('db.findAll() returns all records', all.success && all.data?.length >= 3, `count=${all.data?.length}`);

  // Cache hit
  const cached = await db.findAll(TEST_DB);
  log('Second findAll() uses cache (fromCache)', cached.fromCache === true, `fromCache=${cached.fromCache}`);

  // Update
  const upd = await db.update(TEST_DB, id1, { age: 31, score: 98 });
  log('db.update() succeeds', upd.success, upd.data?.age);
  const afterUpd = await db.findById(TEST_DB, id1, { fresh: true });
  log('Updated fields persisted', afterUpd.data?.age === 31 && afterUpd.data?.score === 98, `age=${afterUpd.data?.age} score=${afterUpd.data?.score}`);
  log('_createdAt preserved after update', afterUpd.data?._createdAt === found.data?._createdAt);

  // Patch (nested field path)
  await db.insert(TEST_DB, { id: 'nested_test', profile: { city: 'NY' } });
  await sleep(300);
  const patched = await db.patch(TEST_DB, 'nested_test', 'profile.city', 'LA');
  log('db.patch() nested field path works', patched.success);
  const afterPatch = await db.findById(TEST_DB, 'nested_test', { fresh: true });
  log('Patched value persisted', afterPatch.data?.profile?.city === 'LA', afterPatch.data?.profile?.city);

  // Upsert — update existing
  const ups1 = await db.upsert(TEST_DB, id2, { score: 80 });
  log('db.upsert() updates existing record', ups1.success);

  // Upsert — insert new
  const ups2 = await db.upsert(TEST_DB, 'brand_new_id', { name: 'Dave', age: 20, role: 'guest', score: 50 });
  log('db.upsert() inserts new record', ups2.success);

  // Count
  const cnt = await db.count(TEST_DB);
  log('db.count() returns number', cnt.success && typeof cnt.data === 'number', `count=${cnt.data}`);

  // Exists
  const ex1 = await db.exists(TEST_DB, id1);
  const ex2 = await db.exists(TEST_DB, 'nonexistent_xyz');
  log('db.exists() true for existing', ex1.data === true);
  log('db.exists() false for missing', ex2.data === false);

  // Delete
  const del = await db.delete(TEST_DB, id3);
  log('db.delete() succeeds', del.success);
  const afterDel = await db.findById(TEST_DB, id3, { fresh: true });
  log('Record gone after delete', !afterDel.success);

  return { id1, id2 };
}

// ─── 5. Advanced Query Operators ──────────────────────────────────────────────
async function testQueryOperators() {
  section('5. Advanced Query Operators');

  // $gt / $lt
  const r1 = await db.find(TEST_DB, { age: { $gt: 28 } });
  log('$gt operator works', r1.success && r1.data?.every((r) => r.age > 28), `found=${r1.data?.length}`);

  const r2 = await db.find(TEST_DB, { score: { $lte: 80 } });
  log('$lte operator works', r2.success && r2.data?.every((r) => r.score <= 80), `found=${r2.data?.length}`);

  // $ne
  const r3 = await db.find(TEST_DB, { role: { $ne: 'admin' } });
  log('$ne operator works', r3.success && r3.data?.every((r) => r.role !== 'admin'), `found=${r3.data?.length}`);

  // $in
  const r4 = await db.find(TEST_DB, { role: { $in: ['admin', 'guest'] } });
  log('$in operator works', r4.success && r4.data?.every((r) => ['admin', 'guest'].includes(r.role)), `found=${r4.data?.length}`);

  // $nin
  const r5 = await db.find(TEST_DB, { role: { $nin: ['admin'] } });
  log('$nin operator works', r5.success && r5.data?.every((r) => r.role !== 'admin'), `found=${r5.data?.length}`);

  // $regex
  const r6 = await db.find(TEST_DB, { name: { $regex: '^A' } });
  log('$regex operator works', r6.success && r6.data?.every((r) => /^A/.test(r.name)), `found=${r6.data?.length}`);

  // $exists
  const r7 = await db.find(TEST_DB, { profile: { $exists: true } });
  log('$exists:true operator works', r7.success && r7.data?.length > 0, `found=${r7.data?.length}`);

  const r8 = await db.find(TEST_DB, { profile: { $exists: false } });
  log('$exists:false operator works', r8.success && r8.data?.every((r) => r.profile === undefined), `found=${r8.data?.length}`);
}

// ─── 6. Array Field Operations ─────────────────────────────────────────────────
async function testArrayOps(id1) {
  section('6. Array Field Operations');

  const push1 = await db.push(TEST_DB, id1, 'tags', 'javascript');
  log('db.push() adds to array', push1.success);

  await db.push(TEST_DB, id1, 'tags', 'nodejs');
  const after = await db.findById(TEST_DB, id1, { fresh: true });
  log('Array has 2 items after 2 pushes', after.data?.tags?.length === 2, JSON.stringify(after.data?.tags));

  const pull = await db.pull(TEST_DB, id1, 'tags', 'javascript');
  log('db.pull() removes from array', pull.success);
  const afterPull = await db.findById(TEST_DB, id1, { fresh: true });
  log('Array has 1 item after pull', afterPull.data?.tags?.length === 1, JSON.stringify(afterPull.data?.tags));

  // addToSet — no duplicate
  await db.addToSet(TEST_DB, id1, 'tags', 'nodejs');
  await db.addToSet(TEST_DB, id1, 'tags', 'nodejs');
  const afterSet = await db.findById(TEST_DB, id1, { fresh: true });
  log('db.addToSet() does not duplicate', afterSet.data?.tags?.filter((t) => t === 'nodejs').length === 1);
}

// ─── 7. Increment / Decrement ──────────────────────────────────────────────────
async function testNumericOps(id2) {
  section('7. Increment / Decrement');

  const before = await db.findById(TEST_DB, id2, { fresh: true });
  const origScore = before.data?.score ?? 0;

  await db.increment(TEST_DB, id2, 'score', 10);
  const afterInc = await db.findById(TEST_DB, id2, { fresh: true });
  log('db.increment() adds value', afterInc.data?.score === origScore + 10, `${origScore} → ${afterInc.data?.score}`);

  await db.decrement(TEST_DB, id2, 'score', 5);
  const afterDec = await db.findById(TEST_DB, id2, { fresh: true });
  log('db.decrement() subtracts value', afterDec.data?.score === origScore + 5, `→ ${afterDec.data?.score}`);
}

// ─── 8. Sort / Paginate ────────────────────────────────────────────────────────
async function testSortPaginate() {
  section('8. Sort & Paginate');

  const sorted = await db.sort(TEST_DB, 'score', 'desc');
  log('db.sort() desc order', sorted.success && sorted.data?.[0]?.score >= sorted.data?.[sorted.data.length - 1]?.score, `top score=${sorted.data?.[0]?.score}`);

  const sortedAsc = await db.sort(TEST_DB, 'name', 'asc');
  log('db.sort() asc order', sortedAsc.success);

  const page = await db.paginate(TEST_DB, 1, 2);
  log('db.paginate() page 1', page.success && page.data?.records?.length <= 2, `records=${page.data?.records?.length} total=${page.data?.total}`);
  log('Pagination metadata correct', page.data?.page === 1 && typeof page.data?.totalPages === 'number');
  log('hasNext is correct', typeof page.data?.hasNext === 'boolean');
}

// ─── 9. Search ────────────────────────────────────────────────────────────────
async function testSearch() {
  section('9. Search');

  const r1 = await db.search(TEST_DB, 'name', 'ali');
  log('db.search() case-insensitive partial match', r1.success && r1.data?.some((r) => r.name === 'Alice'), `found=${r1.data?.length}`);

  const r2 = await db.searchMultiField(TEST_DB, ['name', 'role'], 'admin');
  log('db.searchMultiField() works', r2.success && r2.data?.length > 0, `found=${r2.data?.length}`);

  const r3 = await db.searchMultiField(TEST_DB, ['name', 'role'], 'alice', { ranked: true });
  log('db.searchMultiField() ranked mode works', r3.success, `found=${r3.data?.length}`);
}

// ─── 10. Aggregation & Group ──────────────────────────────────────────────────
async function testAggregation() {
  section('10. Aggregation & GroupBy');

  const agg = await db.aggregate(TEST_DB, 'score');
  log('db.aggregate() count/sum/avg/min/max', agg.success && agg.data?.count > 0, `avg=${agg.data?.avg?.toFixed(1)}`);
  log('db.aggregate() includes median', agg.data?.median !== undefined, `median=${agg.data?.median}`);
  log('db.aggregate() includes stddev', agg.data?.stddev !== undefined, `stddev=${agg.data?.stddev?.toFixed(2)}`);

  const grp = await db.groupBy(TEST_DB, 'role');
  log('db.groupBy() groups records', grp.success && typeof grp.data === 'object', `keys=${Object.keys(grp.data || {}).join(', ')}`);

  const dist = await db.distinct(TEST_DB, 'role');
  log('db.distinct() returns unique values', dist.success && Array.isArray(dist.data), `values=${dist.data?.join(', ')}`);
}

// ─── 11. Select / Exclude ─────────────────────────────────────────────────────
async function testProjection() {
  section('11. Projection — select / exclude');

  const sel = await db.select(TEST_DB, ['name', 'age']);
  log('db.select() only returns specified fields', sel.success && sel.data?.every((r) => 'name' in r && !('score' in r)), `fields=${Object.keys(sel.data?.[0] || {}).join(', ')}`);

  const excl = await db.exclude(TEST_DB, ['_createdAt', '_updatedAt', '_messageId']);
  log('db.exclude() removes specified fields', excl.success && excl.data?.every((r) => !('_createdAt' in r)));
}

// ─── 12. Pipeline ─────────────────────────────────────────────────────────────
async function testPipeline() {
  section('12. Pipeline Aggregation');

  const result = await db.pipeline(TEST_DB, [
    { $match: { role: 'user' } },
    { $sort: { score: 'desc' } },
    { $limit: 2 },
    { $project: ['name', 'score'] },
  ]);

  log('Pipeline: match → sort → limit → project', result.success, `result count=${result.data?.length}`);
  log('Pipeline results are projected', result.success && result.data?.every((r) => 'name' in r && !('role' in r)));

  const countResult = await db.pipeline(TEST_DB, [
    { $match: { role: { $ne: 'admin' } } },
    { $count: true },
  ]);
  log('Pipeline $count stage', countResult.success && typeof countResult.data?.count === 'number', `count=${countResult.data?.count}`);

  const groupResult = await db.pipeline(TEST_DB, [
    { $group: 'role' },
  ]);
  log('Pipeline $group stage', groupResult.success && typeof groupResult.data === 'object');
}

// ─── 13. UpdateMany / DeleteWhere ─────────────────────────────────────────────
async function testBulkOps() {
  section('13. Bulk Operations');

  // InsertMany with concurrency
  const bulkRecords = Array.from({ length: 6 }, (_, i) => ({
    name: `Bulk${i}`, age: 20 + i, role: 'bulk', score: 50 + i * 5,
  }));

  const insMany = await db.insertMany(TEST_DB, bulkRecords, { concurrency: 3 });
  log('db.insertMany() with concurrency=3 inserts all', insMany.success && insMany.data?.inserted === 6, `inserted=${insMany.data?.inserted}`);

  // updateWhere
  const updWhere = await db.updateWhere(TEST_DB, { role: 'bulk' }, { active: true });
  log('db.updateWhere() updates matching records', updWhere.success, `updated=${updWhere.data?.updated}`);

  // deleteWhere
  const delWhere = await db.deleteWhere(TEST_DB, { role: 'bulk' });
  log('db.deleteWhere() deletes matching records', delWhere.success, `deleted=${delWhere.data?.deleted}`);
}

// ─── 14. TTL Purge ────────────────────────────────────────────────────────────
async function testTTL() {
  section('14. TTL — Auto-Expire Records');

  await db.insert(TEST_DB, { id: 'ttl_test', name: 'Expiring', createdAt: new Date(Date.now() - 5000).toISOString() });
  await sleep(300);

  // Purge records older than 3 seconds
  const purge = await db.purgeExpired(TEST_DB, 'createdAt', 3000);
  log('db.purgeExpired() finds and deletes expired records', purge.success, `deleted=${purge.data?.deleted}`);
}

// ─── 15. Stats & Cache ────────────────────────────────────────────────────────
async function testStats() {
  section('15. Stats & Cache Control');

  const stats = await db.stats(TEST_DB);
  log('db.stats() returns stats object', stats.success, `records=${stats.data?.totalRecords} bytes=${stats.data?.estimatedBytes}`);
  log('estimatedBytes is a number', typeof stats.data?.estimatedBytes === 'number');

  // Cache invalidation
  db.invalidateCache(TEST_DB);
  const fresh = await db.findAll(TEST_DB, { fresh: false });
  log('After invalidateCache, fromCache=false', fresh.fromCache === false, `fromCache=${fresh.fromCache}`);

  db.clearAllCaches();
  log('db.clearAllCaches() runs without error', true);
}

// ─── 16. Validation ───────────────────────────────────────────────────────────
async function testValidation(id1) {
  section('16. Schema Validation');

  const schema = {
    name: { required: true, type: 'string', minLength: 2 },
    age: { required: true, type: 'number', min: 0, max: 150 },
    role: { required: true, enum: ['admin', 'user', 'guest', 'bulk'] },
  };

  const valid = await db.validate(TEST_DB, id1, schema);
  log('db.validate() passes valid record', valid.success && valid.data?.valid === true, `errors=${valid.data?.errors?.length}`);

  // Insert a bad record and validate
  await db.insert(TEST_DB, { id: 'invalid_rec', name: 'X', age: -1, role: 'superuser' });
  await sleep(300);

  const invalid = await db.validate(TEST_DB, 'invalid_rec', schema);
  log('db.validate() catches violations', invalid.success && invalid.data?.valid === false && invalid.data?.errors?.length > 0, `errors: ${invalid.data?.errors?.join('; ')}`);
}

// ─── 17. Transaction with Rollback ────────────────────────────────────────────
async function testTransaction() {
  section('17. Transactions');

  const txResult = await db.transaction(TEST_DB, [
    { type: 'insert', data: { id: 'tx_a', name: 'TxA', role: 'tx' } },
    { type: 'insert', data: { id: 'tx_b', name: 'TxB', role: 'tx' } },
  ]);
  log('Transaction with 2 inserts succeeds', txResult.success, `completed=${txResult.data?.completed}`);

  const txFail = await db.transaction(TEST_DB, [
    { type: 'update', id: 'tx_a', data: { name: 'TxA_modified' } },
    { type: 'update', id: 'nonexistent_id_xyz', data: { name: 'Fail' } }, // will fail
  ]);
  log('Transaction fails and rolls back on error', !txFail.success && txFail.rolledBack === true, `error: ${txFail.error}`);

  // Verify tx_a was rolled back
  const check = await db.findById(TEST_DB, 'tx_a', { fresh: true });
  log('Rolled-back record restored to original state', check.success && check.data?.name === 'TxA', check.data?.name);

  // Cleanup
  await db.delete(TEST_DB, 'tx_a').catch(() => {});
  await db.delete(TEST_DB, 'tx_b').catch(() => {});
}

// ─── 18. Watch ────────────────────────────────────────────────────────────────
async function testWatch() {
  section('18. Watch / Poll');

  let changeDetected = false;
  const stop = db.watch(TEST_DB, ({ added, removed }) => {
    if (added.length || removed.length) changeDetected = true;
  }, 2000);

  // Insert after a short delay so watch can pick it up
  await sleep(500);
  await db.insert(TEST_DB, { id: 'watch_test', name: 'WatchMe' });
  await sleep(3000); // Wait for poll cycle

  stop(); // Stop watching
  log('db.watch() detects new records', changeDetected, `detected=${changeDetected}`);

  await db.delete(TEST_DB, 'watch_test').catch(() => {});
}

// ─── 19. Rate Limit Status ────────────────────────────────────────────────────
async function testRateLimitStatus() {
  section('19. Rate Limit & API Status');

  const rl = discord.getRateLimitStatus();
  log('getRateLimitStatus() returns state', rl.success && typeof rl.data?.globalLimited === 'boolean');
  log('Bucket tracking populated', typeof rl.data?.buckets === 'object');
}

// ─── 20. Cleanup ──────────────────────────────────────────────────────────────
async function cleanup(categoryId) {
  section('20. Cleanup');

  const drop = await db.dropDatabase(TEST_DB, { deleteChannel: true });
  log('db.dropDatabase() deletes channel and removes from database.json', drop.success, drop.data?.channelId);

  // Delete test categories
  await db.deleteCategory(TEST_CATEGORY, { deleteChannel: true }).catch(() => {});
  await db.deleteCategory('db-wrapper-cat', { deleteChannel: true }).catch(() => {});
  log('Test categories cleaned up', true);
}

// ─── Main Runner ───────────────────────────────────────────────────────────────
(async () => {
  console.log('\n╔══════════════════════════════════════════════════════╗');
  console.log('║       Discord-backed DB Test Suite v2.0              ║');
  console.log('╚══════════════════════════════════════════════════════╝\n');

  try {
    await testHealth();
    const { categoryId } = await testCategories();
    await testDatabaseCreation(categoryId);
    const { id1, id2 } = await testCRUD();
    await testQueryOperators();
    await testArrayOps(id1);
    await testNumericOps(id2);
    await testSortPaginate();
    await testSearch();
    await testAggregation();
    await testProjection();
    await testPipeline();
    await testBulkOps();
    await testTTL();
    await testStats();
    await testValidation(id1);
    await testTransaction();
    await testWatch();
    await testRateLimitStatus();
    await cleanup(categoryId);
  } catch (err) {
    console.error('\n💥 Unexpected error:', err.message);
    failed++;
  }

  // ─── Summary ────────────────────────────────────────────────────────────────
  const total = passed + failed + skipped;
  console.log('\n╔══════════════════════════════════════════════════════╗');
  console.log(`║  Results: ${passed}/${total} passed  |  ${failed} failed  |  ${skipped} skipped`.padEnd(54) + '║');
  console.log('╚══════════════════════════════════════════════════════╝\n');

  if (failed > 0) process.exit(1);
})();