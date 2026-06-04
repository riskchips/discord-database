# discord-database

> A full-featured NoSQL database engine that uses Discord channels as storage — zero external DB required.

**Made by riskchips**

---

## What is this?

`discord-database` stores JSON records as messages inside Discord text channels. Each Discord channel is a "database collection." A Discord guild (server) is your storage backend. No PostgreSQL, no MongoDB, no Redis — just a bot token and a server.

Features at a glance:

- CRUD with auto-generated IDs
- In-memory caching with TTL (default 30s)
- Advanced query operators (`$gt`, `$lt`, `$in`, `$regex`, `$exists`, etc.)
- Full-text multi-field search with ranking
- Sort, paginate, aggregate, group, distinct
- Projection (`select` / `exclude`)
- Pipeline aggregation (`$match` → `$sort` → `$limit` → `$project` → `$count` → `$group`)
- Bulk operations (`insertMany`, `updateWhere`, `deleteWhere`)
- Atomic transactions with rollback
- TTL / auto-expiry (`purgeExpired`)
- Change polling (`watch`)
- Schema validation
- Backup and migration
- Chunked storage for large records (splits across multiple messages transparently)
- Rate-limit-aware Discord API client with per-bucket queuing

---

## Requirements

- Node.js 18+
- A Discord bot token with the following permissions in your guild:
  - `Manage Channels`
  - `Send Messages`
  - `Read Message History`
  - `Manage Messages` (for bulk delete)

---

## Setup

### 1. Install dependencies

```bash
npm install
```

### 2. Create `config.json` in the **parent directory** of your project files

```json
{
  "discord": {
    "bot": {
      "token": "YOUR_BOT_TOKEN",
      "id": "YOUR_BOT_CLIENT_ID"
    }
  },
  "database": {
    "guild_id": "YOUR_GUILD_ID"
  }
}
```

### 3. File structure

```
your-project/
├── config.json          ← bot credentials (parent dir)
├── database.json        ← auto-created, do not edit manually
└── src/
    ├── database.js
    └── discord.js
```

### 4. Invite your bot

Invite your bot to the guild with `bot` and `applications.commands` scopes and the permissions listed above.

---

## Quick Start

```js
const { db } = require('./database');

async function main() {
  // Create a database (Discord channel)
  await db.createDatabase('users');

  // Insert a record
  const result = await db.insert('users', { name: 'Alice', age: 28, role: 'admin' });
  const id = result.data.id;

  // Find by ID
  const user = await db.findById('users', id);
  console.log(user.data); // { id, name: 'Alice', age: 28, role: 'admin', _createdAt, _updatedAt }

  // Update
  await db.update('users', id, { age: 29 });

  // Delete
  await db.delete('users', id);
}

main();
```

---

## API Reference

All methods return `{ success: boolean, data: any, error?: string }`.

---

### Database Management

#### `db.createDatabase(name, options?)`

Creates a new database (Discord text channel). Returns early if it already exists.

```js
await db.createDatabase('users');

// With a custom Discord category
await db.createDatabase('orders', { categoryName: 'shop' });

// With explicit category ID
await db.createDatabase('logs', { categoryId: '123456789' });
```

| Option | Type | Default | Description |
|---|---|---|---|
| `categoryName` | string | — | Auto-creates/reuses a Discord category with this name |
| `categoryId` | string | — | Use an existing category snowflake ID |
| `channelName` | string | db name | Override the Discord channel name |
| `autoCategory` | boolean | `true` | Auto-group under a "databases" category |
| `defaultCategory` | string | `'databases'` | Name of the auto category |

---

#### `db.dropDatabase(name, options?)`

Deletes the database and its Discord channel.

```js
await db.dropDatabase('users');

// Keep the channel, just remove from registry
await db.dropDatabase('users', { deleteChannel: false });
```

---

#### `db.listDatabases()`

Returns all registered databases.

```js
const { data } = db.listDatabases();
// [{ name: 'users', channelId: '...', createdAt: '...' }, ...]
```

---

#### `db.getDatabase(name)`

Returns info for a single database.

```js
const { data } = db.getDatabase('users');
```

---

#### `db.renameDatabase(oldName, newName)`

Renames a database entry in the registry (does not rename the Discord channel).

---

#### `db.createCategory(name)` / `db.deleteCategory(name)`

Manually manage Discord categories.

---

#### `db.moveDatabaseToCategory(dbName, categoryName)`

Move an existing database channel into a different category.

```js
await db.moveDatabaseToCategory('orders', 'archive');
```

---

### Insert

#### `db.insert(name, data)`

Insert a single record. Auto-generates an `id` if not provided.

```js
const { data } = await db.insert('users', { name: 'Bob', age: 25 });
console.log(data.id); // e.g. "lf8k3zabc0"
```

---

#### `db.insertMany(name, records, options?)`

Bulk insert with concurrency control (default: 3 parallel).

```js
await db.insertMany('users', [
  { name: 'Carol', age: 30 },
  { name: 'Dave',  age: 22 },
]);

// Custom concurrency
await db.insertMany('users', records, { concurrency: 5 });
```

---

### Read

#### `db.findById(name, id, options?)`

```js
const { data } = await db.findById('users', 'lf8k3zabc0');
```

Pass `{ fresh: true }` to bypass the in-memory cache.

---

#### `db.findOne(name, query, options?)`

Returns the first record matching the query.

```js
const { data } = await db.findOne('users', { name: 'Alice' });
```

---

#### `db.find(name, query, options?)`

Returns all records matching the query.

```js
const { data } = await db.find('users', { role: 'admin' });
```

---

#### `db.findAll(name, options?)`

Returns all records.

```js
const { data, fromCache } = await db.findAll('users');
```

---

### Query Operators

Use MongoDB-style operators inside query objects:

```js
// Greater than
await db.find('users', { age: { $gt: 25 } });

// Less than or equal
await db.find('users', { age: { $lte: 30 } });

// Not equal
await db.find('users', { role: { $ne: 'guest' } });

// In array
await db.find('users', { role: { $in: ['admin', 'user'] } });

// Not in array
await db.find('users', { role: { $nin: ['guest'] } });

// Regex match
await db.find('users', { name: { $regex: '^A' } });

// Field exists
await db.find('users', { email: { $exists: true } });

// Field does not exist
await db.find('users', { email: { $exists: false } });
```

You can also pass a **function** as a query for custom logic:

```js
await db.find('users', (r) => r.age > 20 && r.name.startsWith('A'));
```

---

### Update

#### `db.update(name, id, updates)`

Merges `updates` into the existing record. Preserves `id` and `_createdAt`.

```js
await db.update('users', id, { age: 30, role: 'admin' });
```

---

#### `db.patch(name, id, fieldPath, value)`

Update a single nested field using dot notation.

```js
await db.patch('users', id, 'address.city', 'New York');
```

---

#### `db.upsert(name, id, data)`

Updates if the record exists, inserts if it doesn't.

```js
await db.upsert('users', 'known-id', { name: 'Eve', age: 27 });
```

---

#### `db.updateWhere(name, query, updates)`

Update all records matching a query.

```js
await db.updateWhere('users', { role: 'guest' }, { role: 'user' });
// { success: true, data: { updated: 3, failed: 0 } }
```

---

### Delete

#### `db.delete(name, id)`

Delete a single record by ID.

```js
await db.delete('users', id);
```

---

#### `db.deleteWhere(name, query)`

Delete all records matching a query.

```js
await db.deleteWhere('users', { role: 'guest' });
```

---

#### `db.deleteAll(name)`

Delete every record in the database (uses Discord bulk delete).

---

### Count / Exists

```js
const { data: count } = await db.count('users');
const { data: count } = await db.count('users', { role: 'admin' }); // with filter

const { data: exists } = await db.exists('users', id); // true / false
```

---

### Numeric Operations

```js
// Increment a field (default: +1)
await db.increment('users', id, 'score', 10);

// Decrement a field (default: -1)
await db.decrement('users', id, 'score', 5);
```

---

### Array Operations

```js
// Push a value onto an array field
await db.push('users', id, 'tags', 'javascript');

// Remove a value from an array field
await db.pull('users', id, 'tags', 'javascript');

// Add a value only if it's not already in the array
await db.addToSet('users', id, 'tags', 'nodejs');
```

---

### Sort & Paginate

#### `db.sort(name, field, direction?, query?)`

```js
// Sort by age descending
const { data } = await db.sort('users', 'age', 'desc');

// Sort only admins by name ascending
const { data } = await db.sort('users', 'name', 'asc', { role: 'admin' });
```

---

#### `db.paginate(name, page, pageSize, query?)`

```js
const { data } = await db.paginate('users', 1, 10);
// {
//   records: [...],
//   page: 1, pageSize: 10,
//   total: 45, totalPages: 5,
//   hasNext: true, hasPrev: false
// }
```

---

### Search

#### `db.search(name, field, keyword, options?)`

Case-insensitive partial match on a single field.

```js
const { data } = await db.search('users', 'name', 'ali');
// finds 'Alice', 'Alicia', etc.

// Case-sensitive and exact match
await db.search('users', 'name', 'Alice', { caseSensitive: true, exact: true });
```

---

#### `db.searchMultiField(name, fields, keyword, options?)`

Search across multiple fields. Enable `ranked` to sort results by how many fields matched.

```js
const { data } = await db.searchMultiField('users', ['name', 'bio'], 'alice');

// Ranked mode — most-matched fields first
await db.searchMultiField('users', ['name', 'bio', 'email'], 'alice', { ranked: true });
```

---

### Aggregation

#### `db.aggregate(name, field, query?)`

Compute stats on a numeric field.

```js
const { data } = await db.aggregate('users', 'age');
// { count: 10, sum: 270, avg: 27, min: 18, max: 45, median: 26, stddev: 8.2 }
```

---

#### `db.groupBy(name, field, query?)`

Group records by a field's value.

```js
const { data } = await db.groupBy('users', 'role');
// { admin: [...], user: [...], guest: [...] }
```

---

#### `db.distinct(name, field, query?)`

Get unique values of a field.

```js
const { data } = await db.distinct('users', 'role');
// ['admin', 'user', 'guest']
```

---

### Projection

#### `db.select(name, fields, query?)`

Return only the specified fields.

```js
const { data } = await db.select('users', ['name', 'age']);
// [{ name: 'Alice', age: 28 }, ...]
```

---

#### `db.exclude(name, fields, query?)`

Return all fields except the specified ones.

```js
const { data } = await db.exclude('users', ['_createdAt', '_updatedAt']);
```

---

### Pipeline Aggregation

Chain multiple stages in one call. Supported stages: `$match`, `$sort`, `$limit`, `$skip`, `$project`, `$count`, `$group`.

```js
const { data } = await db.pipeline('users', [
  { $match:   { role: 'admin' } },
  { $sort:    { field: 'age', direction: 'desc' } },
  { $limit:   5 },
  { $project: ['name', 'age'] },
]);

// Count stage
const { data } = await db.pipeline('users', [
  { $match: { role: 'user' } },
  { $count: null },
]);
// { count: 12 }

// Group stage
const { data } = await db.pipeline('users', [
  { $group: 'role' },
]);
// { admin: [...], user: [...] }
```

---

### Bulk Operations

#### `db.insertMany(name, records, options?)`

```js
await db.insertMany('users', [
  { name: 'Frank' },
  { name: 'Grace' },
], { concurrency: 3 });
// { success: true, data: { inserted: 2, failed: 0 } }
```

---

#### `db.updateWhere(name, query, updates)`

```js
await db.updateWhere('users', { active: false }, { role: 'archived' });
// { success: true, data: { updated: 7, failed: 0 } }
```

---

#### `db.deleteWhere(name, query)`

```js
await db.deleteWhere('users', { role: 'archived' });
// { success: true, data: { deleted: 7 } }
```

---

### Transactions

Execute multiple operations atomically. If any step fails, all previous steps are automatically rolled back.

```js
const { data } = await db.transaction('users', [
  { type: 'insert', data: { name: 'Heidi', age: 31 } },
  { type: 'update', id: existingId, data: { score: 100 } },
  { type: 'delete', id: anotherExistingId },
]);
// { completed: 3, results: [...] }
```

On failure: `{ success: false, error: '...', rolledBack: true, completed: [...] }`

---

### TTL — Auto-Expire Records

#### `db.purgeExpired(name, ttlField, maxAgeMs)`

Delete records where the value of `ttlField` is older than `maxAgeMs` milliseconds.

```js
// Insert a record with a timestamp
await db.insert('sessions', {
  userId: 'u1',
  createdAt: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(), // 2h ago
});

// Purge records older than 1 hour
const { data } = await db.purgeExpired('sessions', 'createdAt', 60 * 60 * 1000);
// { deleted: 1, total: 1 }
```

---

### Watch (Poll for Changes)

#### `db.watch(name, callback, intervalMs?)`

Polls the database at the given interval and fires the callback when records are added or removed. Returns a `stop()` function.

```js
const stop = db.watch('users', ({ added, removed }) => {
  if (added.length)   console.log('New records:', added);
  if (removed.length) console.log('Deleted IDs:', removed);
}, 5000); // poll every 5s

// Stop watching
stop();
```

---

### Schema Validation

#### `db.validate(name, id, schema)`

Validate a stored record against a schema definition.

```js
const schema = {
  name:  { required: true, type: 'string', minLength: 2 },
  age:   { required: true, type: 'number', min: 0, max: 120 },
  email: { type: 'string', match: '^[^@]+@[^@]+$' },
  role:  { enum: ['admin', 'user', 'guest'] },
};

const { data } = await db.validate('users', id, schema);
// { valid: true, errors: [] }
// or
// { valid: false, errors: ['Field "age" must be >= 0', ...] }
```

| Rule | Type | Description |
|---|---|---|
| `required` | boolean | Field must be present and non-null |
| `type` | string | `typeof` check: `'string'`, `'number'`, `'boolean'`, `'object'` |
| `min` / `max` | number | Numeric range (inclusive) |
| `minLength` / `maxLength` | number | String length range (inclusive) |
| `enum` | array | Value must be one of the listed options |
| `match` | string | Regex pattern the string must match |

---

### Cache Control

Records are cached in memory per-channel for 30 seconds after a read.

```js
// Invalidate cache for a specific database
db.invalidateCache('users');

// Clear all caches
db.clearAllCaches();

// Force a fresh read (bypass cache for one call)
await db.findAll('users', { fresh: true });
```

---

### Stats

#### `db.stats(name)`

```js
const { data } = await db.stats('users');
// {
//   name: 'users',
//   channelId: '...',
//   totalMessages: 52,
//   totalRecords: 50,
//   chunkedRecords: 2,
//   normalRecords: 48,
//   estimatedBytes: 12400,
//   cacheHit: true
// }
```

---

### Backup & Migration

#### `db.backup(sourceName, targetName)`

Copy all records from one database to another.

```js
await db.backup('users', 'users_backup');
```

---

#### `db.migrate(sourceName, targetName, transform?)`

Copy all records with an optional transformation function.

```js
await db.migrate('users_v1', 'users_v2', (record) => ({
  ...record,
  displayName: record.name.toUpperCase(),
}));
```

---

## Rate Limiting

The Discord API client (`discord.js`) handles rate limits automatically:

- Per-bucket queuing ensures no two requests hit the same endpoint simultaneously
- 429 responses are caught and retried after the reset window
- Global rate limits pause all requests until lifted

You can inspect the current state:

```js
const discord = require('./discord');
const status = discord.getRateLimitStatus();
// { global, globalResetAt, buckets: { ... } }
```

---

## Storage Format

Records are stored as JSON messages in Discord channels.

- **Small records** (< 1850 chars): single message
- **Large records**: split into numbered chunk messages with a metadata message that references them — fully transparent to you
- **Compression**: internal fields use short keys (`_c` = `_createdAt`, `_u` = `_updatedAt`, `_id` = `id`) to reduce storage by ~40%

---

## Limitations

| Constraint | Value |
|---|---|
| Max record size | No hard limit (chunked automatically) |
| Discord message limit | 2000 characters per message |
| Channels per guild | 500 (Discord limit) |
| Rate limit | ~50 requests/sec sustained (handled automatically) |
| Cache TTL | 30 seconds (default) |

Because Discord enforces rate limits, very high-frequency write workloads (thousands of inserts/sec) are not suitable. For read-heavy workloads with caching enabled, throughput is effectively unlimited.

---

## Error Handling

Every method returns `{ success: false, error: string }` on failure — no exceptions are thrown for expected errors.

```js
const result = await db.findById('users', 'nonexistent-id');
if (!result.success) {
  console.error(result.error); // "Record not found"
}
```

---

## License

MIT — use freely, credit appreciated.

---

*Made by riskchips*