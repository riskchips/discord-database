<div align="center">
  <img src="assets/logo.png" alt="discord-database" width="200" />
  <h1>discord-database</h1>
  <p>NoSQL database engine that runs entirely on Discord. No external DB, no hosting — just a bot token and a server.</p>
  <p><strong>Made by riskchips</strong></p>
</div>

---

## What is it?

Every database is a Discord text channel. Every record is a message. Your Discord guild is the storage backend. Large records get chunked across multiple messages transparently. Reads are cached in memory so you're not hammering the API constantly.

Works great for bots, small apps, dashboards, and anything where you want persistence without spinning up a database server.

---

## Requirements

- Node.js 18+
- A Discord bot with these permissions in your guild:
  - `Manage Channels`, `Send Messages`, `Read Message History`, `Manage Messages`

---

## Setup

```bash
npm install
```

Create `config.json` one directory above your project files:

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

File layout:
```
your-project/
├── config.json        ← credentials (never commit this)
├── database.json      ← auto-managed, don't touch
└── functions/
    ├── database.js
    └── discord.js
```

---

## Quick Start

```js
const { db } = require('./functions/database');

await db.createDatabase('users');

const { data } = await db.insert('users', { name: 'Alice', age: 28, role: 'admin' });
const id = data.id;

await db.update('users', id, { age: 29 });
await db.delete('users', id);
```

Every method returns `{ success, data, error? }` — no exceptions thrown for expected failures.

---

## API Reference

### Databases

| Method | Description |
|---|---|
| `db.createDatabase(name, opts?)` | Creates a Discord channel as a DB. Skips if already exists. Options: `categoryName`, `categoryId`, `channelName`, `autoCategory`, `defaultCategory` |
| `db.dropDatabase(name, opts?)` | Deletes the DB and its channel. Pass `{ deleteChannel: false }` to keep the channel |
| `db.listDatabases()` | Returns all registered databases |
| `db.getDatabase(name)` | Info for a single database |
| `db.renameDatabase(oldName, newName)` | Renames the registry entry (not the Discord channel) |
| `db.createCategory(name)` | Creates a Discord category |
| `db.deleteCategory(name)` | Deletes a Discord category |
| `db.listCategories()` | Lists all tracked categories |
| `db.moveDatabaseToCategory(dbName, categoryName)` | Moves a DB channel into a category |

### Insert

| Method | Description |
|---|---|
| `db.insert(name, data)` | Insert one record. Auto-generates `id` if not provided |
| `db.insertMany(name, records, opts?)` | Bulk insert. Default concurrency: 3. Returns `{ inserted, failed }` |
| `db.upsert(name, id, data)` | Updates if record exists, inserts if not |

### Read

| Method | Description |
|---|---|
| `db.findById(name, id, opts?)` | Find by ID. Pass `{ fresh: true }` to skip cache |
| `db.findOne(name, query, opts?)` | First record matching query |
| `db.find(name, query, opts?)` | All records matching query |
| `db.findAll(name, opts?)` | All records. Returns `fromCache: true` on cache hit |
| `db.count(name, query?)` | Count of records, optionally filtered |
| `db.exists(name, id)` | Returns `{ data: true/false }` |

### Query Operators

Pass operator objects inside queries:

```js
// comparison
{ age: { $gt: 25 } }
{ age: { $lt: 30 } }
{ age: { $gte: 18 } }
{ age: { $lte: 65 } }
{ role: { $ne: 'guest' } }

// array membership
{ role: { $in: ['admin', 'user'] } }
{ role: { $nin: ['banned'] } }

// pattern & existence
{ name: { $regex: '^A' } }
{ email: { $exists: true } }
{ phone: { $exists: false } }

// custom function
await db.find('users', (r) => r.age > 18 && r.active === true);
```

### Update

| Method | Description |
|---|---|
| `db.update(name, id, updates)` | Merge updates into record. Preserves `id` and `_createdAt` |
| `db.patch(name, id, fieldPath, value)` | Update a single nested field with dot notation e.g. `'address.city'` |
| `db.updateWhere(name, query, updates)` | Update all records matching a query. Returns `{ updated, failed }` |

### Delete

| Method | Description |
|---|---|
| `db.delete(name, id)` | Delete one record |
| `db.deleteWhere(name, query)` | Delete all matching records. Returns `{ deleted }` |
| `db.deleteAll(name)` | Wipe everything in the DB using Discord bulk delete |

### Numeric & Array Fields

| Method | Description |
|---|---|
| `db.increment(name, id, field, amount?)` | Add to a numeric field. Default amount: 1 |
| `db.decrement(name, id, field, amount?)` | Subtract from a numeric field. Default amount: 1 |
| `db.push(name, id, field, value)` | Append a value to an array field |
| `db.pull(name, id, field, value)` | Remove a value from an array field |
| `db.addToSet(name, id, field, value)` | Push only if value isn't already in the array |

### Sort, Paginate & Search

| Method | Description |
|---|---|
| `db.sort(name, field, direction?, query?)` | Sort records by field. Direction: `'asc'` or `'desc'`. Records without the field are excluded |
| `db.paginate(name, page, pageSize, query?)` | Paginate records. Returns `{ records, page, pageSize, total, totalPages, hasNext, hasPrev }` |
| `db.search(name, field, keyword, opts?)` | Case-insensitive partial match on one field. Options: `caseSensitive`, `exact` |
| `db.searchMultiField(name, fields, keyword, opts?)` | Search across multiple fields. Use `{ ranked: true }` to sort by match count |

### Aggregation

| Method | Description |
|---|---|
| `db.aggregate(name, field, query?)` | Stats on a numeric field: `count`, `sum`, `avg`, `min`, `max`, `median`, `stddev` |
| `db.groupBy(name, field, query?)` | Group records by a field's value. Returns `{ value: [...records] }` |
| `db.distinct(name, field, query?)` | Unique values of a field |

### Projection

| Method | Description |
|---|---|
| `db.select(name, fields, query?)` | Return only specified fields. Records missing all requested fields are excluded |
| `db.exclude(name, fields, query?)` | Return all fields except specified ones |

### Pipeline Aggregation

Chain stages in a single call:

```js
await db.pipeline('users', [
  { $match:   { role: 'user' } },
  { $sort:    { score: 'desc' } },
  { $limit:   10 },
  { $skip:    0 },
  { $project: ['name', 'score'] },
  { $count:   true },   // returns { count: N }
  { $group:   'role' }, // returns { role: [...] }
]);
```

### Transactions

Operations run in order. If any fail, all previous ones are rolled back automatically.

```js
const result = await db.transaction('users', [
  { type: 'insert', data: { name: 'Bob' } },
  { type: 'update', id: someId, data: { score: 100 } },
  { type: 'delete', id: oldId },
]);
// on failure: { success: false, rolledBack: true, error, completed }
```

### TTL & Watch

| Method | Description |
|---|---|
| `db.purgeExpired(name, ttlField, maxAgeMs)` | Delete records where `record[ttlField]` is older than `maxAgeMs` ms |
| `db.watch(name, callback, intervalMs?)` | Poll for changes every N ms. Callback receives `{ added, removed }`. Returns a `stop()` function |

### Schema Validation

```js
const schema = {
  name: { required: true, type: 'string', minLength: 2, maxLength: 50 },
  age:  { required: true, type: 'number', min: 0, max: 150 },
  role: { enum: ['admin', 'user', 'guest'] },
  email: { match: '^[^@]+@[^@]+$' },
};

const { data } = await db.validate('users', id, schema);
// { valid: true, errors: [] }
// { valid: false, errors: ['Field "age" must be >= 0', ...] }
```

| Rule | Type | What it checks |
|---|---|---|
| `required` | boolean | Field must exist and be non-null |
| `type` | string | `typeof` — `'string'`, `'number'`, `'boolean'`, `'object'` |
| `min` / `max` | number | Numeric range (inclusive) |
| `minLength` / `maxLength` | number | String length (inclusive) |
| `enum` | array | Value must be one of the listed options |
| `match` | string | Regex pattern the string must pass |

### Cache & Stats

| Method | Description |
|---|---|
| `db.invalidateCache(name)` | Clear cached records for one DB |
| `db.clearAllCaches()` | Clear all in-memory caches |
| `db.stats(name)` | Returns `{ totalRecords, estimatedBytes, chunkedRecords, cacheHit, ... }` |

Cache TTL is 30 seconds by default. Pass `{ fresh: true }` to any read method to bypass it for that call.

### Backup & Migration

```js
// copy all records from one DB to another
await db.backup('users', 'users_backup');

// copy with a transform applied to each record
await db.migrate('users_v1', 'users_v2', (r) => ({ ...r, displayName: r.name.toUpperCase() }));
```

---

## Storage Details

- Records under 1850 chars → single Discord message
- Larger records → automatically chunked across multiple messages + a metadata message to stitch them back
- Field names are compressed on-wire (`_createdAt` → `_c`, etc.) — saves ~40% space
- All DB metadata lives in `database.json`, never inside Discord

---

## Limitations

| Thing | Limit |
|---|---|
| Discord message size | 2000 chars (chunked automatically) |
| Channels per guild | 500 (Discord hard limit) |
| Sustained write rate | ~50 req/s (rate limiting handled automatically) |
| Cache TTL | 30 seconds |

High-frequency writes (thousands/sec) aren't a good fit. Read-heavy workloads with caching are fine.

---

## License

MIT

*Made by riskchips*