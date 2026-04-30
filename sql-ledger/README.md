# sql-ledger


## Overview

The `sql-ledger` module provides a SQL-backed implementation of `SourceLedger`. A ledger
is defined by exactly two parameterless/parameterized SQL `SELECT` statements:

| Query | Purpose |
|-------|---------|
| **size query** | Parameterless; returns a single row with a single `LONG` value — the current row count |
| **row query** | Takes one `LONG` parameter (the 1-based row number); returns the column values for that row |

`SqlLedger` reads one row at a time from the database, maps each column to a `DataType`,
and feeds the resulting typed cells into the skipledger commitment chain. The database
is never written to; the connection is opened read-only.

---

## SQL Type Mapping

Each column in the row query result is mapped to a `DataType` as follows:

| SQL type(s) | `DataType` | Java value | Notes |
|-------------|------------|------------|-------|
| `BOOLEAN` | `BOOL` | `Boolean` | |
| `BIT` | `BOOL` | `Boolean` | MySQL/MariaDB alias for `BOOLEAN` |
| `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | `LONG` | `Long` | |
| `DECIMAL`, `NUMERIC` | `BIG_DEC` | `BigDecimal` | |
| `DATE`, `TIME`, `TIME WITH TIMEZONE`, `TIMESTAMP`, `TIMESTAMP WITH TIMEZONE` | `DATE` | UTC milliseconds as `Long` |  |
| `CHAR`, `VARCHAR`, `LONGVARCHAR`, `NCHAR`, `NVARCHAR`, `LONGNVARCHAR`, `CLOB`, `NCLOB` | `STRING` | `String` | UTF-8 |
| `BLOB` | `BYTES` | `byte[]` | Size-guarded; see below |
| `BINARY`, `VARBINARY`, `LONGVARBINARY` | `BYTES` | `byte[]` | Same size guard as `BLOB` |
| `NULL` result | `NULL` | `null` | |

### No floating-point

`FLOAT`, `DOUBLE`, and `REAL` columns are explicitly **rejected** with a
`SqlLedgerException`. IEEE-754 has multiple bit representations of the same logical
value (e.g. `+0` vs `−0`, `NaN` variants), which makes it impossible to guarantee
that a hash computed on one platform will match one computed on another. Use
`DECIMAL`/`NUMERIC` instead.

### BLOB / binary size guard

`BLOB`, `BINARY`, `VARBINARY`, and `LONGVARBINARY` columns are read into a `byte[]`
and checked against `Config.maxBlobSize` (default **64 MB**). Rows whose binary
column exceeds the limit are rejected with a `SqlLedgerException` rather than silently
producing a truncated or garbage hash.

### Unsupported types

Any SQL type not listed above (e.g. `ARRAY`, `JAVA_OBJECT`, `STRUCT`, `REF`,
`DATALINK`, `ROWID`, `SQLXML`) raises a `SqlLedgerException` that identifies the
offending type by name and code, along with the `[row:col]` coordinates of the cell.

---

## Salting

Each cell in a row may be salted — a 32-byte random value that is hashed together with
the cell's type byte and data, making the cell hash unlinkable without knowledge of the
salt. Whether a cell is salted is determined by the `SaltScheme`:

- `SaltScheme.of()` — no cells salted
- `SaltScheme.ofAll()` — all cells salted
- `SaltScheme.ofAllExcept(int...)` — all cells salted except specific column indices
- `SaltScheme.of(int...)` — only the specified column indices are salted

When any cells are salted, a `TableSalt` instance (from the `tablesalt` module) must
also be supplied. `TableSalt` deterministically derives a per-cell salt from a 32-byte
secret seed, the row number, and the column index. The seed must be kept secret and
**cannot be changed** after the ledger has been created — losing it makes it impossible
to regenerate proofs for existing rows.

---

## Core API

### `SqlLedger`

The central class. Implements `SourceLedger` and `java.nio.channels.Channel`.

```java
// Build the two prepared statements however you like, then:
var config = new SqlLedger.Config(sizeQuery, rowQuery, saltScheme, shaker);
try (var ledger = new SqlLedger(config)) {
    long n = ledger.updateSize();   // snapshot the current row count
    SourceRow row = ledger.getSourceRow(42L);
}
```

**`SqlLedger.Config`** (record):

| Field | Type | Notes |
|-------|------|-------|
| `sizeQuery` | `PreparedStatement` | Parameterless; must return a single `LONG` |
| `rowByNoQuery` | `PreparedStatement` | One `LONG` param (row number) |
| `saltScheme` | `SaltScheme` | Required; use `SaltScheme.of()` for no salting |
| `shaker` | `TableSalt` | Required when `saltScheme.hasSalt()` is `true` |
| `maxBlobSize` | `int` | Default `67_108_864` (64 MB) |

### `DbSession`

Factory that handles JDBC driver loading, connection creation, and `SqlLedger`
construction from `config` package objects:

```java
try (var session = DbSession.newInstance(dbConnection)) {
    try (var ledger = session.openLedger(ledgerDef, saltScheme, saltSeed)) {
        // ...
    }
}
```

---

## Configuration (`io.crums.sldg.src.sql.config`)

The `config` sub-package provides JSON-serializable records for file- or
database-driven configuration:

| Class | JSON keys | Purpose |
|-------|-----------|---------|
| `LedgerDef` | `size_query`, `row_query`, `max_blob_size`? | SQL statements + optional blob limit |
| `DbConnection` | `url`, `driver_class`?, `username`?, `password`? | JDBC connection settings |
| `DbCredentials` | `username`, `password` | Standalone credential fragment |
| `SaltSeed` | `salt_seed` | 32-byte secret; base64-32 or 64-char hex |
| `SaltSchemeParser` | `ssip`, `ssi` | JSON parser for `SaltScheme` |

### Secrets handling

Two kinds of secrets are involved:

1. **Database credentials** — username and password for the JDBC connection.
2. **Salt seed** — the 32-byte secret that drives deterministic salt derivation.
   This is especially sensitive: it must never be exposed, and if lost, skipledger
   proofs can no longer be generated for any row of the ledger.

Sensitive fragments can be kept in separate files with restricted permissions
(`chmod 600`) and composed at runtime, rather than stored together in a single
world-readable config file.

---

## Dependencies

| Module | Role |
|--------|------|
| `source-ledger` | Defines `SourceLedger`, `SourceRow`, `Cell`, `DataType`, `SaltScheme` |
| `tablesalt` | Provides `TableSalt` for deterministic cell-salt derivation |
| `jsonimple` | Lightweight JSON parsing for the `config` package |
| JDBC driver | Supplied at runtime by the caller; not bundled |
