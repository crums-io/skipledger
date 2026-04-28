# source-ledger

## Overview

The `source-ledger` module defines how typed source-row data is structured,
serialized, and hashed for skip ledgers. Each row is an ordered sequence of
typed cells; a row's hash is derived from its cell hashes; cell hashes commit
to the cell's data type, raw data, and optional salt. These row hashes are the
*input hashes* fed into the skipledger commitment chain.

---

## Data Types (`DataType`)

Every cell carries one of the following types. The type byte (enum ordinal) is
included in the cell's hash, so two cells with identical raw bytes but different
types hash differently.

| `DataType` | Java type | Size | Notes |
|------------|-----------|------|-------|
| `NULL`     | `null`    | 5 bytes fixed | Zeroed; not a column type — used when a typed value is `null` |
| `STRING`   | `String`  | variable | UTF-8 encoding |
| `LONG`     | `Long`    | 8 bytes | Big-endian two's-complement |
| `DATE`     | `Long` / `Date` | 8 bytes | UTC milliseconds; a qualified `LONG` subtype |
| `BIG_INT`  | `BigInteger` | ≥ 1 byte | Two's-complement byte array (`BigInteger.toByteArray()`) |
| `BIG_DEC`  | `BigDecimal` | ≥ 2 bytes | 1-byte scale (−128…127) followed by unscaled `BIG_INT` |
| `BOOL`     | `Boolean` | 1 byte | `0` (false) or `1` (true) |
| `BYTES`    | `ByteBuffer` / `byte[]` | variable | Untyped blob |
| `HASH`     | `ByteBuffer` / `byte[]` | 32 bytes | SHA-256 digest |

> **No floating-point types.** `float` and `double` are deliberately excluded
> to avoid ambiguity (IEEE-754 has multiple representations of the same logical
> value, e.g. `+0` vs `−0`, `NaN` variants).

`DataType.guessType(Object)` infers the type from a Java value.
`DataType.toByteBuffer(Object)` converts a value to its canonical byte sequence.
`DataType.toValue(ByteBuffer)` converts bytes back to a Java value.

---

## Cells (`Cell`)

A `Cell` carries a data type and a byte sequence that represents the value.
Cells may be *revealed* (data is present) or *redacted* (only the hash is kept).
Revealed cells may be *salted* or *unsalted*.

### Hash formula

| Cell kind | Formula |
|-----------|---------|
| Unsalted  | `SHA-256( type_byte ‖ data )` |
| Salted    | `SHA-256( cell_salt ‖ type_byte ‖ data )` |

where `type_byte` is the enum ordinal of the `DataType`.

### Key implementations

| Class | Description |
|-------|-------------|
| `Cell.UnsaltedReveal` | Revealed, no salt |
| `Cell.SaltedCell` | Revealed, with an explicit 32-byte cell salt |
| `Cell.RowSaltedCell` | Revealed, salt derived from a per-row root salt |
| `Cell.SaltedNull` | Null value with cell salt |
| `Cell.RowSaltedNull` | Null value with row salt |
| `Cell.Redacted` | Only the 32-byte hash is stored; `dataType()` returns `HASH` |
| `Cell.UNSALTED_NULL` | Singleton unsalted null cell |

`cell.redact()` returns a `Cell.Redacted` whose `hash()` equals the original
cell's hash. Calling `redact()` on an already-redacted cell is a no-op
(returns `this`).

---

## Rows (`SourceRow`)

A `SourceRow` has a 1-based row number and an ordered list of `Cell` instances.

### Row hash rules

| Cells | Row hash |
|-------|----------|
| 0 (empty / null row) | sentinel hash (32 zeroed bytes) |
| 1 | `cell.hash()` directly — **not** re-hashed |
| ≥ 2 | `SHA-256( cellHash₀ ‖ cellHash₁ ‖ … )` |

`SourceRow.nullRow(rowNo)` creates an empty row whose hash is the sentinel.

`row.redact(index)` returns a new `SourceRow` with cell `index` replaced by
its redacted form; the row hash is unchanged.

---

## Salting (`SaltScheme`)

`SaltScheme` controls which cells within a row are salted.

| Constant / factory | Effect |
|--------------------|--------|
| `SaltScheme.NO_SALT` | No cells are salted |
| `SaltScheme.SALT_ALL` | Every cell is salted |
| `SaltScheme.of(int... indices)` | Only the listed column indices are salted |
| `SaltScheme.ofAllExcept(int... indices)` | All columns salted except the listed ones |

Cell salts are derived **deterministically** from a per-table `TableSalt` root
value and the `(row-number, cell-index)` pair via `TableSalt.cellSalt()`.
This means the same root produces identical salts across processes and restarts.

---

## Serialization (`SourcePack`)

`SourcePack` is a compact binary format that holds a collection of source rows.
It supports selective redaction — individual cells can be redacted while keeping
the remaining data and the overall row hash verifiable.

```java
// Writing
SourcePackBuilder builder = ...;
ByteBuffer packed = builder.serialize();

// Reading
SourcePack pack = SourcePack.load(packed);
List<SourceRow> rows = pack.sources();
```

The format uses variable-width encodings for cell counts and data sizes to keep
small packs small. See `SourcePackBinaryFormat.txt` for the detailed byte layout.
