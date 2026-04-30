/*
 * Copyright 2025 Babak Farhang
 */
/**
 * SQL-backed {@code SourceLedger} defined by a pair of SQL {@code SELECT} statements.
 *
 * <h2>Overview</h2>
 * <p>
 * A ledger is defined by exactly two prepared statements:
 * </p>
 * <ul>
 *   <li><b>Size query</b> — parameterless; returns a single row with a single {@code LONG}
 *       value representing the current row count.</li>
 *   <li><b>Row query</b> — takes one {@code LONG} parameter (the 1-based row number);
 *       returns the column values for that row.</li>
 * </ul>
 * <p>
 * {@link io.crums.sldg.src.sql.SqlLedger SqlLedger} reads one row at a time, maps each
 * column to a {@link io.crums.sldg.src.DataType DataType}, and feeds the resulting typed
 * cells into the skipledger commitment chain. The database is never written to; the
 * connection is opened read-only.
 * </p>
 *
 * <h2>SQL Type Mapping</h2>
 * <table border="1">
 *   <caption>SQL-to-DataType mapping</caption>
 *   <tr><th>SQL type(s)</th><th>DataType</th><th>Java value</th><th>Notes</th></tr>
 *   <tr><td>{@code BOOLEAN}</td><td>{@code BOOL}</td><td>{@code Boolean}</td><td></td></tr>
 *   <tr><td>{@code BIT}</td><td>{@code BOOL}</td><td>{@code Boolean}</td><td>MySQL/MariaDB alias for {@code BOOLEAN}</td></tr>
 *   <tr><td>{@code TINYINT}, {@code SMALLINT}, {@code INTEGER}, {@code BIGINT}</td><td>{@code LONG}</td><td>{@code Long}</td><td></td></tr>
 *   <tr><td>{@code DECIMAL}, {@code NUMERIC}</td><td>{@code BIG_DEC}</td><td>{@code BigDecimal}</td><td></td></tr>
 *   <tr><td>{@code DATE}, {@code TIME}, {@code TIME WITH TIMEZONE}, {@code TIMESTAMP}, {@code TIMESTAMP WITH TIMEZONE}</td><td>{@code DATE}</td><td>UTC milliseconds as {@code Long}</td><td></td></tr>
 *   <tr><td>{@code CHAR}, {@code VARCHAR}, {@code LONGVARCHAR}, {@code NCHAR}, {@code NVARCHAR}, {@code LONGNVARCHAR}, {@code CLOB}, {@code NCLOB}</td><td>{@code STRING}</td><td>{@code String}</td><td>UTF-8</td></tr>
 *   <tr><td>{@code BLOB}</td><td>{@code BYTES}</td><td>{@code byte[]}</td><td>Size-guarded</td></tr>
 *   <tr><td>{@code BINARY}, {@code VARBINARY}, {@code LONGVARBINARY}</td><td>{@code BYTES}</td><td>{@code byte[]}</td><td>Same size guard as {@code BLOB}</td></tr>
 *   <tr><td>{@code NULL} result</td><td>{@code NULL}</td><td>{@code null}</td><td></td></tr>
 * </table>
 *
 * <h3>No floating-point</h3>
 * <p>
 * {@code FLOAT}, {@code DOUBLE}, and {@code REAL} columns are explicitly rejected with a
 * {@link io.crums.sldg.src.sql.SqlLedgerException SqlLedgerException}. IEEE-754 has
 * multiple bit representations of the same logical value (e.g. {@code +0} vs {@code −0},
 * {@code NaN} variants), making cross-platform hash reproducibility impossible. Use
 * {@code DECIMAL}/{@code NUMERIC} for any value that must appear in a ledger.
 * </p>
 *
 * <h3>BLOB / binary size guard</h3>
 * <p>
 * {@code BLOB}, {@code BINARY}, {@code VARBINARY}, and {@code LONGVARBINARY} columns are
 * checked against {@link io.crums.sldg.src.sql.SqlLedger.Config#maxBlobSize() Config.maxBlobSize}
 * (default 64 MB). Rows whose binary column exceeds the limit are rejected with a
 * {@code SqlLedgerException} rather than silently producing a truncated or garbage hash.
 * </p>
 *
 * <h3>Unsupported types</h3>
 * <p>
 * Any SQL type not in the table above (e.g. {@code ARRAY}, {@code JAVA_OBJECT},
 * {@code STRUCT}, {@code ROWID}, {@code SQLXML}) raises a {@code SqlLedgerException}
 * that identifies the offending type by name and JDBC code, along with the
 * {@code [row:col]} coordinates of the cell.
 * </p>
 *
 * <h2>Salting</h2>
 * <p>
 * Each cell in a row may be salted — a 32-byte value hashed together with the cell's
 * type byte and data, making the cell hash unlinkable without knowledge of the salt.
 * The {@link io.crums.sldg.src.SaltScheme SaltScheme} controls which columns are salted.
 * When any cells are salted, a {@code TableSalt} instance must also be supplied;
 * it deterministically derives a per-cell salt from a 32-byte secret seed, the row
 * number, and the column index.
 * </p>
 * <p>
 * The salt seed must be kept secret and <em>cannot be changed</em> after a ledger has
 * been created — losing it makes it impossible to regenerate proofs for existing rows.
 * </p>
 *
 * <h2>Core API</h2>
 * <p>
 * {@link io.crums.sldg.src.sql.SqlLedger SqlLedger} is the central class. It implements
 * {@code SourceLedger} and {@code java.nio.channels.Channel}. Construct it via
 * {@link io.crums.sldg.src.sql.SqlLedger.Config SqlLedger.Config}:
 * </p>
 * <pre>{@code
 * var config = new SqlLedger.Config(sizeQuery, rowQuery, saltScheme, shaker);
 * try (var ledger = new SqlLedger(config)) {
 *     long n = ledger.updateSize();
 *     SourceRow row = ledger.getSourceRow(42L);
 * }
 * }</pre>
 * <p>
 * {@link io.crums.sldg.src.sql.DbSession DbSession} is a higher-level factory that
 * handles JDBC driver loading, connection creation, and {@code SqlLedger} construction
 * from {@link io.crums.sldg.src.sql.config config} package objects.
 * </p>
 *
 * @see io.crums.sldg.src.sql.SqlLedger
 * @see io.crums.sldg.src.sql.DbSession
 * @see io.crums.sldg.src.sql.config
 */
package io.crums.sldg.src.sql;
