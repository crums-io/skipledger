/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql;


import java.nio.channels.Channel;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import io.crums.sldg.salt.TableSalt;
import io.crums.sldg.src.SourceLedger;
import io.crums.sldg.src.SaltScheme;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.SourceRowBuilder;
import io.crums.util.TaskStack;

/**
 * A ledger composed of 2 user-defined SQL queries. One returns the
 * number of rows in the ledger; the other returns the contents of a single
 * row, given a row-no. parameter.
 */
public class SqlLedger implements SourceLedger, Channel {
  
  /** Default maximum BLOB size (length) in bytes. */
  public final static int DEFAULT_MAX_BLOB_SIZE = 64 * 1024 * 1024;
  
  
  /**
   * Chunky configuration parameters.
   */
  protected record Config(
      PreparedStatement sizeQuery,
      PreparedStatement rowByNoQuery,
      SaltScheme saltScheme,
      TableSalt shaker,
      int maxBlobSize) implements Channel {
    
    
    /**
     * Full constructor.
     * 
     * @param sizeQuery       size-query takes no arguments
     * @param rowByNoQuery    row-by-no query takes single row-no. argument
     * @param saltScheme      not {@code null}
     * @param shaker          not {@code null}, if {@code saltScheme.hasSalt()}
     * @param maxBlobSize     maximum length of SQL BLOBs (defaults to
     *                        {@linkplain SqlLedger#DEFAULT_MAX_BLOB_SIZE})
     */
    public Config {
      if (!isOpen(sizeQuery, rowByNoQuery, shaker))
        throw new IllegalArgumentException(
            "one or both prepared statements closed");
      if (saltScheme.hasSalt() && shaker == null)
        throw new IllegalArgumentException(
            "null shaker with salted scheme (%s)".formatted(saltScheme));
      if (maxBlobSize < 0)
        throw new IllegalArgumentException(
            "negative maxBlobSize: " + maxBlobSize);
    }
    
    
    /**
     * Constructs an instance with {@linkplain SqlLedger#DEFAULT_MAX_BLOB_SIZE}.
     * 
     * @param sizeQuery         size-query takes no arguments
     * @param rowByNoQuery      row-by-no query takes single row-no. argument
     * @param saltScheme        not {@code null}
     * @param shaker            not {@code null}, if {@code saltScheme.hasSalt()}
     */
    public Config(
        PreparedStatement sizeQuery,
        PreparedStatement rowByNoQuery,
        SaltScheme saltScheme,
        TableSalt shaker) {
      
      this(sizeQuery, rowByNoQuery, saltScheme, shaker, DEFAULT_MAX_BLOB_SIZE);
    }
    
    public Config(
        PreparedStatement sizeQuery,
        PreparedStatement rowByNoQuery,
        SaltScheme saltScheme,
        TableSalt shaker,
        Optional<Integer> maxBlobSize) {
      
      this(sizeQuery, rowByNoQuery, saltScheme, shaker,
          maxBlobSize.orElse(DEFAULT_MAX_BLOB_SIZE));
    }
    
    /**
     * Returns {@code true} is salt is used.
     * 
     * @return {@code saltScheme().hasSalt()}
     */
    public boolean isSalted() {
      return saltScheme.hasSalt();
    }
    
    
    private static boolean isOpen(
        PreparedStatement sizeQuery, PreparedStatement rowQuery, TableSalt shaker) {
      try {
        return
            (shaker == null || shaker.isOpen()) &&
            !sizeQuery.isClosed() &&
            !rowQuery.isClosed();
      } catch (SQLException sx) {
        throw new SqlLedgerException(sx);
      }
    }
    
    @Override
    public boolean isOpen() {
      return isOpen(sizeQuery, rowByNoQuery, shaker);
    }
    @Override
    public void close() {
      
      try (var closer = new TaskStack()) {
        if (shaker != null)
          closer.pushClose(shaker);
        closer.pushClose(rowByNoQuery, sizeQuery);
      }
    }
  }
  
  
  
  /** Faster than {@code volatile long} (?) */
  private final AtomicLong sizeSnapshot = new AtomicLong();
  
  private final Config config;
  
  private final SourceRowBuilder builder;
  

  /**
   * 
   */
  SqlLedger(Config config) {
    this.config = config;
    builder = new SourceRowBuilder(config.saltScheme, config.shaker);
    sizeSnapshot.set(liveSize());
  }

  /**
   * 
   */
  SqlLedger(Config config, long size) {
    builder = new SourceRowBuilder(config.saltScheme, config.shaker);
    this.config = config;
    setSize(size);
  }
  
  
  /**
   * Returns the <em>current</em> size of the ledger in the backing
   * RDBMS.
   * 
   * @return &ge; {@linkplain #size()}
   */
  public final synchronized long liveSize() throws SqlLedgerException {
    try {
      ResultSet rs = config.sizeQuery.executeQuery();
      if (!rs.next())
        throw new SqlLedgerException(
            "empty result-set from size-query " + config.sizeQuery);
      long size = rs.getLong(1);
      if (size < 0L)
        throw new SqlLedgerException(
            "size-query (%s) returned negative value %d"
            .formatted(config.sizeQuery, size));
      return size;
    
    } catch (SQLException sqx) {
      throw new SqlLedgerException("on size(): " + sqx, sqx);
    }
    
  }
  
  
  public synchronized void setSize(long size) {
    if (size == 0L) {
      sizeSnapshot.set(0L);
      return;
    }
    if (size < 0L)
      throw new IllegalArgumentException("negative size: " + size);
    
    long liveSize = liveSize();
    if (size > liveSize)
      throw new IllegalArgumentException(
          "attempt to set size (%d) beyond no. of rows in live query (%d)"
         .formatted(size, liveSize));
    
    sizeSnapshot.set(size);
  }
  
  
  public synchronized long updateSize() throws SqlLedgerException {
    long size = liveSize();
    sizeSnapshot.set(size);
    return size;
  }

  /**
   * Returns the "fixed" size.
   * 
   * @return &ge; 0 and $le; {@linkplain #liveSize()}
   * 
   * @see #setSize(long)
   * @see #liveSize()
   * @see #updateSize()
   */
  @Override
  public long size() {
    return sizeSnapshot.get();
  }

  @Override
  public synchronized SourceRow getSourceRow(long rn)
      throws IndexOutOfBoundsException, SqlLedgerException {
    
    if (rn <= 0L)
      throw new IndexOutOfBoundsException(
          "always-out-of-bounds row-no. (rn) " + rn);
    if (rn > size())
      throw new IndexOutOfBoundsException(
          "row-no. (rn) %d beyond size (%d)".formatted(rn, size()));
    
    try {
      
      return getSourceRowImpl(rn);
    
    } catch (SQLException sx) {
      throw new SqlLedgerException(
          "on getSourceRow(%d): %s".formatted(rn, sx), sx);
    }
  }

  @Override
  public SaltScheme saltScheme() {
    return config.saltScheme();
  }

  @Override
  public boolean isOpen() {
    return config.isOpen();
  }

  @Override
  public void close() {
    config.close();
  }
  
  /**
   * @param rn          range-checked
   */
  protected SourceRow getSourceRowImpl(long rn) throws SQLException {
    
    config.rowByNoQuery.setLong(1, rn);
    ResultSet rs = config.rowByNoQuery.executeQuery();
    
    if (!rs.next())
      return SourceRow.nullRow(rn);
    
    ResultSetMetaData meta = rs.getMetaData();
    
    final int cc = meta.getColumnCount();
    if (cc == 0)
      return SourceRow.nullRow(rn);
    
    Object[] values = new Object[cc];
    
    for (int index = 0; index < cc; ++index)
      values[index] = getColumnValue(rs, meta, rn, 1 + index);
    
    return builder.buildRow(rn, values);
  }
  
  /**
   * Returns the column value.
   * 
   * @param row         used only exception messages
   * @param col         1-based col no. (per SQL convention)
   * 
   * @see SourceRowBuilder#buildRow(long, Object...)
   */
  private Object getColumnValue(ResultSet rs, ResultSetMetaData meta, long row, int col)
      throws SQLException {
    
    switch (meta.getColumnType(col)) {
    
    case Types.ARRAY:
      throw new SqlLedgerException(
          "SQL type ARRAY not supported; cell [%d:%d]".formatted(row, col));
    
    case Types.BOOLEAN:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
      {
        long longVal = rs.getLong(col);
        return rs.wasNull() ? null : longVal;
      }
    case Types.DATE:
    case Types.TIME:
    case Types.TIME_WITH_TIMEZONE:
    case Types.TIMESTAMP:
    case Types.TIMESTAMP_WITH_TIMEZONE:
      {
        var date = rs.getDate(col);
        return date == null ? null : new Date(date.getTime());
      }
    case Types.NULL:
      return null;
      
    case Types.DECIMAL:
    case Types.DOUBLE:
    case Types.FLOAT:
    case Types.NUMERIC:
    case Types.REAL:
      throw new SqlLedgerException(
          "SQL type %s is not supported; cell [%d:%d]".formatted(
              meta.getColumnTypeName(col), row, col));
    case Types.BLOB:
      {
        Blob blob = rs.getBlob(col);
        if (blob == null)
          return null;
        long len = blob.length();
        if (len > config.maxBlobSize)
          throw new SqlLedgerException(
              "BLOB length %d > max blob size (%d bytes); cell [%d:%d]"
              .formatted(len, config.maxBlobSize));
        return blob.getBytes(0, (int) len);
      }
     
    // the various char types (not worth enumerating) are cast as strings below
    default:
      return rs.getString(col);
    }
  }

}
















