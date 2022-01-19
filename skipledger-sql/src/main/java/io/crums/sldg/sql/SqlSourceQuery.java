/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.SldgConstants;
import io.crums.sldg.SourceLedger;
import io.crums.sldg.src.BytesValue;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.DateValue;
import io.crums.sldg.src.DoubleValue;
import io.crums.sldg.src.HashValue;
import io.crums.sldg.src.LongValue;
import io.crums.sldg.src.NullValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.src.TableSalt;
import io.crums.util.BigShort;
import io.crums.util.Lists;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * An SQL {@linkplain SourceLedger}.
 * 
 * @see Builder
 */
public class SqlSourceQuery implements SourceLedger {
  
  
  public final static int DEFAULT_MAX_COL_MEM_SIZE = 4 * 1024;
  
  
  
  /**
   * Thin abstraction for building {@linkplain SqlSourceQuery} instances.
   * 
   * @see DefaultBuilder
   * @see DirectBuilder
   */
  public static abstract class Builder {
    
    /**
     * Returns the "SELECT COUNT(*) .." (from whatever table or view) query.
     */
    public abstract String getPreparedSizeQuery();
    
    /**
     * Returns the row-by-number prepared-statement string. (Parameterized with a single '?').
     * Note it's OK for the query result to include {@code NULL}s for column-values.
     */
    public abstract String getPreparedRowByNumberQuery();


    
    /**
     * Builds and returns a {@linkplain SqlSourceQuery} instance using the given arguments
     * and the instance's size- and row-queries.
     * 
     * @param con DB connection (read-only is fine)
     * @param shaker per table-cell salt generator
     * 
     * @see #getPreparedSizeQuery()
     * @see #getPreparedRowByNumberQuery()
     */
    public SqlSourceQuery build(Connection con, TableSalt shaker) throws SqlLedgerException {
      Objects.requireNonNull(con, "null connection");
      Objects.requireNonNull(shaker, "null table salt");
      try {
        var szQuery = con.prepareStatement(getPreparedSizeQuery());
        var rowByNumQuery = con.prepareStatement(getPreparedRowByNumberQuery());
        return new SqlSourceQuery(szQuery, rowByNumQuery, shaker);
      
      } catch (SQLException sqx) {
        throw new SqlLedgerException("on build(" + con + "): " + sqx, sqx);
      }
    }
  }
  
  /**
   * Uses 2 SQL queries in string form.
   * 
   * @see DirectBuilder#DirectBuilder(String, String)
   */
  public static class DirectBuilder extends Builder {
    
    protected final String sizeQuery;
    
    protected final String rowQuery;
    
    
    /**
     * 
     * @param sizeQuery the "SELECT COUNT(*) .." query
     * @param rowQuery the row-by-number prepared-statement string
     */
    public DirectBuilder(String sizeQuery, String rowQuery) {
      this.sizeQuery = Objects.requireNonNull(sizeQuery, "null sizeQuery");
      this.rowQuery = Objects.requireNonNull(rowQuery, "null rowQuery");
      
      int q = rowQuery.indexOf('?');
      if (q == -1)
        throw new IllegalArgumentException("missing '?' in rowQuery: " + rowQuery);
      else if (rowQuery.lastIndexOf('?') != q)
        throw new IllegalArgumentException("multiple '?'s in rowQuery: " + rowQuery);
      if (sizeQuery.indexOf('?') != -1)
        throw new IllegalArgumentException("sizeQuery should not be parameterized with '?': " + sizeQuery);
    }


    @Override
    public String getPreparedSizeQuery() {
      return sizeQuery;
    }


    @Override
    public String getPreparedRowByNumberQuery() {
      return rowQuery;
    }
  }
  
  
  /**
   * This is a <em>starter</em> builder. It helps setup the 
   * {@linkplain #getPreparedSizeQuery() size-} and
   * {@linkplain #getPreparedRowByNumberQuery() row-}queries using
   * the following information about the source-table's PRIMARY KEY column:
   * <ul>
   * <li>The table name</li>
   * <li>"PRIMARY KEY" column name</li>
   * <li>Other column names</li>
   * </ul>
   * <p>
   * The idea here is that a DBA may review these generated queries, tweak them (or
   * even re-write them from scratch as long as they follow the protocol below).
   * </p>
   * <h2>Assumptions</h2>
   * <ol>
   * <li>The table can be ordered by the "PRIMARY KEY" column (quoted here because any
   * column whose values behave this way, not necessarily the PK, should work fine). </li>
   * <li>Every <em>new</em> row's PRIMARY KEY column value is greater than those of the
   * previous existing rows.</li>
   * <li>The column values, are never modified. (At least not in the normal workflow; if you
   * do change a previous row's value, then you also lose history for every row after it.)</li>
   * </ol>
   */
  public static class DefaultBuilder extends Builder {
    
    protected final String table;
    protected final String primaryKeyColumn;
    
    protected final List<String> columnNames;
    
    
    public DefaultBuilder(String table, List<String> columnNames) {
      this.table = Objects.requireNonNull(table, "null table name").trim();
      int count = columnNames.size();
      if (count < 2)
        throw new IllegalArgumentException("too few column names: " + columnNames);
      this.primaryKeyColumn = columnNames.get(0);
      this.columnNames = Lists.readOnlyCopy(columnNames.subList(1, count), true);
      
      checkArgs();
    }
    
    public DefaultBuilder(String table, String primaryKeyColumn, String... columnNames) {
      this.table = Objects.requireNonNull(table, "null table name").trim();
      this.primaryKeyColumn = Objects.requireNonNull(primaryKeyColumn, "null primary key column name").trim();
      this.columnNames = Lists.readOnlyCopy(Arrays.asList(columnNames), true);
      
      checkArgs();
    }
    
    
    private void checkArgs() {
      if (this.table.length() < 2)
        throw new IllegalArgumentException("table name too short: '" + table + "'");
      if (this.primaryKeyColumn.length() < 2)
        throw new IllegalArgumentException(
            "primary key column name too short: '" + primaryKeyColumn + "'");
      
      if (this.columnNames.isEmpty())
        throw new IllegalArgumentException("empty column names");
      
      for (var col : this.columnNames) {
        if (malformedColumnName(col))
          throw new IllegalArgumentException("column name '" + col + "'");
      }
      
      if (columnNames.contains(primaryKeyColumn))
        throw new IllegalArgumentException(
            "primary key column name '" + primaryKeyColumn +
            "' duplicated (occurs) in columnNames: " + columnNames);
    }
    
    
    
    private boolean malformedColumnName(String colName) {
      if (colName == null || colName.isEmpty())
        return true;
      for (int index = colName.length(); index-- > 1; ) {
        char c = colName.charAt(index);
        boolean ok = Strings.isAlphabet(c) || Strings.isDigit(c) || c == '_';
        if (!ok)
          return true;
      }
      return ! Strings.isAlphabet(colName.charAt(0));
    }
    
    
    
    public String getPreparedSizeQuery() {
      return "SELECT count(*) FROM " + table + " AS rcount";
    }
    
    
    
    public String getPreparedRowByNumberQuery() {
      StringBuilder sql = new StringBuilder("SELECT * FROM (  SELECT ROW_NUMBER() OVER (ORDER BY ");
      sql.append(primaryKeyColumn).append(" ASC) AS row_index, ").append(primaryKeyColumn);
      for (String cName : columnNames)
        sql.append(", ").append(cName);
      sql.append(" FROM ").append(table).append(") AS snap WHERE row_index = ?");
      return sql.toString();
    }
    
    
  }
  
  
  
  private final PreparedStatement sizeQuery;
  private final PreparedStatement rowByNumberQuery;
  private final TableSalt shaker;
  private int maxColumnBytes;

  /**
   * Creates a salted instance.
   */
  public SqlSourceQuery(PreparedStatement sizeQuery, PreparedStatement rowByNumberQuery, TableSalt shaker)
      throws SqlLedgerException {
    this(sizeQuery, rowByNumberQuery, shaker, DEFAULT_MAX_COL_MEM_SIZE);
  }

  /**
   * Creates a salted instance.
   */
  public SqlSourceQuery(
      PreparedStatement sizeQuery, PreparedStatement rowByNumberQuery,
      TableSalt shaker, int maxColumnBytes)
      throws SqlLedgerException {
    
    try {
      this.sizeQuery = Objects.requireNonNull(sizeQuery, "null sizeQuery");
      this.rowByNumberQuery = Objects.requireNonNull(rowByNumberQuery, "null rowByNumberQuery");
      this.shaker = Objects.requireNonNull(shaker, "null shaker");
      setMaxColumnBytes(maxColumnBytes);
      
      if (sizeQuery.isClosed() || rowByNumberQuery.isClosed())
        throw new IllegalArgumentException("prepared statement[s] is closed");
      
    } catch (SQLException sqx) {
      throw new SqlLedgerException(sqx);
    }
  }
  
  
  public synchronized void setMaxColumnBytes(int maxColumnBytes) {
    if (maxColumnBytes < 0)
      throw new IllegalArgumentException("negative maxColumnBytes: " + maxColumnBytes);
    this.maxColumnBytes = maxColumnBytes;
  }
  
  
  public int getMaxColumnBytes() {
    return maxColumnBytes;
  }

  @Override
  public synchronized void close() {
    try (TaskStack closer = new TaskStack()) {
      closer.pushClose(sizeQuery);
      closer.pushClose(rowByNumberQuery);
      closer.pushClose(shaker);
    }
  }

  @Override
  public synchronized long size() {
    try {
      ResultSet rs = sizeQuery.executeQuery();
      if (!rs.next())
        throw new SqlLedgerException("no result-set from query " + sizeQuery);
      
      return rs.getLong(1);
    } catch (SQLException sqx) {
      throw new SqlLedgerException("on size(): " + sqx, sqx);
    }
  }

  @Override
  public synchronized SourceRow getSourceRow(long rn) {
    try {
      rowByNumberQuery.setLong(1, rn);
      ResultSet rs = rowByNumberQuery.executeQuery();
      
      ResultSetMetaData meta = rs.getMetaData();
      
      if (!rs.next())
        throw new SqlLedgerException("no result-set from query " + rowByNumberQuery);
      
      final int columnCount = meta.getColumnCount();
      if (columnCount < 2)
        throw new SqlLedgerException("no columns in result-set (!)");
      
      // the row number is returned in the result set
      ColumnValue[] columns = new ColumnValue[columnCount - 1];
      for (int col = 2; col <= columnCount; ++col)
        columns[col - 2] = getColumnValue(rs, meta, rn, col);
      
      return new SourceRow(rn, columns);
      
    } catch (SQLException sqx) {
      throw new SqlLedgerException("on getSourceByRowNumber(" + rn + "): " + sqx, sqx);
    }
  }
  
  
  private ColumnValue getColumnValue(ResultSet rs, ResultSetMetaData meta, long rn, int col) throws SQLException {
    int sqlType = meta.getColumnType(col);
    var salt = shaker.salt(rn, col);
    
    switch (sqlType) {
    case Types.BOOLEAN:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
      {
        long longVal = rs.getLong(col);
        return rs.wasNull() ? new NullValue(salt) : new LongValue(longVal, salt);
      }
    case Types.DATE:
    case Types.TIME:
    case Types.TIME_WITH_TIMEZONE:
    case Types.TIMESTAMP:
    case Types.TIMESTAMP_WITH_TIMEZONE:
      {
        Date date = rs.getDate(col);
        return date == null ? new NullValue(salt) : new DateValue(date.getTime(), salt);
      }
    case Types.NULL:
      return new NullValue(salt);
    case Types.DECIMAL:
    case Types.DOUBLE:
    case Types.FLOAT:
    case Types.NUMERIC:
    case Types.REAL:
      {
        double value = rs.getDouble(col);
        return rs.wasNull() ? new NullValue(salt) : new DoubleValue(value, salt);
      }
    
    case Types.BLOB:
      {
        Blob blob = rs.getBlob(col);
        if (blob == null)
          return new NullValue(salt);
        
        final long len = blob.length();
        try (var stream = blob.getBinaryStream()) {
          if (len > maxColumnBytes) {
            MessageDigest digest = SldgConstants.DIGEST.newDigest();
            byte[] in = new byte[DEFAULT_MAX_COL_MEM_SIZE];
            while (true) {
              int amtRead = stream.read(in);
              if (amtRead == -1)
                break;
              digest.update(in, 0, amtRead);
            }
            
            byte[] unsaltedHash = digest.digest();
            byte[] hash = ColumnValue.saltHash(salt, unsaltedHash, digest);
            return new HashValue(ByteBuffer.wrap(hash));
          
          } else {
            byte[] bytes = new byte[(int) len];
            for (int off = 0; off < bytes.length;) {
              int bytesRead = stream.read(bytes, off, bytes.length - off);
              assert bytesRead > 0;
              off += bytesRead;
            }
            
            return new BytesValue(ByteBuffer.wrap(bytes), salt);
          }
        } catch (IOException iox) {
          throw new SqlLedgerException("on retrieving blob @ [" + rn + ", " + col + "]:" + iox , iox);
        }
      }
    
    // the various char types (not worth enumerating) are cast as strings below
    default:
      // per the jdbc tutorial this always works
      // see https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html#retrieve_rs
      //
      String stringVal = rs.getString(col);
      if (stringVal == null)
        return new NullValue(salt);
      
      var out = new StringValue(stringVal, salt);
      return
          (out.serialSize() > maxColumnBytes + BigShort.BYTES + 1) ?
              new HashValue(out.getHash()) : out;
    }
  }

}
