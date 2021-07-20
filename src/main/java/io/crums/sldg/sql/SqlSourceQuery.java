/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import static io.crums.sldg.src.ColumnValue.*;

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
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.SldgConstants;
import io.crums.sldg.SourceLedger;
import io.crums.sldg.src.BytesValue;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.HashValue;
import io.crums.sldg.src.LongValue;
import io.crums.sldg.src.NullValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.src.TableSalt;
import io.crums.util.Lists;
import io.crums.util.TaskStack;

/**
 * 
 */
public class SqlSourceQuery implements SourceLedger {
  
  
  public final static int DEFAULT_MAX_COL_MEM_SIZE = 4 * 1024;
  
  
  
  public static class Builder {
    
    protected final String table;
    protected final String primaryKeyColumn;
    
    protected final List<String> columnNames;
    
    
    public Builder(String table, String primaryKeyColumn, String... columnNames) {
      this.table = Objects.requireNonNull(table, "null table name").trim();
      this.primaryKeyColumn = Objects.requireNonNull(primaryKeyColumn, "null primary key column name").trim();
      this.columnNames = Lists.asReadOnlyList(columnNames);
      
      if (this.table.length() < 2)
        throw new IllegalArgumentException("table name too short: '" + table + "'");
      if (this.primaryKeyColumn.length() < 2)
        throw new IllegalArgumentException("primary key column name too short: '" + primaryKeyColumn + "'");
      
      if (this.columnNames.isEmpty())
        throw new IllegalArgumentException("empty column names");
      
      HashSet<String> names = new HashSet<>();
      for (var col : this.columnNames) {
        if (col == null || col.trim().length() < 2)
          throw new IllegalArgumentException("column name");
        if (!names.add(col))
          throw new IllegalArgumentException("duplicate column name '" + col + "': " + columnNames);
      }
      if (!names.add(primaryKeyColumn))
        throw new IllegalArgumentException(
            "primary key column name '" + primaryKeyColumn + "' must not be duplicated in " + columnNames);
    }
    
    
    
    public SqlSourceQuery build(Connection con) throws SqlLedgerException {
      return build(con, TableSalt.NULL_SALT);
    }
    
    
    public SqlSourceQuery build(Connection con, TableSalt shaker) throws SqlLedgerException {
      Objects.requireNonNull(con, "null connection");
      try {
        var szQuery = createSizeQuery(con);
        var rowByNumQuery = createRowByNumberQuery(con);
        return new SqlSourceQuery(szQuery, rowByNumQuery, shaker);
      
      } catch (SQLException sqx) {
        throw new SqlLedgerException("on build(" + con + "): " + sqx, sqx);
      }
    }
    
    
    protected PreparedStatement createSizeQuery(Connection con) throws SQLException {
      return con.prepareStatement("SELECT count(*) FROM " + table + " AS rcount");
    }
    
    protected PreparedStatement createRowByNumberQuery(Connection con) throws SQLException {
      StringBuilder sql = new StringBuilder("SELECT * FROM (\n  SELECT ROW_NUMBER() OVER (ORDER BY ");
      sql.append(primaryKeyColumn).append(" ASC) AS row_index, ").append(primaryKeyColumn);
      for (String cName : columnNames)
        sql.append(", ").append(cName);
      sql.append(" FROM ").append(table).append(") AS snap WHERE row_index = ?");
      
      return con.prepareStatement(sql.toString());
    }
  }
  
  
  
  private final PreparedStatement sizeQuery;
  private final PreparedStatement rowByNumberQuery;
  private final TableSalt shaker;

  /**
   * 
   */
  public SqlSourceQuery(PreparedStatement sizeQuery, PreparedStatement rowByNumberQuery)
      throws SqlLedgerException {
    this(sizeQuery, rowByNumberQuery, TableSalt.NULL_SALT);
  }

  /**
   * 
   */
  public SqlSourceQuery(PreparedStatement sizeQuery, PreparedStatement rowByNumberQuery, TableSalt shaker)
      throws SqlLedgerException {
    
    try {
      this.sizeQuery = Objects.requireNonNull(sizeQuery, "null sizeQuery");
      this.rowByNumberQuery = Objects.requireNonNull(rowByNumberQuery, "null rowByNumberQuery");
      this.shaker = Objects.requireNonNull(shaker, "null shaker");
      
      if (sizeQuery.isClosed() || rowByNumberQuery.isClosed())
        throw new IllegalArgumentException("prepared statement[s] is closed");
      
    } catch (SQLException sqx) {
      throw new SqlLedgerException(sqx);
    }
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
  public synchronized SourceRow getSourceByRowNumber(long rn) {
    try {
      rowByNumberQuery.setLong(1, rn);
      ResultSet rs = rowByNumberQuery.executeQuery();
      
      ResultSetMetaData meta = rs.getMetaData();
      
      if (!rs.next())
        throw new SqlLedgerException("no result-set from query " + rowByNumberQuery);
      
      final int columnCount = meta.getColumnCount();
      if (columnCount < 2)
        throw new SqlLedgerException("no columns in result-set (wtf?)");
      
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
    switch (sqlType) {
    case Types.BOOLEAN:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
      {
        long longVal = rs.getLong(col);
        return rs.wasNull() ? new NullValue(shaker.salt(rn, col)) : new LongValue(longVal, shaker.salt(rn, col));
      }
    case Types.DATE:
      {
        Date date = rs.getDate(col);
        return date == null ? new NullValue(shaker.salt(rn, col)) : new LongValue(date.getTime());
      }
    case Types.TIME:
      {
        Time time = rs.getTime(col);
        return time == null ? new NullValue(shaker.salt(rn, col)) : new LongValue(time.getTime());
      }
    case Types.TIMESTAMP:
      {
        
        Timestamp timestamp = rs.getTimestamp(col);
        return timestamp == null ? new NullValue(shaker.salt(rn, col)) : new LongValue(timestamp.getTime());
      }
    case Types.NULL: return NULL_VALUE;
    
    case Types.BLOB:
      {
        Blob blob = rs.getBlob(col);
        if (blob == null)
          return NULL_VALUE;
        
        final long len = blob.length();
        try (var stream = blob.getBinaryStream()) {
          if (len > DEFAULT_MAX_COL_MEM_SIZE) {
            MessageDigest digest = SldgConstants.DIGEST.newDigest();
            byte[] in = new byte[DEFAULT_MAX_COL_MEM_SIZE];
            while (true) {
              int amtRead = stream.read(in);
              if (amtRead == -1)
                break;
              digest.update(in, 0, amtRead);
            }
            
            byte[] hash = digest.digest();
            return new HashValue(ByteBuffer.wrap(hash));
          
          } else {
            byte[] bytes = new byte[(int) len];
            for (int off = 0; off < bytes.length;) {
              int bytesRead = stream.read(bytes, off, bytes.length - off);
              assert bytesRead > 0;
              off += bytesRead;
            }
            
            return new BytesValue(ByteBuffer.wrap(bytes));
          }
        } catch (IOException iox) {
          throw new SqlLedgerException("on retrieving blob: " + iox , iox);
        }
      }
    
    default:
      // per the jdbc tutorial this always works
      // see https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html#retrieve_rs
      //
      String stringVal = rs.getString(col);
      return stringVal == null ? ColumnValue.NULL_VALUE : new StringValue(stringVal);
    }
  }

}
