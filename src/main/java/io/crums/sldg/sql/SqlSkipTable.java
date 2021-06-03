/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import static io.crums.sldg.sql.HashLedgerSchema.*;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Logger;

import io.crums.sldg.SkipTable;
import io.crums.sldg.SldgConstants;
import io.crums.util.Base64_32;

/**
 * 
 * 
 */
public class SqlSkipTable implements SkipTable {




  /**
   * Creates a new skip ledger table in the given database.
   * 
   * @param con       the db connection
   * @param tableName the table name (no table by this name should exist in the database)
   * 
   * @see HashLedgerSchema#createSkipLedgerTableSql(String)
   */
  public static SqlSkipTable declareNewTable(Connection con, String tableName) throws SQLException {
    // create the table
    String sql = HashLedgerSchema.createSkipLedgerTableSql(tableName);
    Statement stmt = Objects.requireNonNull(con, "null con").createStatement();
    stmt.execute(sql);
    con.commit();
    return new SqlSkipTable(con, tableName);
  }
  
  
  
  
  
  
  
  private final Connection con;
  private final String tableName;
  private final PreparedStatement szStmt;
  private final PreparedStatement selectRow;
  
  
  
  

  /**
   * 
   */
  public SqlSkipTable(Connection con, String tableName) throws SQLException {
    this.con = Objects.requireNonNull(con, "null connection");
    if (!con.isReadOnly())
      con.setAutoCommit(false);
    this.tableName = Objects.requireNonNull(tableName, "null table name").trim();
    
    this.szStmt = con.prepareStatement(
        "SELECT COUNT(" + ROW_NUM + ") FROM " + tableName + " AS count");
    
    this.selectRow = con.prepareStatement(
        "SELECT " + SRC_HASH + ", " + ROW_HASH + " FROM " + tableName + " WHERE " + ROW_NUM + " = ?");
  }
  
  
  

  @Override
  public synchronized long addRows(ByteBuffer rows, long index) {
    // row count:
    final int count = rows.remaining() / ROW_WIDTH;
    if (count == 0 || count * ROW_WIDTH != rows.remaining())
      throw new IllegalArgumentException("remaining bytes not a positive multiple of " + ROW_WIDTH + ": " + rows);
    
    final long size = size();
    if (index != size)
      throw new IllegalArgumentException("on addRows(), index=" + index + ", size=" + size);
    
    final long base = size + 1;
    
    // ea row gets a number and 2 hashes
    StringBuilder query = new StringBuilder()
        .append("INSERT INTO ").append(tableName).append('(')
        .append(ROW_NUM).append(", ").append(SRC_HASH).append(", ").append(ROW_HASH).append(") VALUES");

    String[] encoded = new String[count * 2];
    for (int i = 0; i < encoded.length; ++i)
      encoded[i] = Base64_32.encodeNext32(rows);
    
    for (int r = 0; r < count; ++r) {
      long rowNum = base + r;
      int ei = r * 2;
      query.append("\n( ").append(rowNum).append(", '").append(encoded[ei]).append("', '").append(encoded[ei + 1]).append("'),");
    }
    
    // trim the last comma
    query.setLength(query.length() - 1);
    
    try {
      Statement stmt = con.createStatement();
      stmt.execute(query.toString());
      int updateCount = stmt.getUpdateCount();
      if (updateCount == count)
        con.commit();
      else {
        con.rollback();
        throw new SqlLedgerException(
            "on addRow(): updateCount=" + updateCount + ", count=" + count + ", index=" + index +
            ", encodedRows=" + Arrays.asList(encoded));
      }
    } catch (SQLException sx) {
      throw new SqlLedgerException(
          "on addRow(): index=" + index + ", encoded inputs/row-hashes: " + Arrays.asList(encoded), sx);
    }
    return size + count;
  }
  
  
  

  @Override
  public synchronized ByteBuffer readRow(long index) {
    if (index < 0)
      throw new IllegalArgumentException("index " + index);
    
    try {
      selectRow.setLong(1, index + 1);
      ResultSet result = selectRow.executeQuery();
      
      if (!result.next())
        throw new IllegalArgumentException("index out-of-bounds: " + index);
      
      byte[] row = new byte[ROW_WIDTH];
      
      Base64_32.decode(result.getString(1), row, 0);
      Base64_32.decode(result.getString(2), row, ROW_WIDTH / 2);
      
      return ByteBuffer.wrap(row);
        
    } catch (SQLException sx) {
      throw new SqlLedgerException("on readRow(): index=" + index, sx);
    }
  }
  

  @Override
  public synchronized long size() {
    try {
      ResultSet result = szStmt.executeQuery();
      if (result.next())
        return result.getLong(1);
      
      throw new SqlLedgerException("onSize(): result=" + result);
    
    } catch (SQLException sx) {
      throw new SqlLedgerException("on size()", sx);
    }
  }

  
  @Override
  public synchronized void close() {
    try {
      con.close();
    } catch (SQLException sx) {
      // eat it but nag
      Logger.getLogger(SldgConstants.LOGGER_NAME).warning("ignoring con.close() complaint: " + sx);
    }
  }

}
