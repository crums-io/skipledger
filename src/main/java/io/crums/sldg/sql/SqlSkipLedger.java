/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.sql.Connection;
import java.sql.SQLException;

import io.crums.sldg.CompactSkipLedger;

/**
 * 
 */
public class SqlSkipLedger extends CompactSkipLedger {
  
  
  
  public static SqlSkipLedger declareNewLedger(Connection con, String tableName) throws SQLException {
    SqlTableAdaptor table = SqlTableAdaptor.declareNewTable(con, tableName);
    return new SqlSkipLedger(table);
  }

  
  
  public SqlSkipLedger(Connection con, String tableName) throws SQLException {
    this(new SqlTableAdaptor(con, tableName));
  }
  
  /**
   * @param table
   */
  public SqlSkipLedger(SqlTableAdaptor table) {
    super(table);
  }

}
