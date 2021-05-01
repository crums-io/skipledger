/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.sql.Connection;
import java.sql.SQLException;

import io.crums.sldg.CompactLedger;

/**
 * 
 */
public class SqlLedger extends CompactLedger {
  
  
  
  public static SqlLedger declareNewLedger(Connection con, String tableName) throws SQLException {
    SqlTableAdaptor table = SqlTableAdaptor.declareNewTable(con, tableName);
    return new SqlLedger(table);
  }

  
  
  public SqlLedger(Connection con, String tableName) throws SQLException {
    this(new SqlTableAdaptor(con, tableName));
  }
  
  /**
   * @param table
   */
  public SqlLedger(SqlTableAdaptor table) {
    super(table);
  }

}
