/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 
 */
public class SqlTestHarness {

  public final static String MOCK_DB_NAME = "sqledger";
  

  public final static String LEDGER_TABLENAME = "sldg";
  
  
  /**
   * @return an H2 db connection
   */
  public static Connection newDatabase(File dir) throws ClassNotFoundException, SQLException {
    dir.mkdirs();
    String dbPath = new File(dir, MOCK_DB_NAME).getPath();
    // load the driver so it registers itself with the DM
    Class.forName("org.h2.Driver");
    return DriverManager.getConnection("jdbc:h2:" + dbPath);
  }
  
  
  public static SqlSkipTable newAdaptor(File dir) throws ClassNotFoundException, SQLException {
    Connection con = newDatabase(dir);
    return SqlSkipTable.declareNewTable(con, LEDGER_TABLENAME);
  }
  
  
  /**
   * @return schema appropriate for H2 db
   */
  public static HashLedgerSchema newSchema(String protoname) {
    var schema = new HashLedgerSchema(protoname);
    return schema;
  }
  

}
