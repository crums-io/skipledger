/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql;


import java.nio.channels.Channel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

import io.crums.sldg.src.sql.SqlLedger.Config;
import io.crums.sldg.salt.TableSalt;
import io.crums.sldg.src.SaltScheme;
import io.crums.sldg.src.sql.config.DbConnection;
import io.crums.sldg.src.sql.config.LedgerDef;
import io.crums.sldg.src.sql.config.SaltSeed;
import io.crums.util.TaskStack;

/**
 * 
 */
public class DbSession implements Channel {
  
  public static DbSession newInstance(DbConnection dbConn)
      throws SqlLedgerException {
    

    if (dbConn.driverClass().isPresent()) {
      String driver = dbConn.driverClass().get();
      try {
        Class.forName(driver);
      } catch (ClassNotFoundException cnfx) {
        throw new SqlLedgerException(
            "Driver class %s not found".formatted(driver), cnfx);
      }
    }
    
    try {
      
      String url = dbConn.url();
      Connection con;
      if (dbConn.creds().isPresent()) {
        var creds = dbConn.creds().get();
        con = DriverManager.getConnection(
            url, creds.username(), creds.password());
      } else
        con = DriverManager.getConnection(url);
      
      con.setReadOnly(true);
      
      return new DbSession(con);
      
    
    } catch (SQLException sx) {
      throw new SqlLedgerException(sx);
    }
  }
  
  protected final Connection connection;

  
  protected DbSession(Connection connection) throws SQLException {
    this.connection = connection;
    if (connection.isClosed())
      throw new IllegalArgumentException(
          "database connection closed: " + connection);
    if (!connection.isReadOnly())
      Constants.logWarning(
          "read/write database connection (will only read): " + connection);
  }

  
  @Override
  public void close() {
    try {
      connection.close();
    } catch (SQLException sx) {
      Constants.logWarning(
          "ignoring error on closing database connection (%s): %s"
          .formatted(connection, sx));
    }
  }

  @Override
  public boolean isOpen() throws SqlLedgerException {
    try {
      return !connection.isClosed();
    } catch (SQLException sx) {
      throw new SqlLedgerException(sx);
    }
  }
  
  
  /**
   * Returns a new {@linkplain SqlLedger} instance using the given
   * ledger definition, salt-scheme, and salt. Since the returned
   * object uses this instance's database connection, it is "bound"
   * to this instance.
   * 
   * @param ledgerDef   not {@code null}
   * @param saltScheme  not {@code null}
   * @param salt        not {@code null}; must be present if
   *                    {@code saltscheme.}{@link SaltScheme#hasSalt() hasSalt()}
   * 
   * @throws SqlLedgerException
   *         if there is an SQL error in {@code ledgerDef}
   * 
   */
  public SqlLedger openLedger(
      LedgerDef ledgerDef,
      SaltScheme saltScheme,
      Optional<SaltSeed> salt)
          throws SqlLedgerException {
    
    if (saltScheme.hasSalt() && salt.isEmpty())
      throw new IllegalArgumentException(
          "salt-scheme %s has salt but salt-seed is not present"
          .formatted(saltScheme));
    
    PreparedStatement sizeQuery = null;
    PreparedStatement rowQuery = null;
    
    try (var closeOnFail = new TaskStack()) {
      sizeQuery = connection.prepareStatement(ledgerDef.sizeQuery());
      closeOnFail.pushClose(sizeQuery);
      rowQuery = connection.prepareStatement(ledgerDef.rowByNoQuery());
      closeOnFail.pushClose(rowQuery);
      
      TableSalt shaker = salt.map(s -> new TableSalt(s.seed())).orElse(null);
      Config conf =
          new Config(sizeQuery, rowQuery, saltScheme, shaker,
              ledgerDef.maxBlobSize());
      
      
      SqlLedger ledger = new SqlLedger(conf);
      closeOnFail.clear();
      return ledger;
      
      
    } catch (SQLException sx) {
      String msg = "bad %s-query in %s: %s".formatted(
          sizeQuery == null ? "size" : "row",
          ledgerDef,
          sx);
      throw new SqlLedgerException(msg, sx);
    }
  }
  
  

}
