/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.sql.Connection;

import io.crums.sldg.ledgers.HashLedger;
import io.crums.sldg.ledgers.Ledger;
import io.crums.sldg.ledgers.SourceLedger;
import io.crums.sldg.src.TableSalt;
import io.crums.util.TaskStack;
import io.crums.util.ticker.Ticker;

/**
 * A {@linkplain Ledger} backed by relational databases.
 * 
 * @see #loadInstance(Config)
 * @see Config
 */
public class SqlLedger extends Ledger {
  
  
  public static SqlLedger declareNewInstance(Config config) {
    try (TaskStack closeOnFail = new TaskStack()) {
      // first get the source ledger
      Connection con = config.getSourceConnection();
      closeOnFail.pushClose(con);
      TableSalt shaker = config.getSourceSalt();
      var srcLedger = config.getSourceBuilder().build(con, shaker);
      
      // ok, now create the hash ledger tables
      
      if (config.dedicatedSourceCon()) {
        con = config.getHashConnection();
        closeOnFail.pushClose(con);
      }
      
      var hashLedger =
          SqlHashLedger.declareNewInstance(con, config.getHashLedgerSchema());
      
      SqlLedger ledger = new SqlLedger(srcLedger, hashLedger, null, true);
      
      closeOnFail.clear();
      return ledger;
    }
  }
  
  
  
  
  /**
   * Loads an existing instance using the given configuration data. By default, the
   * hash-ledger is trusted.
   * 
   * @return {@code loadInstance(config, null, true)}
   * @see #loadInstance(Config, Ticker, boolean)
   */
  public static SqlLedger loadInstance(Config config) {
    return loadInstance(config, null, true);
  }
  
  /**
   * Loads an existing instance using the given configuration data.
   * 
   * @param progress        optional progress ticker (may be {@code null}), ticked once per row access
   * @param trustHashLedger if {@code false}, the skip-ledger is checked for self-consistency
   * 
   */
  public static SqlLedger loadInstance(Config config, Ticker progress, boolean trustHashLedger) {
    SqlLedger ledger;
    
    try (TaskStack closeOnFail = new TaskStack()) {
      Connection con = config.getSourceConnection();
      closeOnFail.pushClose(con);
      var builder = config.getSourceBuilder();
      var shaker = config.getSourceSalt();
      var srcLedger = builder.build(con, shaker);
      if (config.dedicatedSourceCon()) {
        con = config.getHashConnection();
        closeOnFail.pushClose(con);
      }
      String prefix = config.getHashTablePrefix();
      var hashLedger = new SqlHashLedger(prefix, con);
      ledger = new SqlLedger(srcLedger, hashLedger, progress, trustHashLedger);
      closeOnFail.clear();
    }
    return ledger;
  }
  
  


  
  private SqlLedger(SourceLedger srcLedger, HashLedger hashLedger, Ticker progress, boolean trustHashLedger) {
    super(srcLedger, hashLedger, progress, trustHashLedger);
  }

}
