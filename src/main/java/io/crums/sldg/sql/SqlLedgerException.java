/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.sql;


import java.sql.SQLException;

import io.crums.sldg.SldgException;

/**
 * Unchecked ledger wrapper exception around checked (ugh) {@code SQLException}s.
 */
@SuppressWarnings("serial")
public class SqlLedgerException extends SldgException {

  public SqlLedgerException(String message) {
    super(message);
  }

  public SqlLedgerException(Throwable cause) {
    super(cause);
  }

  public SqlLedgerException(String message, Throwable cause) {
    super(message, cause);
  }
  
  
  /**
   * Returns the cause as an {@code SQLException}, if castable; {@code null}, o.w.
   */
  public SQLException sqlCause() {
    Throwable cause = getCause();
    return cause instanceof SQLException ? (SQLException) cause : null;
    
  }

}
