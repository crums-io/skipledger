/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src.sql;

import java.sql.SQLException;

/**
 * Usually wraps an {@linkplain SQLException}.
 */
@SuppressWarnings("serial")
public class SqlLedgerException extends RuntimeException {

  public SqlLedgerException() {
  }

  public SqlLedgerException(String message) {
    super(message);
  }

  public SqlLedgerException(Throwable cause) {
    super(cause);
  }

  public SqlLedgerException(String message, Throwable cause) {
    super(message, cause);
  }

}
