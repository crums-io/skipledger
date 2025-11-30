/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;


import java.sql.SQLException;

/**
 * A wrapped {@linkplain SQLException}.
 */
@SuppressWarnings("serial")
class SqlAppException extends AppException {

  public SqlAppException(SQLException cause) {
    super(cause);
  }

  public SqlAppException(String message, SQLException cause) {
    super(message, cause);
  }

  @Override
  public synchronized SQLException getCause() {
    return (SQLException) super.getCause();
  }

  /**
   * @param cause  instance of {@linkplain SQLException}; o.w. silently ignored
   */
  @Override
  public synchronized SqlAppException initCause(Throwable cause) {
    if (cause instanceof SQLException)
      super.initCause(cause);
    return this;
  }
  
  
  

}
