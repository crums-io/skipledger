/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;

import io.crums.model.CrumsException;

/**
 * Base exception in the <code>skipledger</code> module. Mostly thrown
 * as a result of data corruption.
 */
@SuppressWarnings("serial")
public class SldgException extends CrumsException {

  public SldgException(String message) {
    super(message);
  }

  public SldgException(Throwable cause) {
    super(cause);
  }

  public SldgException(String message, Throwable cause) {
    super(message, cause);
  }

  public SldgException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
