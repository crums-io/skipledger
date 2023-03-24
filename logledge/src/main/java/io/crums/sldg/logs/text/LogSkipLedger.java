/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;

/**
 * {@code SkipLedger} view of a {@linkplain LogHashRecorder}.
 */
public class LogSkipLedger extends SkipLedger {
  
  private final LogHashRecorder logRecorder;

  /**
   * 
   */
  public LogSkipLedger(LogHashRecorder logRecorder) {
    this.logRecorder = Objects.requireNonNull(logRecorder, "null log recorder");
  }
  
  
  /**
   * Returns the underlying recorder. Use to update state.
   */
  public LogHashRecorder getRecorder() {
    return logRecorder;
  }

  @Override
  public int hashWidth() {
    return SldgConstants.HASH_WIDTH;
  }

  @Override
  public String hashAlgo() {
    return SldgConstants.DIGEST.hashAlgo();
  }

  /**
   * Not supported. In order to append rows, use the {@linkplain #getRecorder()}
   * directly.
   */
  @Override
  public long appendRows(ByteBuffer entryHashes) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long size() {
    return logRecorder.getState().map(State::rowNumber).orElse(0L);
  }

  @Override
  public Row getRow(long rowNumber) {
    try {
      return logRecorder.getRow(rowNumber);
    
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

  @Override
  public List<Row> getRows(List<Long> rns) throws IllegalArgumentException {
    try {
      return logRecorder.getRows(rns);
    
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

  @Override
  public ByteBuffer rowHash(long rowNumber) {
    try {
      return logRecorder.getRowHash(rowNumber);
    
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

}
