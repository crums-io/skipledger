/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.sldg.cache.HashFrontier;

/**
 * The state of the log up to a given offset. This contains sufficient
 * information such that if the log is appended beyond the (EOL) offset,
 * then the new hash of the log can be calculated without having to replay
 * the log from the beginning.
 * <p>
 * Note, if rolling log files are used to model a single ledger, then the
 * <em>eol may less than the row no.</em>
 * </p>
 * 
 * @param frontier      hash frontier at the last row
 * @param eol           offset (exc) marking the end of the last row
 * 
 * @see #rowNo()
 */
public record LogState(HashFrontier frontier, long eol)
    implements Serial {
  
  
  

  /** The empty state. All zeroes. */
  public final static LogState EMPTY = new LogState(HashFrontier.SENTINEL, 0L);
  
  
  public LogState {
    if (eol < 0L)
      throw new IllegalArgumentException("eol: " + eol);
    Objects.requireNonNull(frontier, "null frontier");
  }
  
  
  /**
   * Returns the last row no.
   * 
   * @return {@code frontier().rowNo()}
   * 
   * @see #rowHash()
   */
  public long rowNo() {
    return frontier.rowNo();
  }
  
  
  /**
   * Returns the (commitment) hash of the last row.
   * 
   * @return {@code frontier().frontierHash()}
   */
  public ByteBuffer rowHash() {
    return frontier.frontierHash();
  }
  

  //   S E R I A L

  @Override
  public int serialSize() {
    return 8 + frontier.serialSize();
  }


  /**
   * <p>Writes the EOL offset and line number (8 bytes each)
   * followed by the serial representation of {@linkplain HashFrontier}.
   * </p>
   * {@inheritDoc}
   * @see HashFrontier#writeTo(ByteBuffer)
   */
  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    out.putLong(eol);
    return frontier.writeTo(out);
  }
  
  
  
  
  
  /**
   * Loads an instance from its serial representation.
   * 
   * @throws BufferUnderflowException if {@code in} has too few remaining bytes
   * @throws IllegalArgumentException if {@code in} contains nonsense
   */
  public static LogState load(ByteBuffer in)
      throws BufferUnderflowException, IllegalArgumentException {
    long eol = in.getLong();
    var front = HashFrontier.loadSerial(in);
    return new LogState(front, eol);
  }

}
