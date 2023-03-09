/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


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
 * Note, depending on context, offsets and lineNos <em>may</em> be relative. The API
 * will make clear when they are not absolute.
 * </p>
 * 
 * @param frontier  hash frontier at the last row
 * @param eolOffset end-of-line (end-of-row) offset frontier row ends. This is one
 *                  beyond the last byte in the frontier source row (i.e. exclusive).
 * @param lineNo    the number of new line ({@code '\n'}) chars before {@code eolOffset}.
 */
public record State(HashFrontier frontier, long eolOffset, long lineNo) implements Serial {
  
  /** The empty state. All zero. */
  public final static State EMPTY = new State(HashFrontier.SENTINEL, 0, 0);
  
  
  public State {
    Objects.requireNonNull(frontier, "null hash frontier");
    if (eolOffset < 0)
      throw new IllegalArgumentException("eolOffset: " + eolOffset);
    if (lineNo < frontier.rowNumber())
      lineNo = frontier.rowNumber();
    if (lineNo > eolOffset)
      throw new IllegalArgumentException(
          "lineNo %d > eolOffset %d".formatted(lineNo, eolOffset));
  }
  
  
  public State(HashFrontier frontier, long eolOffset) {
    this(frontier, eolOffset, frontier.rowNumber());
  }
  
  /** @return {@code frontier().rowNumber()} */
  public long rowNumber() {
    return frontier.rowNumber();
  }
  
  /**
   * Tests the given {@code state} is equal to this one, ignoring differences in line number.
   */
  public boolean fuzzyEquals(State state) {
    return state != null && state.frontier.equals(frontier) && state.eolOffset == eolOffset;
  }
  
  
  /** Returns the [skip ledger] row hash. */
  public ByteBuffer rowHash() {
    return frontier.frontierHash();
  }
  
  
  //   S E R I A L

  @Override
  public int serialSize() {
    return 16 + frontier.serialSize();
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
    out.putLong(eolOffset).putLong(lineNo);
    return frontier.writeTo(out);
  }
  
  
  
  
  
  /**
   * Loads an instance from its serial representation.
   * 
   * @throws BufferUnderflowException if {@code in} has too few remaining bytes
   * @throws IllegalArgumentException if {@code in} contains nonsense
   */
  public static State loadSerial(ByteBuffer in)
      throws BufferUnderflowException, IllegalArgumentException {
    long eol = in.getLong();
    long lineNo = in.getLong();
    var front = HashFrontier.loadSerial(in);
    return new State(front, eol, lineNo);
  }
  
  

}



