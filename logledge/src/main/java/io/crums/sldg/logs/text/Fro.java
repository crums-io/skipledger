/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.nio.ByteBuffer;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.sldg.Row;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.cache.HashFrontier;

/**
 * <p>
 * A frontiered, <em>skip ledger</em> row with offset information.
 * (Recall a skip ledger row only records hash information.)
 * </p>
 * <h2>Purpose</h2>
 * <p>
 * This is used to record the state of the log up to a certain row (line)
 * number. It's similar to the {@linkplain State} record, in that
 * it lets you play the log forward starting from the recorded offset.
 * But it also offers a minimal sanity check allowing the contents of the
 * last row of the log to be replayed and checked.
 * </p>
 * <h2>Components</h2>
 * <p>
 * The structure actually contains hash information both at its
 * {@linkplain #rowNumber() row number} and the row before it. Here's a bill
 * of parts.
 * </p>
 * <h3>Pre-state</h3>
 * <p>
 * The {@linkplain #preState()} records the {@linkplain State state} of the
 * log at the row <em>before</em> this row number. We can replay the last
 * row using this information.
 * </p>
 * <h3>Input hash</h3>
 * <p>
 * The input hash (computed from the source) for this row number. Using the
 * pre-state and this information, an instance can compute both the 
 * {@linkplain #rowHash() row's hash}, and the {@linkplain State state} at this
 * row number--as well as a full skip ledger row. On replaying the last row,
 * the computed hash of the source row must match this value. 
 * </p>
 * <h3>Offsets and line numbers</h3>
 * <p>
 * EOL offsets and line numbers are recorded both for this row <em>and</em> the
 * previous row (thru the pre-state).
 * </p>
 */
public class Fro implements Serial {
  
  
  private final State preState;
  private final ByteBuffer inputHash;
  private final long eolOffset;
  private final long lineNo;

  
  /**
   * @param preState  the state of the log immediately before this row
   * @param inputHash the hash of the row (of the line of text), 32-bytes (do not modify!)
   * @param eolOffset EOL offset for this row
   * @param lineNo    line number for this row
   */
  public Fro(State preState, ByteBuffer inputHash, long eolOffset, long lineNo) {
    this.preState = Objects.requireNonNull(preState, "null pre state");
    this.inputHash = inputHash.slice();
    if (this.inputHash.remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException("inputHash (remaining): " + inputHash);
    this.eolOffset = eolOffset;
    this.lineNo = lineNo;
    
    if (eolOffset <= preState.eolOffset())
      throw new IllegalArgumentException(
          "EOL offset %d <= pre state EOL offset %d"
          .formatted(eolOffset, preState.eolOffset()));
    if (lineNo > eolOffset)
      throw new IllegalArgumentException(
          "EOL offset %d < line no. %d".formatted(eolOffset, lineNo));
  }
  
  
  /**
   * Returns {@code true} iff all members are equal. {@inheritDoc}
   */
  public final boolean equals(Object o) {
    return
        o instanceof Fro fro &&
        fro.preState.equals(preState) &&
        fro.inputHash.equals(inputHash) &&
        fro.eolOffset == eolOffset &&
        fro.lineNo == lineNo;
  }
  
  
  /** The hashcode is dependent on every field except the pre-state. {@inheritDoc}*/
  public final int hashCode() {
    return Long.hashCode(eolOffset + 31*lineNo) * 31 + inputHash.hashCode();
  }
  
  /**
   * @return {@code preState().rowNumber() + 1}
   */
  public final long rowNumber() {
    return preState.rowNumber() + 1;
  }
  
  
  /**
   * The hash of the row (32 bytes).
   */
  public final ByteBuffer rowHash() {
    return frontier().frontierHash();
  }
  
  
  /**
   * The ending offset in the log (exclusive) for this row.
   */
  public final long eolOffset() {
    return eolOffset;
  }
  
  
  /**
   * Line number in the log for this row.
   */
  public final long lineNo() {
    return lineNo;
  }
  
  
  /**
   * The hash of the source row (line), from which the
   * skip ledger's {@linkplain #rowHash() row hash} is derived.
   */
  public final ByteBuffer inputHash() {
    return inputHash.asReadOnlyBuffer();
  }
  
  
  /**
   * The state of the log <em>before</em> this row.
   */
  public final State preState() {
    return preState;
  }
  
  
  /**
   * The hash frontier at this row.
   * 
   * @return {@code preState().frontier().nextFrontier(inputHash())}
   */
  public final HashFrontier frontier() {
    return preState.frontier().nextFrontier(inputHash());
  }
  
  
  /**
   * State at this row.
   * 
   * @return {@code new State(frontier(), eolOffset(), lineNo())}
   */
  public final State state() {
    return new State(frontier(), eolOffset, lineNo);
  }
  
  
  /**
   * The skip ledger row.
   * 
   * @return {@code preState().frontier().nextRow(inputHash())}
   */
  public Row row() {
    return preState.frontier().nextRow(inputHash());
  }



  @Override
  public int serialSize() {
    return 8 + 8 + SldgConstants.HASH_WIDTH + preState.serialSize();
  }



  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    out.putLong(lineNo);
    out.putLong(eolOffset);
    out.put(inputHash());
    return preState.writeTo(out);
  }
  
  
  /**
   * Loads an instance from its serial version.
   * 
   * @see #writeTo(ByteBuffer)
   */
  public static Fro loadSerial(ByteBuffer in) {
    long lineNo = in.getLong();
    long eolOffset = in.getLong();
    byte[] inputHash = new byte[SldgConstants.HASH_WIDTH];
    in.get(inputHash);
    State preState = State.loadSerial(in);
    return new Fro(preState, ByteBuffer.wrap(inputHash), eolOffset, lineNo);
  }

}




















