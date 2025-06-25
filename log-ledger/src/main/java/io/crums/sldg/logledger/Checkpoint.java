/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.cache.HashFrontier;
import io.crums.util.IntegralStrings;

/**
 * The "state" of a parsed log file after <em>n</em> rows (ledgerable lines)
 * have been {@linkplain LineParser#parse() parse}d. This records hashes and
 * EOL offsets of the last 2 rows. (Instances of this class cannot
 * represent the state of the empty log.)
 * 
 * <h2>Purpose</h2>
 * <p>
 * Checkpoints record the state of a log up to a certain row (line)
 * number and allow subsequent parses of the log to begin from the
 * offset in recorded in the checkpoint. This can be useful, if the
 * log is still being appended to. Additionally, when checkpoints are
 * recorded at regular intervals, they can serve as a kind of coarse-grained
 * index into the log's rows and their skipledger hashes.
 * </p><p>
 * With a live log stream, one may reach an EOF condition during parsing
 * before a new-line char ({@code '\n'} terminates the last line. This design
 * allows one to detect such conditions.
 * </p>
 * <h2>Components</h2>
 * <p>
 * The structure actually contains hash information both at its
 * {@linkplain #rowNo() row no.} and the row before it. Here's a bill
 * of parts.
 * </p>
 * <h3>Pre-state</h3>
 * <p>
 * The {@linkplain #preState()} records the {@linkplain LogState} of the
 * log at the row <em>before</em> this row no. We can replay the last
 * row using this information.
 * </p>
 * <h3>Input hash</h3>
 * <p>
 * The input hash (computed from the source) for this row number. Using the
 * pre-state and this information, an instance can compute the
 * {@linkplain LogState} at this row no. On reparsing the last (row) line,
 * the computed hash of the source row must match this value.
 * </p>
 * <h3>EOL offsets</h3>
 * <p>
 * EOL offsets are recorded both for this row <em>and</em> the
 * previous row (thru the pre-state).
 * </p>
 * 
 */
public class Checkpoint implements Serial {
  
  private final LogState preState;
  private final ByteBuffer inputHash;
  private final long eol;

  
  /**
   * 
   * @param preState    the log's state <em>before</em> the current row no.
   *                    (with row no. one less than the current row no.)
   * @param inputHash   the input-hash for the current row
   * @param eol         eol offset (exc) where the last ledgered line (row) ends
   *                    (byte{@code [eol-1]} is {@code '\n'} unless EOF)
   */
  public Checkpoint(
      LogState preState, ByteBuffer inputHash, long eol) {
    
    this.preState = preState;
    this.inputHash = inputHash.slice();
    this.eol = eol;

    if (eol < 0L)
      throw new IllegalArgumentException("eol: " + eol);
    if (this.inputHash.remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException("inputHash (remaining): " + inputHash);
  }
  
  
  /**
   * Returns the <em>previous</em> state .
   */
  public final LogState preState() {
    return preState;
  }
  

  /**
   * Returns the hash frontier at the last row. Lazy evaluation.
   * 
   * @return {@code preState().frontier().nextFrontier(inputHash())}
   */
  public final HashFrontier frontier() {
    return preState.frontier().nextFrontier(inputHash());
  }
  
  
  /**
   * Returns the state at the last row. Lazy evaluation.
   * 
   * @return {@code new LogState(frontier(), eol(), lineNo())}
   */
  public final LogState state() {
    return new LogState(frontier(), eol);
  }
  
  
  /**
   * Returns the last row's input-hash.
   * 
   * @return a new read-only view
   */
  public final ByteBuffer inputHash() {
    return inputHash.asReadOnlyBuffer();
  }
  
  
  /**
   * Returns EOL offset of the last row.
   * 
   * @return &gt; 0
   */
  public final long eol() {
    return eol;
  }
  
  /**
   * Returns the EOL offset of the row before last.
   * 
   * @return &ge; 0
   */
  public final long prevEol() {
    return preState.eol();
  }
  
  
  /**
   * Returns the row no. of the last row.
   * 
   * @return &ge; 1 ({@code preState().rowNo() + 1})
   */
  public final long rowNo() {
    return preState.rowNo() + 1;
  }
  
  /** Returns {@code true} iff all members are equal. */
  public final boolean equals(Object o) {
    return
        o == this ||
        o instanceof Checkpoint other &&
        other.eol == eol &&
        other.preState.equals(preState) &&
        other.inputHash.equals(inputHash);
  }
  
  /**
   * Consistent with {@linkplain #equals(Object)}.
   * Computed from row no. and eol, only.
   */
  @Override
  public final int hashCode() {
    return Long.hashCode(31L * rowNo() + eol);
  }
  
  
  @Override
  public String toString() {
    String inHash = IntegralStrings.toHex(inputHash().limit(3));
    String hash = IntegralStrings.toHex(frontier().frontierHash().limit(3));
    return "[%d: (%d-%d) %s %s]".formatted(rowNo(), prevEol(), eol(), inHash, hash);
  }

  
  //      S  E  R  I  A  L

  @Override
  public int serialSize() {
    return 8 + SldgConstants.HASH_WIDTH + preState.serialSize();
  }


  @Override
  public ByteBuffer writeTo(ByteBuffer out) throws BufferOverflowException {
    out.putLong(eol).put(inputHash());
    return preState.writeTo(out);
  }
  
  
  /**
   * Loads and returns an instance from its serial representation.
   * 
   * @param in          contents not copied (do not modify!)
   */
  public static Checkpoint load(ByteBuffer in) {
    long eol = in.getLong();
    var inputHash = BufferUtils.slice(in, SldgConstants.HASH_WIDTH);
    var preState = LogState.load(in);
    return new Checkpoint(preState, inputHash, eol);
  }

}























