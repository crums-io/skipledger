/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.Objects;

import io.crums.io.sef.Alf;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.cache.Frontier;
import io.crums.sldg.cache.HashFrontier;
import io.crums.util.TaskStack;

/**
 * <p>Records state information at fixed row intervals (the block). The fixed interval
 * is configurable, but must equal a power of 2. Every row numbered a muliple of the
 * interval has its {@linkplain State ledger state} information saved.
 * </p>
 * <h2>Design</h2>
 * <p>
 * This uses 2 files: one to store frontier row hashes (32 bytes each),
 * the other to store end-of-row (EOL) offsets. Because the block sizes are a power
 * of 2, the {@linkplain HashFrontier} for every row numbered at the block
 * boundary can be looked up. Combined with the row's end-of-row offset, the log
 * can be played forward starting at the beginning of every block.
 * </p><p>
 * Packaged as a {@linkplain ContextedHasher.Context} implementation so it
 * can be mixed in with other capabilities.
 * </p>
 */
public class BlockRecorder implements
    ContextedHasher.Context, Channel {

  private final ByteBuffer workBuff = ByteBuffer.allocate(32);
  

  private final FrontierTable frontiers;
  /** EOL (<em>!</em>) offsets. The offsets rows end at (exc). */
  private final Alf rowOffsets;
  private final long rnMask;
  
  
  
  /**
   * Creates a new instance with the given backing storage argument. The
   * {@code dex} controls how often row hashes and offsets are recorded:
   * zero (2<sup>0</sup>) sets the block size to 1, i.e. every row.
   * 
   * @param frontiers   fixed-width table
   * @param rowOffsets  compact, ascending-longs format
   * @param dex         delta exponent. {@code 0 <= dex <= 62}
   */
  public BlockRecorder(
      FrontierTable frontiers, Alf rowOffsets, int dex) {
    
    this.frontiers = Objects.requireNonNull(frontiers, "null frontier table");
    this.rowOffsets = Objects.requireNonNull(rowOffsets, "null row offsets (Alf)");

    if (dex < 0 || dex > 62)
      throw new IllegalArgumentException("dex (delta exponent) out of bounds: " + dex);
    
    this.rnMask = (1L << dex) - 1;
    
    long fsize = frontiers.size();
    long osize = rowOffsets.size();
    if (fsize < osize)
      throw new IllegalArgumentException(
          "frontiers (size=%d) < offsets (size=%d); table sizes mismatch"
          .formatted(fsize, osize));
  }
  
  
  

  /** Closes the backing storage files (row hashes and offsets). */
  @Override
  public void close() {
    try (var closer = new TaskStack()) {
      closer.pushClose(frontiers);
      closer.pushCall(() -> { rowOffsets.close(true); return null; });
    }
  }
  
  
  @Override
  public boolean isOpen() {
    return rowOffsets.isOpen();
  }

  
  /**
   * Returns the row number delta between recorded blocks.
   */
  public final long rnDelta() {
    return rnMask + 1;
  }
  
  
  /**
   * Returns the delta exponent. The following relationship
   * holds: 2<sup>dex()</sup> == rnDelta()
   */
  public final long dex() {
    return Long.numberOfTrailingZeros(rnDelta());
  }
  
  
  /**
   * Returns the row number of the last block recorded.
   * 
   * @return {@code frontiersRecorded() * rnDelta()}
   */
  public long lastRn() {
    return frontiers.size() * rnDelta();
  }
  
  /**
   * Observes a ledgered line.
   * 
   * <h4>Side Effects</h4>
   * <p>
   * If the row number (as specified by {@code frontier}) is a multiple of
   * {@linkplain #rnDelta()}, then both the row hash and the EOL offset into the
   * log stream are either appended, or if already recorded, checked for
   * conflicts.
   * </p>
   * 
   * @param frontier      the state of the ledger up to the given row
   * @param offset        the <em>start</em> offset of the row's source text (not used)
   * @param eolOff        the <em>EOL</em> offset of the rows's source text
   * @param lineNo        zero-based line number (not used because this info is mostly contextual)
   * 
   * @throws HashConflictException
   *          if the row's hash conflicts with what's already recorded
   * @throws OffsetConflictException
   *          if the row's starting offset conflicts with that already recorded
   * @throws IllegalArgumentException
   *          if a frontier hash or (EOL) offset (that should be recorded) has been
   *          skipped                 
   */
  @Override
  public void observeLedgeredLine(
      HashFrontier frontier, long offset, long eolOff, long lineNo)
          throws
          IOException,
          RowHashConflictException,
          OffsetConflictException,
          IllegalArgumentException {
    
    long rn = frontier.rowNumber();
    if ((rn & rnMask) != 0)
      return;
    
    long index = (rn / rnDelta()) - 1;
    
    long fc = frontiers.size();
    long oc = rowOffsets.size();
    
    if (index > fc)
      throw new IllegalArgumentException(
          "skipped frontier: count %d; index %d".formatted(fc, index));
    if (index > oc)
      throw new IllegalArgumentException(
          "skipped offset: count %d; index %d".formatted(oc, index));
    
    if (index ==  fc) {
      frontiers.append(frontier.frontierHash());
      
    } else {
      
      var rowHash = frontiers.get(index);
      if (!rowHash.equals(frontier.frontierHash()))
        throw new RowHashConflictException(rn,
            "at row [%d] (%d:%d)".formatted(rn, lineNo, offset));
      
    }
    
    if (index == oc) {
      
      rowOffsets.addNext(eolOff, workBuff);
    
    } else {
      
      long eol = rowOffsets.get(index, workBuff);
      if (eol != eolOff)
        throw new OffsetConflictException(
            rn, eol, eolOff,
            "row [%d] recorded EOL offset was %d; given EOL offset is %d (%d:%d)"
            .formatted(rn, eol, eolOff, lineNo, offset));
    }
  }
  

  /**
   * Uses the already recorded information to jump ahead and return the
   * {@linkplain State state} closest (but less than) the given row number
   * {@code rn}, if any.
   */
  @Override
  public State nextStateAhead(State state, long rn) throws IOException {
    long rnDelta = rnDelta();
    long index = ((rn - 1) / rnDelta) - 1;
    if (index == -1 || index >= rowOffsets.size())
      return state;
    long lookAheadRn = (1 + index) * rnDelta;
    if (lookAheadRn <= state.rowNumber())
      return state;
    
    long endOffset = rowOffsets.get(index);
    var frontier = readFrontier(index);
    return new State(frontier, endOffset);
  }

  
  private HashFrontier readFrontier(long index) {
    final long rnDelta = rnDelta();
    final long rn = (index + 1) * rnDelta;
    ByteBuffer[] levelHashes = new ByteBuffer[Frontier.levelCount(rn)];
    levelHashes[0] = frontiers.get(index);
    long lastRn = rn;
    for (int level = 1; level < levelHashes.length; ++level) {
      long levelRn = Frontier.levelRowNumber(rn, level);
      if (levelRn == lastRn)
        levelHashes[level] = levelHashes[level - 1];
      else {
        lastRn = levelRn;
        levelHashes[level] = frontiers.get((levelRn / rnDelta) - 1);
      }
    }
    return new HashFrontier(rn, levelHashes);
  }
  


  /**
   * Returns the hash frontier stored at the given block {@code index}.
   * The returned frontier's row number is equal to {@code (index + 1) * rnDelta()}.
   * 
   * @param index block index: {@code 0 <= index < frontiersRecorded()}
   */
  public HashFrontier frontier(long index) {
    Objects.checkIndex(index, frontiers.size());
    return readFrontier(index);
  }
  
  
  /**
   * Returns the ending offset (exclusive) of the block recorded for the specified
   * block.
   * 
   * @param index block index: {@code 0 <= index < endOffsetsRecorded()}
   */
  public long endOffset(long index) throws IOException {
    return rowOffsets.get(index);
  }
  
  
  /** Returns the number of frontier row hashes (blocks) recorded. */
  public long frontiersRecorded() {
    return frontiers.size();
  }
  
  /** Returns the number of ending offsets for row blocks recorded. */
  public long endOffsetsRecorded() {
    return rowOffsets.size();
  }
  
  
  /** Trims the number of row offsets recorded. */
  public void trimOffsetsRecorded(long count) throws IOException {
    rowOffsets.trimSize(count);
  }
  
  /** Trims the number of frontier row hashes recorded. */
  public void trimFrontiersRecorded(long count) throws IOException {
    frontiers.trimSize(count);
  }
  
  
  /**
   * Trims the number of blocks recorded (both frontier hashes and ending
   * offsets).
   */
  public void trimRecorded(long count) throws IOException {
    trimOffsetsRecorded(count);
    trimFrontiersRecorded(count);
  }
  
}