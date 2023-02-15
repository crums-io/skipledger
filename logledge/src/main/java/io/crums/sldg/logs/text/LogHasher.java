/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import java.util.function.Predicate;

import io.crums.io.channels.ChannelUtils;
import io.crums.io.sef.Alf;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.cache.Frontier;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.TableSalt;
import io.crums.util.TaskStack;

/**
 * Persistent version of {@linkplain StateHasher}. Records row hashes
 * and end-of-row offsets at fixed (block) intervals of row numbers.
 * 
 */
public class LogHasher extends StateHasher implements Channel {

  
  
  /** Default replay limit.
   * @see #replayLimit() */
  public final static int DEFAULT_REPLAY = 10;

  
  private final ByteBuffer workBuff = ByteBuffer.allocate(32);
  

  /** EOL (<em>!</em>) offsets. */
  private final Alf rowOffsets;
  private final FrontierTable frontiers;
  private final long rnMask;
  
  
  
  
  /**
   * 
   */
  public LogHasher(
      TableSalt salt, Predicate<ByteBuffer> commentFilter, String tokenDelimiters,
      Alf rowOffsets, FrontierTable frontiers, int rnExponent) {
    
    super(salt, commentFilter, tokenDelimiters);
    
    this.rowOffsets = Objects.requireNonNull(rowOffsets, "null offsets table");
    this.frontiers = Objects.requireNonNull(frontiers, "null frontier table");
    
    if (rnExponent < 0 || rnExponent > 62)
      throw new IllegalArgumentException("rnExponent out of bounds: " + rnExponent);
    
    this.rnMask = (1L << rnExponent) - 1;
    
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
      closer.pushCall(() -> { rowOffsets.close(true); return null; });
      closer.pushClose(frontiers);
    }
  }
  
  
  @Override
  public boolean isOpen() {
    return rowOffsets.isOpen();
  }
  
  
  /**
   * 
   * @param rn
   * @param logChannel
   */
  public FrontieredRow getFrontieredRow(long rn, SeekableByteChannel logChannel)
      throws IOException {
    SkipLedger.checkRealRowNumber(rn);
    
    State state;
    {
      long preRn = rn - 1;
      long indexSize = rowOffsets.size();
      long delta = rnDelta();
      
      
      if (indexSize == 0 || preRn < delta) {
        state = State.EMPTY;
      } else {
        long index = (preRn > indexSize * delta ? indexSize : (preRn / delta)) - 1;
        HashFrontier frontier = frontier(index);
        long eol = rowOffsets.get(index);
        state = new State(frontier, eol);
      }
    }
    
    return getFrontieredRow(rn, state, logChannel);
  }
  
  
  
  
  
  
  
  
  /**
   * Updates and returns the state of the given log stream.
   * 
   * @param logChannel  seekable channel
   *
   * @throws RowHashConflictException
   *    if the hash of a given row (ledgered line) conflicts with what's
   *    already recorded
   * @throws OffsetConflictException
   *    if the EOL offset of a given row (ledgered line) conflicts with what's
   *    already recorded
   */
  public State update(SeekableByteChannel logChannel)
      throws IOException, RowHashConflictException, OffsetConflictException {
    long fsize = frontiers.size();
    long osize = rowOffsets.size();
    
    State state;
    if (fsize < replayLimit() || osize < fsize) {
      state = play(logChannel);
    } else {
      state = checkFirstBlock(logChannel);
      // set the state to one before the last block recorded
      long index = fsize - 2;
      HashFrontier frontier = readFrontier(index);
      // position the logChannel to the end of this one-before-last block
      // this is why we record the EOL (not start) offset for the row, btw
      long eolOff = rowOffsets.get(index);
      logChannel.position(eolOff);
      long lineNoEst = state.rowNumber() - 1; // minimum (likely greater)
      state = play(logChannel, new State(frontier, eolOff, lineNoEst));
    }
    return state;
  }
  

  
  
  
  public State play(SeekableByteChannel logChannel) throws IOException {
    return play((ReadableByteChannel) logChannel.position(0));
  }
  
  
  
  
  
  /**
   * Returns the replay limit: if there are more than this many blocks already
   * tracked by the (persistent) instance, then only the first block is checked;
   * otherwise, the log is replayed. Any conflicts are reported as errors
   * ({@linkplain HashConflictException} for differences in content,
   * {@linkplain IllegalStateException} for differences in offset).
   * 
   * @return &ge; 2. Default is {@code DEFAULT_REPLAY}
   * @see #DEFAULT_REPLAY
   */
  protected int replayLimit() {
    return DEFAULT_REPLAY;
  }
  
 
  /**
   * Checks the first block recorded (by replaying the stream) and returns its
   * state. 
   * 
   * @param logChannel  seekable stream
   * @return the state immediately after the first block
   * @throws IllegalStateException
   *    if the first block has yet to be recorded
   * @throws RowHashConflictException
   *    if the hash of the first row (ledgered line) conflicts with what's
   *    already recorded
   * @throws OffsetConflictException
   *    if the EOL offset of the first row (ledgered line) conflicts with what's
   *    already recorded
   */
  public State checkFirstBlock(SeekableByteChannel logChannel) throws IOException {
    if (rowOffsets.isEmpty())
      throw new IllegalStateException("empty row offsets, nothing to check");
    logChannel.position(0);
    long eof = rowOffsets.get(0);
    ReadableByteChannel truncated = ChannelUtils.truncate(logChannel, eof);
    return play(truncated);
  }
  

  
  public State recoverFromConflict(
      OffsetConflictException conflict, SeekableByteChannel logChannel)
          throws IOException {
    
    long newSize = trimmedSize(conflict.rowNumber());
    
    trimOffsetsRecorded(newSize);
    
    return update(logChannel);
  }
  
  
  private long trimmedSize(long rn) {
    return rn / rnDelta() - 1;
  }
  

  
  public State recoverFromConflict(
      RowHashConflictException conflict, SeekableByteChannel logChannel)
          throws IOException {
    
    long newSize = trimmedSize(conflict.rowNumber());
    
    trimRecorded(newSize);
    
    return update(logChannel);
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
  protected void observeLedgeredLine(
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
  
  
  
  public long frontiersRecorded() {
    return frontiers.size();
  }
  
  public long endOffsetsRecorded() {
    return rowOffsets.size();
  }
  
  
  public HashFrontier frontier(long index) {
    Objects.checkIndex(index, frontiers.size());
    return readFrontier(index);
  }
  
  
  public long endOffset(long index) {
    try {
      return endOffsetChecked(index);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on index %d: %s".formatted(index, iox.getMessage()), iox);
    }
  }
  
  
  public long endOffsetChecked(long index) throws IOException {
    return rowOffsets.get(index);
  }
  
  
  
  public void trimOffsetsRecorded(long count) {
    try {
      rowOffsets.trimSize(count);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on " + count + ": " +iox.getMessage(), iox);
    }
  }
  
  
  public void trimFrontiersRecorded(long count) {
    frontiers.trimSize(count);
  }
  
  
  public void trimRecorded(long count) {
    trimOffsetsRecorded(count);
    trimFrontiersRecorded(count);
  }
  
  
  
  /**
   * Returns the row number delta between recorded blocks.
   */
  public final long rnDelta() {
    return rnMask + 1;
  }
  
  
  public final long rnExponent() {
    return Long.numberOfTrailingZeros(rnDelta());
  }
  
  
  public long lastRn() {
    return frontiers.size() * rnDelta();
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
  
  
  
  
  @Override
  protected State nextStateAhead(State state, long rn) throws IOException {
    long rnDelta = rnDelta();
    long index = ((rn - 1) / rnDelta) - 1;
    if (index == -1 || index >= rowOffsets.size())
      return state;
    long lookAheadRn = 1 + index * rnDelta;
    if (lookAheadRn <= state.rowNumber())
      return state;
    
    long endOffset = rowOffsets.get(index);
    var frontier = readFrontier(index);
    return new State(frontier, endOffset);
  }
  

}
















