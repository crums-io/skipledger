/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import java.util.function.Predicate;

import io.crums.io.channels.ChannelUtils;
import io.crums.io.sef.SortedLongs;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.cache.Frontier;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.TableSalt;
import io.crums.util.TaskStack;

/**
 * Persistent version of {@linkplain StateHasher}.
 * 
 * <h2>TODO</h2>
 * <p>
 * Recovery from errors.
 * </p>
 * <ol>
 * <li>Ability to truncate frontiers (on errors)</li>
 * <li>Ability to truncate offsets, e.g. when a comment line was added</li>
 * </ol>
 */
public class LogHasher extends StateHasher implements AutoCloseable {

  
  
  /** Default replay limit.
   * @see #replayLimit() */
  public final static int DEFAULT_REPLAY = 10;

  
  private final ByteBuffer workBuff = ByteBuffer.allocate(32);
  

  /** EOL (<em>!</em>) offsets. */
  private final SortedLongs rowOffsets;
  private final FrontierTable frontiers;
  private final long rnMask;
  
  
  
  
  /**
   * 
   */
  public LogHasher(
      TableSalt salt, Predicate<ByteBuffer> commentFilter, String tokenDelimiters,
      SortedLongs rowOffsets, FrontierTable frontiers, int rnExponent) {
    
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
          "frontiers (size=%d) != offsets (size=%d); table sizes mismatch"
          .formatted(fsize, osize));
  }
  
  
  /** Closes the backing storage files (row hashes and offsets). */
  public void close() {
    try (var closer = new TaskStack()) {
      closer.pushCall(() -> { rowOffsets.close(true); return null; });
      closer.pushClose(frontiers);
    }
    
  }
  
  
  
  
  public HashFrontier update(SeekableByteChannel logChannel) throws IOException {
    long fsize = frontiers.size();
    HashFrontier state;
    if (fsize < replayLimit()) {
      state = play(logChannel);
    } else {
      state = checkFirstBlock(logChannel);
      // set the state to one before the last block recorded
      long index = fsize - 2;
      state = readFrontier(index);
      // position the logChannel to the end of this one-before-last block
      // this is why we record the EOL (not start) offset for the row, btw
      long eolOff = rowOffsets.get(index);
      logChannel.position(eolOff);
      long lineNoEst = state.rowNumber() - 1; // minimum (likely greater)
      state = play(logChannel, state, eolOff, lineNoEst);
    }
    return state;
  }
  

  
  
  
  public HashFrontier play(SeekableByteChannel logChannel) throws IOException {
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
  
 
  
  public HashFrontier checkFirstBlock(SeekableByteChannel logChannel) throws IOException {
    logChannel.position(0);
    long eof = rowOffsets.get(0);
    ReadableByteChannel truncated = ChannelUtils.truncate(logChannel, eof);
    return play(truncated);
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
          HashConflictException,
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
        throw new HashConflictException(
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
  
  
  
  

}
















