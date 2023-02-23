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
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.TableSalt;

/**
 * Persistent version of {@linkplain StateHasher}. Records row hashes
 * and end-of-row offsets at fixed (block) intervals of row numbers.
 * <h2>TODO</h2>
 * <p>Find me a better name.</p>
 */
public class LogHasher extends ContextedHasher implements Channel {
  
  
  
  /** Default replay limit.
   * @see #replayLimit() */
  public final static int DEFAULT_REPLAY = 10;

  private final BlockRecorder recorder;
  
  
  
  
  /**
   * 
   */
  public LogHasher(
      TableSalt salt, Predicate<ByteBuffer> commentFilter, String tokenDelimiters,
      Alf rowOffsets, FrontierTable frontiers, int rnExponent) {
    
    this(
        new StateHasher(salt, commentFilter, tokenDelimiters),
        rowOffsets, frontiers, rnExponent);
  }
  

  public LogHasher(
      StateHasher promote,
      Alf rowOffsets, FrontierTable frontiers, int rnExponent) {
    
    this(promote, new BlockRecorder(frontiers, rowOffsets, rnExponent));
    
  }
  
  
  public LogHasher(StateHasher promote, BlockRecorder recorder) {
    this(promote, recorder, NO_MO_CTX );
  }
  
  private final static ContextedHasher.Context[] NO_MO_CTX = new Context[0];
  
  private static ContextedHasher.Context merge(BlockRecorder recorder, ContextedHasher.Context[] contexts) {
    Objects.requireNonNull(recorder, "null block recorder");
    if (contexts == null || contexts.length == 0)
      return recorder;
    Context[] array = new Context[contexts.length + 1];
    for (int index = contexts.length; index-- > 0; )
      array[index + 1] = contexts[index];
    array[0] = recorder;
    return new ContextArray(array);
  }
  
  public LogHasher(StateHasher promote, BlockRecorder recorder, ContextedHasher.Context... contexts) {
    super(promote, merge(recorder, contexts));
    this.recorder = recorder;
  }
  
  
  /** Closes the backing storage files (row hashes and offsets). */
  @Override
  public void close() {
    recorder.close();
  }
  
  
  @Override
  public boolean isOpen() {
    return recorder.isOpen();
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
    long fsize = recorder.frontiersRecorded();
    long osize = recorder.endOffsetsRecorded();
    
    State state;
    if (fsize < replayLimit() || osize < fsize) {
      state = play(logChannel);
    } else {
      state = checkFirstBlock(logChannel);
      // set the state to one before the last block recorded
      long index = fsize - 2;
      HashFrontier frontier = recorder.frontier(index);
      // position the logChannel to the end of this one-before-last block
      // this is why we record the EOL (not start) offset for the row, btw
      long eolOff = recorder.endOffset(index);
      logChannel.position(eolOff);
      long lineNoEst = state.rowNumber(); // minimum (likely greater)
      state = play((ReadableByteChannel) logChannel, new State(frontier, eolOff, lineNoEst));
    }
    return state;
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
    if (recorder.endOffsetsRecorded() == 0)
      throw new IllegalStateException("empty row offsets, nothing to check");
    logChannel.position(0);
    long eof = recorder.endOffset(0);
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
  

  
  
  
  public long frontiersRecorded() {
    return recorder.frontiersRecorded();
  }
  
  public long endOffsetsRecorded() {
    return recorder.endOffsetsRecorded();
  }
  
  
  /**
   * Returns the hash frontier stored at the given {@code index}.
   * The returned frontier's row number is equal to {@code (index + 1) * rnDelta()}.
   * 
   * @param index constraint: {@code 0 <= index < frontiersRecorded()}
   */
  public HashFrontier frontier(long index) {
    return recorder.frontier(index);
  }
  
  
  public long endOffset(long index) {
    try {
      return recorder.endOffset(index);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on index %d: %s".formatted(index, iox.getMessage()), iox);
    }
  }
  
  
  
  
  public void trimOffsetsRecorded(long count) {
    try {
      recorder.trimOffsetsRecorded(count);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on " + count + ": " + iox.getMessage(), iox);
    }
  }
  
  
  public void trimFrontiersRecorded(long count) {
    try {
      recorder.trimFrontiersRecorded(count);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on " + count + ": " + iox.getMessage(), iox);
    }
  }
  
  
  public void trimRecorded(long count) {
    trimOffsetsRecorded(count);
    trimFrontiersRecorded(count);
  }
  
  
  
  /**
   * Returns the row number delta between recorded blocks.
   */
  public final long rnDelta() {
    return recorder.rnDelta();
  }
  
  
  public final long rnExponent() {
    return recorder.dex();
  }
  
  
  public long lastRn() {
    return recorder.lastRn();
  }
  
  

  

}
















