/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static io.crums.sldg.logs.text.LogledgeConstants.*;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import io.crums.io.BackedupFile;
import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.logs.text.ContextedHasher.Context;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * Records the [end] state of a log (after it's been parsed to the end)
 * as a <em>.state</em> file. Since the log is allowed to be append-only,
 * there may be multiple of these files lying around: in that event,
 * on playback, if there are any conflicts, they are reported via
 * {@linkplain OffsetConflictException} or {@linkplain RowHashConflictException}.
 * 
 * @see #observeEndState(State)
 */
public class EndStateRecorder implements Context {
  

  /**
   * Infers and returns the state file's row number from its name, or
   * -1 if not well formed.
   */
  public static long inferStateRn(File stateFile) {
    var name = stateFile.getName();
    if (!name.startsWith(STATE_PREFIX) || !name.endsWith(STATE_POSTFIX))
        return -1L;
    var rn = name.substring(
        STATE_PREFIX.length(),
        name.length() - STATE_POSTFIX.length());
    try {
      
      return Long.parseLong(rn);
    
    } catch (NumberFormatException ignore) {
      sysLogger().log(Level.TRACE, "failed parsing no. in funky name: " + name);
      return -1L;
    }
  }
  
  
  private static FileFilter newStateFileFilter(long minRowNumber) {
    if (minRowNumber < 0)
      throw new IllegalArgumentException(
          "min row number %d < 0".formatted(minRowNumber));
    
    return (f) -> inferStateRn(f) >= minRowNumber;
  }

  
  
  private final static Comparator<File> STATE_FILE_COMP = new Comparator<File>() {
    @Override
    public int compare(File a, File b) {
      long ar = inferStateRn(a);
      long br = inferStateRn(b);
      return ar < br ? -1 : (ar == br ? 0 : 1);
    }
  };
  


  private static String stateFilename(State state) {
    return stateFilename( state.rowNumber() );
  }

  private static String stateFilename(long rn) {
    return STATE_PREFIX + rn + STATE_POSTFIX;
  }
  
  
  
  
  //    I N S T A N C E  

  
  private final File dir;
  private final boolean repairOffsets;

  /**
   * 
   */
  public EndStateRecorder(File dir, boolean repairOffsets) {
    this.dir = dir;
    if (dir.isFile())
      throw new IllegalArgumentException("not a directory: " + dir);
    this.repairOffsets = repairOffsets;
  }
  
  
  
  public Optional<State> loadState(long rn) throws IOException {
    File stateFile = new File(dir, stateFilename(rn));
    return stateFile.exists() ?
        Optional.of( loadState(stateFile) ) :
          Optional.empty(); 
  }
  
  

  private long nextStateCheck = -1;

  
  /**
   * If the ledgered row (line) is that recorded in a state file,
   * then the saved state is verified to match what we're seeing. Does not
   * create or modify state files.
   * 
   * @throws OffsetConflictException
   *    if the EOL offset recorded in a state file for this row number
   *    does not match
   * @throws RowHashConflictException
   *    if the row hash recorded in a state file for this row number
   *    does not match
   */
  @Override
  public void observeLedgeredLine(
      HashFrontier frontier, long offset, long eolOff, long lineNo)
          throws IOException, OffsetConflictException, RowHashConflictException {
    long rn = frontier.rowNumber();
    
    if (nextStateCheck == -1) {
      nextStateCheck =
          listStateFiles(false, rn).stream()
          .map(EndStateRecorder::inferStateRn).min(Long::compare).orElse(0L);
    }
    
    if (nextStateCheck == rn) {
      State recorded = loadState( new File(dir, stateFilename(rn)) );
      State observed = new State(frontier, eolOff, lineNo);
      nextStateCheck = -1;
      if (recorded.equals(observed))
        return;
      
      boolean offsetsEqual = recorded.eolOffset() == observed.eolOffset();
      boolean hashesEqual = recorded.frontier().equals(observed.frontier());
      
      if (!offsetsEqual) {
        if (hashesEqual && repairOffsets)
          save(observed);
        else
          throw new OffsetConflictException(
              rn, recorded.eolOffset(), observed.eolOffset());
      
      } else if (!hashesEqual) {
        throw new RowHashConflictException(rn);
      } else {
        sysLogger().log(
            Level.WARNING,
            "ignoring differing line no.s for row [%d]: expected %d; actual %d"
            .formatted(rn, recorded.lineNo(), observed.lineNo()));
      }
    }
  }


  @Override
  public int lineBufferSize() {
    this.nextStateCheck = -1;
    return Context.super.lineBufferSize();
  }


  /**
   * Loads the given state file.
   * 
   * @throws IllegalArgumentException
   *         if the file name implies a false row number
   */
  public static State loadState(File stateFile) {
    long inferredRn = inferStateRn(stateFile);
    ByteBuffer buffer;
    try (var fc = Opening.READ_ONLY.openChannel(stateFile)) {
      
      int bufferSize = (int) Math.min(8 * 1024, fc.size());
      buffer = ByteBuffer.allocate(bufferSize);
      ChannelUtils.readRemaining(fc, 0L, buffer).flip();
    
    } catch (IOException iox) {
      throw new UncheckedIOException("on loading " + stateFile, iox);
    }

    assertMagic(buffer);
    buffer.position(buffer.position() + UNUSED_PAD);
    State state = State.loadSerial(buffer);
    if (state.rowNumber() != inferredRn && inferredRn != -1)
      throw new IllegalArgumentException(
          "file-naming (%s) hanky panky: inferred state RN %d; actual %d"
          .formatted(stateFile.getName(), inferredRn, state.rowNumber()));
    
    return state;
  }

  /**
   * Checks the magic bytes and advances the given buffer.
   * 
   * @param expected  expected bytes (as UTF-8 encoded)
   * @param buffer    positioned at beginning of magic bytes
   */
  private static void assertMagic(ByteBuffer buffer) {
    for (int index = 0; index < STATE_MAGIC.length(); ++index) {
      if (buffer.get() != STATE_MAGIC.charAt(index))
        throw new ByteFormatException(
            "magic bytes '%s' not matched".formatted(STATE_MAGIC));
    }
  }
  
  
  
  /**
   * Returns the list of state files in order of row number.
   * 
   * @return {@code listStateFiles(true, 0)}
   */
  public List<File> listStateFiles() {
    return listStateFiles(true, 0);
  }
  

  /**
   * Returns the list of state files.
   * 
   * @param sort          if {@code true} then sorted by row number
   * @param minRowNumber  the minimum row number (&ge; 0)
   * @return not null.
   */
  public List<File> listStateFiles(boolean sort, long minRowNumber) {
    File[] stateFiles = dir.listFiles(newStateFileFilter(minRowNumber));
    if (stateFiles == null)
      return List.of();
    if (sort && stateFiles.length > 1)
      Arrays.sort(stateFiles, STATE_FILE_COMP);
    return Lists.asReadOnlyList(stateFiles);
  }


  @Override
  public State nextStateAhead(State state, long rn) throws IOException {
    var stateFiles = listStateFiles();
    if (stateFiles.isEmpty())
      return state;
    int candidateIndex;
    {
      List<Long> stateRns = Lists.map(
          stateFiles, EndStateRecorder::inferStateRn);
      int index = Collections.binarySearch(stateRns, rn);
      if (index < 0)
        index = -1 - index;
      candidateIndex = index - 1;
    }
    
    if (candidateIndex != -1) {
      State candidate = loadState(stateFiles.get(candidateIndex));
      if (candidate.rowNumber() > state.rowNumber()) {
        assert candidate.rowNumber() < rn;
        return candidate;
      }
    }
    return state;
  }


  /**
   * Saves the end {@code state} on observing it.
   */
  @Override
  public void observeEndState(State state) throws IOException {
    save(state);
  }
  
  
  /**
   * Lists the row numbers of the saved state files.
   */
  public List<Long> listStateRns() {
    return Lists.map(listStateFiles(), EndStateRecorder::inferStateRn);
  }
  
  
  
  /**
   * Saves the given {@code state} at its expected file path.
   * If the file already exists, and is different from what's
   * about to be written, then the exisitng file is first moved
   * out of the way.
   */
  public void save(State state) throws IOException {
    File stateFile = new File(dir, stateFilename(state));
    if (stateFile.exists()) {
      try {
        if (loadState(stateFile).equals(state))
          return;
      } catch (Exception x) {
        sysLogger().log(
            Level.WARNING, "moving unreadable state file " + stateFile);
      }
    }
    BackedupFile backup = new BackedupFile(stateFile);
    backup.moveToBackup();
    
    var ch = Opening.CREATE.openChannel(stateFile);
    
    try (ch) {
      
      var buffer = ByteBuffer.allocate(64 * 128); // 8k
      buffer.put(Strings.utf8Bytes(STATE_MAGIC));
      for (int bytes = UNUSED_PAD; bytes-- > 0;)
        buffer.put((byte) 0);
      state.writeTo(buffer).flip();
      ChannelUtils.writeRemaining(ch, 0L, buffer);
    
    } catch (Exception x) {
      ch.close();
      backup.rollback();
      if (x instanceof IOException iox)
        throw iox;
      if (x instanceof RuntimeException rx)
        throw rx;
      
      assert false; // not reachable
    }
  }
  

}





