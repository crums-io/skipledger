/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static io.crums.sldg.logs.text.LogledgeConstants.STATE_EXT;
import static io.crums.sldg.logs.text.LogledgeConstants.STATE_MAGIC;
import static io.crums.sldg.logs.text.LogledgeConstants.STATE_PREFIX;
import static io.crums.sldg.logs.text.LogledgeConstants.UNUSED_PAD;
import static io.crums.sldg.logs.text.LogledgeConstants.parseRnInName;
import static io.crums.sldg.logs.text.LogledgeConstants.sysLogger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.crums.io.BackedupFile;
import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.logs.text.ContextedHasher.Context;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * Records the [end] state of a log (after it's been parsed to the end)
 * as a <em>.fstate</em> file. Since the log is allowed to be append-only,
 * there may be multiple of these files lying around: in that event,
 * on playback, if there are any conflicts, they are reported via
 * {@linkplain LineNoConflictException} or {@linkplain RowHashConflictException}s.
 * 
 * 
 * @see #observeEndState(Fro)
 * @see Fro
 */
public class EndStateRecorder extends NumberFiler implements Context {
  

  /**
   * Infers and returns the state file's row number from its name, or
   * -1 if not well formed.
   */
  public static long inferStateRn(File stateFile) {
    return parseRnInName(stateFile.getName(), STATE_PREFIX, STATE_EXT);
  }
  
 
  
  
  
  
  //    I N S T A N C E  

  
  private boolean repairOffsets;

  private long nextStateCheck = -1;
  
  /**
   * Instance does not repair offsets.
   * 
   * @param dir   directory path
   */
  public EndStateRecorder(File dir) {
    this(dir, false);
  }

  /**
   * Full constructor.
   */
  public EndStateRecorder(File dir, boolean repairOffsets) {
    super(dir, STATE_PREFIX, STATE_EXT);
    this.repairOffsets = repairOffsets;
  }
  
  
  /**
   * Sets the repair mode. After repairing, you usually want to set it
   * back to {@code false}.
   */
  public void repairsOffsets(boolean on) {
    this.repairOffsets = on;
  }
  
  /**
   * Determines whether the instance is configured to repair EOL offsets and line no.s.
   * Instances start in the {@code false} state.
   * 
   * @see #repairsOffsets(boolean)
   */
  public final boolean repairsOffsets() {
    return repairOffsets;
  }
  
  
  
  
  public Optional<Fro> loadState(long rn) throws IOException {
    File stateFile = rnFile(rn);
    return stateFile.exists() ?
        Optional.of( loadFro(stateFile) ) :
          Optional.empty(); 
  }
  

  

  @Override
  public void init() {
    this.nextStateCheck = -1;
  }

  
  /**
   * If the ledgered row (line) is that recorded in a state file,
   * then the saved state is verified to match what we're seeing.
   * 
   * <p>
   * If the row hash does not match {@code RowHashConflictException} is thrown.
   * If an EOL offset or line no. conflict is encountered but the row hash is
   * still correct, and the {@linkplain #repairsOffsets()} property is {@code false}
   * then a {@code LineNoConflictException} is thrown; if {@code repairsOffsets}
   * is {@code true}, then the line no.s and offsets are repaired (the old file
   * is backed up and a new file is written).
   * </p>
   * 
   * @throws OffsetConflictException
   *    if the EOL offset or line no. recorded in a state file for this row number
   *    does not match, but the row hash does
   * @throws RowHashConflictException
   *    if the row hash recorded in a state file for this row number
   *    does not match
   *
   * @see #repairsOffsets()
   * @see #repairsOffsets(boolean)
   */
  @Override
  public void observeLedgeredLine(Fro fro, long eolOff)
          throws IOException, LineNoConflictException, RowHashConflictException {
    
    long rn = fro.rowNumber();
    
    if (nextStateCheck == -1) {
      nextStateCheck =
          listFiles(false, rn).stream()
          .map(this::inferRn).min(Long::compare).orElse(0L);
    }
    
    if (nextStateCheck == rn) {
      Fro recorded = loadFro( rnFile(rn) );
      State observed = fro.state();
      nextStateCheck = -1;
      
      State recordedState = recorded.state();
      
      if (recordedState.equals(observed))
        return;
      
      if (recordedState.frontier().equals(observed.frontier())) {
        if (repairOffsets) {
          save(fro);
        } else {
          throw new LineNoConflictException(recordedState, observed);
        }
      } else {
        throw new RowHashConflictException(rn,
            "hash conflict at row [%d] (recorded/actual): line no. (%d/%d); EOL offset (%d/%d)"
            .formatted(
                rn,
                recorded.lineNo(), observed.lineNo(),
                recorded.eolOffset(), observed.eolOffset()));
      }
    }
  }


  /**
   * Loads the given state file.
   * 
   * @throws IllegalArgumentException
   *         if the file name implies a false row number
   */
  public static Fro loadFro(File stateFile) {
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
    Fro state = Fro.loadSerial(buffer);
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



  @Override
  public State nextStateAhead(State state, long rn) throws IOException {
    
    assert state.rowNumber() < rn;
    
    var stateFiles = listFiles(true, state.rowNumber());
    if (stateFiles.isEmpty())
      return state;
    
    State candidate;
    File stateFile;
    {
      List<Long> stateRns = Lists.map(stateFiles, this::inferRn);
      int index = Collections.binarySearch(stateRns, rn);
      if (index >= 0) {
        stateFile = stateFiles.get(index);
        candidate = loadFro(stateFile).preState();
      } else {
        int insertIndex = -1 - index;
        index = insertIndex - 1;
        if (index >= 0) {
          stateFile = stateFiles.get(index);
          candidate = loadFro(stateFile).state();
        } else
          return state;
      }
    }
    
    if (candidate.rowNumber() == state.rowNumber()) {
      
      if (candidate.equals(state))
        return state;
      
      if (candidate.eolOffset() != state.eolOffset() || candidate.lineNo() < state.lineNo())
        throw new LineNoConflictException(candidate, state);
      
      
      if (!candidate.frontier().equals(state.frontier()))
          throw new RowHashConflictException(
              rn, "state %s row hash conflicts with that recorded in %s"
              .formatted(state.toString(), stateFile.getName()));
      
      assert candidate.lineNo() > state.lineNo(); // they can't be equal
      
      // prefer the state with the greater line no.
      return candidate;
    }
    
    assert candidate.rowNumber() < rn;
    
    return candidate.rowNumber() < state.rowNumber() ? state : candidate;
    
  }
  
  


  /**
   * Saves the end {@code state} on observing it.
   */
  @Override
  public void observeEndState(Fro fro) throws IOException {
    save(fro);
  }
  
  
  
  
  public Optional<Fro> lastFro() {
    var files = listFiles();
    return files.isEmpty() ?
        Optional.empty() :
          Optional.of(loadFro(files.get(files.size() - 1)));
  }
  
  
  
  /**
   * Saves the given {@code state} at its expected file path.
   * If the file already exists, and is different from what's
   * about to be written, then the exisitng file is first moved
   * out of the way.
   */
  public void save(Fro fro) throws IOException {
    File stateFile = rnFile(fro.rowNumber());
    if (stateFile.exists()) {
      try {
        if (loadFro(stateFile).equals(fro))
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
      fro.writeTo(buffer).flip();
      ChannelUtils.writeRemaining(ch, 0L, buffer);
    
    } catch (IOException | RuntimeException x) {
      ch.close();
      backup.rollback();
      throw x;
    }
  }
  

}





