/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.demo.jurno;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

import io.crums.client.ClientException;
import io.crums.sldg.Ledger;
import io.crums.sldg.db.Db;
import io.crums.util.hash.Digests;

/**
 * A text file journaled with a {@linkplain Ledger ledger}.
 * The hash of every (ledgerable) line in a journal corresponds to a (non-beacon) row
 * in a ledger. Line numbers, like ledger row numbers, are 1-based.
 * 
 * <h3>Limits</h3>
 * <p>
 * This implementation can manage a maximum of 2,147,483,647 lines.
 * </p>
 * <h3>Concurrent Writes</h3>
 * <p>
 * BAD. Everything breaks: don't do this. On the plus side, the next time you
 * try to read the data, you'll know you broke it.
 * </p>
 * 
 * @see State
 */
public class Journal {
  
  
  /**
   * The state of a {@linkplain journal} is the state of its text file vs the state
   * of the ledger that records its history.
   */
  public enum State {
    /**
     * Both the ledger and text file are empty.
     */
    INIT,
    /**
     * The ledger and text file agree up to some positive row number followed by a row
     * in the ledger than conflicts with the hash of the next line in the text file.
     * These conflicting row and line numbers are called <em>forked</em> numbers.
     * 
     * @see Journal#getForkLineNumber()
     */
    FORKED,
    /**
     * The text file is a truncated version of the file the ledger recorded.
     */
    TRIMMED,
    /**
     * The text file contains new lines that the ledger has not recorded.
     */
    PENDING,
    /**
     * The ledger is up-to-date with the non-empty text file.
     */
    COMPLETE;
    
    public boolean isForked() {
      return this == FORKED;
    }
    
    public boolean isTrimmed() {
      return this == TRIMMED;
    }
    
    /**
     * Determines if the ledger needs mending (because lines previously recorded in the ledger have
     * since changed).
     */
    public boolean needsMending() {
      return this == FORKED || this == TRIMMED;
    }
    
    public boolean isPending() {
      return this == PENDING;
    }
    
    /**
     * Determines if the journal is up-to-date. The {@linkplain #INIT} and
     * {@linkplain #COMPLETE} states both satisfy this property.
     * @return
     */
    public boolean isComplete() {
      return this == COMPLETE || this == INIT;
    }
  }
  
  
  private final static Charset UTF8 = Charset.forName("UTF-8");
  
  private final File textFile;
  
  private final Db db;
  
  private State state;
  
  private int forkLineNumber;
  private int forkTextLineNumber;

   
  private int lines; // (int type: bound by Integer.MAX_VALUE anyway, cuz of List interface)
  private int allLines;
  
  
  
  public void clear() {
    state = null;
    forkLineNumber = forkTextLineNumber = lines = allLines = 0;
  }
  
  
  
  
  public Journal(File textFile, Db db) {
    this.textFile = Objects.requireNonNull(textFile, "null textFile");
    this.db = Objects.requireNonNull(db, "null db");
    if (!textFile.isFile())
      throw new IllegalArgumentException("not a file: " + textFile);
  }
  
  
  /**
   * Returns the directory the backing ledger is kept in.
   * 
   * @see #ledgerDir()
   */
  public File ledgerDir() {
    return db.getDir();
  }
  
  
  
  public Db db() {
    return db;
  }
  
  
  /**
   * Returns the last witnessed line, if any; 0 (zero), otherwise.
   */
  public int lastLineWitnessed() {
    return (int) db.lastSansBeaconRowWitnessed();
  }
  
  
  /**
   * Returns the text file.
   * 
   * @see #ledgerDir()
   */
  public File getTextFile() {
    return textFile;
  }
  
  
  /**
   * Returns the current number of already-legered lines in the database. This value is always available
   * (even immediately after construction).
   * 
   *  @return {@code db().sizeSansBeacons()}
   */
  public int getLedgeredLines() {
    return db.sizeSansBeacons();
  }
  
  
  /**
   * Returns the state of the instance, or {@code null} if neither {@linkplain #update() update}d nor
   * {@linkplain #dryRun() dry-run}ned.
   * 
   * <h3>State of Accessor Methods</h3>
   * 
   * The return values of the methods
   * <ul>
   * <li>{@linkplain #getLines()},</li>
   * <li>{@linkplain #getTextLineCount()},</li>
   * <li>{@linkplain #getForkLineNumber()}, and</li>
   * <li>{@linkplain #getForkTextLineNumber()}</li>
   * </ul>
   * are significant only if this method does not return null.
   */
  public State getState() {
    return state;
  }
  
  
  /**
   * Returns the number of lines in the text file. The number of <em>ledgerable</em>
   * lines may be fewer. For example, blank lines typically don't count.
   * 
   * @see #getLines()
   */
  public int getTextLineCount() {
    return allLines;
  }
  
  
  /**
   * Returns the number of <em>ledgerable lines</em> in the text file.
   * 
   * @see #getTextLineCount()
   * @see #canonicalizeLine(String)
   */
  public int getLines() {
    return lines;
  }
  
  
  /**
   * Returns the <em>ledgerable</em> line number at which the text file has forked from its historical
   * ledger, or zero if the {@linkplain #getState() state} is not {@linkplain State#FORKED forked}.
   */
  public int getForkLineNumber() {
    return forkLineNumber;
  }
  
  
  /**
   * Returns the line number in the text at which the file has forked from its historical
   * ledger, or zero if the {@linkplain #getState() state} is not {@linkplain State#FORKED forked}.
   */
  public int getForkTextLineNumber() {
    return forkTextLineNumber;
  }
  
  
  public void trim() throws IllegalStateException {
    if (state == null)
      throw new IllegalStateException(
          "trim() method invalid as 1st member invocation on instance: " + this);
    if (!state.needsMending())
      throw new IllegalStateException(state + " doesn't need mending: " + this);
    
    long lastValidRow;
    if (state.isTrimmed()) {
      assert lines > 0;
      lastValidRow = db.rowNumWithBeacons(lines);
    } else {
      // state isForked
      assert forkLineNumber > 1;
      lastValidRow = db.rowNumWithBeacons(forkLineNumber - 1);
    }

    db.truncate(lastValidRow);
  }
  
  
  
  public Db.WitnessReport witness() {
    return db.witness();
  }

  
  
  public boolean update() throws IllegalStateException {
    return update(false);
  }
  
  
  public boolean update(boolean abortOnFork) throws IllegalStateException {
    if (state == null) {
      long initSize = db.size();
      parseText(verifyLineFunc(), newLineProc(), abortOnFork);
      return db.size() > initSize;
    } else if (state.isComplete()) {
      return false;
    } else if (state.needsMending()) {
      throw new IllegalStateException(this + " must be mended first -- see trim()");
    }
    
    assert state.isPending();
    long initSize = db.size();
    
    parseText((s, r) -> true, newLineProc(), false);
    
    assert initSize < db.size() && state.isComplete();
    
    return true;
  }
  
  
  public void dryRun() {
    parseText(verifyLineFunc(), line -> { }, false);
  }
  
  
  
  
  
  private void parseText(
      BiPredicate<String, Long> verifyLineFunc, Consumer<String> newLineProc,
      boolean exitOnFirstError)
          throws UncheckedIOException {
    
    clear();
    
    List<Long> beaconRns = db.getBeaconRowNumbers();
    int bcnIndex = 0;
    long nextBeaconRn = beaconRns.isEmpty() ? Long.MAX_VALUE : beaconRns.get(0);
    
    long rowNumber = 0;

    final long maxRow = db.size();
    
    
    try (BufferedReader reader = new BufferedReader(new FileReader(textFile))) {
      
      
      while (rowNumber < maxRow) {
        
        String line = reader.readLine();
        if (line == null)
          break;
        
        ++allLines;
        
        line = canonicalizeLine(line);
        if (line == null)
          continue;
        
        ++lines;
        ++rowNumber;
        
        // if the next row is a beacon, skip over it
        while (rowNumber == nextBeaconRn) { // in case there are consecutive beacons (?)
          ++rowNumber;
          ++bcnIndex;
          nextBeaconRn = bcnIndex == beaconRns.size() ? Long.MAX_VALUE : beaconRns.get(bcnIndex);
          assert nextBeaconRn >= rowNumber && rowNumber <= maxRow;
        }
        

        if (!verifyLineFunc.test(line, rowNumber)) {

          if (lines == 1)
            throw new IllegalStateException(
                "Wrong file/ledger combination(?): 1st line of " + textFile + " conflicts with 1st row in ledger. " +
                "To start from scratch create a new journal");
          state = State.FORKED;
          forkLineNumber = lines;
          forkTextLineNumber = allLines;
          break;
        }
      } // while
      
      
      final boolean okToAdd;
      
      if (state == State.FORKED) {
        if (exitOnFirstError)
          return;
        
        okToAdd = false;
      
      } else if (rowNumber == maxRow) {
        
        okToAdd = true;
        
      } else {
        if (lines == 0)
          throw new IllegalStateException(
              "text file " + textFile + " has been trimmed to zero lines. " +
              "To start from scratch create a new journal");
        state = State.TRIMMED;
        return;
      }
      
        
      // process, or just count, the remaining lines
      while (true) {
        
        String line = reader.readLine();
        if (line == null)
          break;
        
        ++allLines;
        
        line = canonicalizeLine(line);
        if (line == null)
          continue;
        
        ++lines;
        
        if (okToAdd)
          newLineProc.accept(line);
        
      }
      
      
    } catch (IOException iox) {
      throw new UncheckedIOException("on initState", iox);
    }
    // the reader is closed
    
    
    // update the state
    
    if (state != null)
      return;
    
    final long nonBeaconRows = db.sizeSansBeacons();
    
    if (lines > nonBeaconRows) {
      state = State.PENDING;
    
    } else if (lines < nonBeaconRows) {
      
      throw new IllegalStateException("something's amiss " + this);
    
    } else {
      
      // both the following states are complete, however
      // it's worth distinguishing the empty case..
      state = lines == 0 ? State.INIT : State.COMPLETE;
    }
  }
  
  
  
  
  /**
   * Canonicalizes the given non-null {@code line} and returns it. Some
   * lines, per the parsing rules of the subclass, may count for naught:
   * in such cases, {@code null} is returned.
   * <p>
   * The base implementation trims leading and trailing whitespace from
   * the given line: empty lines don't count and so are returned as {@code null}.
   * </p>
   */
  protected String canonicalizeLine(String line) {
    line = line.trim();
    return line.isEmpty() ? null : line;
  }
  
  

  
  /**
   * Returns the new line processor. The returned object adds a new row to the ledger
   * on each invocation.
   */
  protected Consumer<String> newLineProc() {
    MessageDigest digest = Digests.SHA_256.newDigest();
    int[] countPtr = { 0 };
    return line ->  addLine(line, digest, countPtr[0]++);
  }
  
  
  private void addLine(String line, MessageDigest digest, int count) {
    if (count == 0) {
      try {
        db.addBeacon();
      } catch (ClientException x) {
        // swallow
      }
    }
    ByteBuffer hash = lineHash(line, digest);
    db.getLedger().appendRows(hash);
  }
  
  
  /**
   * Returns the line verification function.
   */
  protected BiPredicate<String, Long> verifyLineFunc() {
    MessageDigest digest = Digests.SHA_256.newDigest();
    return (line, rn) -> verifyLine(line, rn, digest);
  }
  
  
  private boolean verifyLine(String line, long rowNumber, MessageDigest digest) {
    ByteBuffer hash = lineHash(line, digest);
    return db.getLedger().getRow(rowNumber).inputHash().equals(hash);
  }
  
  
  private ByteBuffer lineHash(String line, MessageDigest digest) {
    if (line.isEmpty())
      return Digests.SHA_256.sentinelHash();

    digest.reset();
    byte[] b = line.getBytes(UTF8);
    return ByteBuffer.wrap( digest.digest(b) );
  }
  
  
  @Override
  public String toString() {
    File dir = db.getDir().getAbsoluteFile();
    File parent = dir.getParentFile();
    return "Journal[" + parent.getName() + "/" + dir.getName() + "]";
  }

}
