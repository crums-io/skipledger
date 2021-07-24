/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.logging.Logger;

import io.crums.sldg.packs.LedgerMorselBuilder;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.time.WitnessReport;
import io.crums.util.Lists;
import io.crums.util.TaskStack;
import io.crums.util.ticker.Ticker;

/**
 * A <em>source</em>-ledger paired with a <em>hash</em>-ledger that tracks it.
 * An instance encapsulates a pair of these ledgers that are in agreement up to
 * some non-zero row number (excepting initialization corner cases).
 * 
 * <h2>Instance State</h2>
 * <p>
 * Instances of this class are somewhat tolerant if source ledger rows are discovered
 * to have been modified (or deleted) after they were recorded in the hash ledger. In such
 * cases, if the offending source ledger row cannot be restored to its old state, then
 * the only solution is to {@linkplain #rollback() rollback} the hash ledger.
 * </p><p>
 * However, an instance will not tolerate (will throw exceptions) if any of the following
 * is detected.
 * <ol>
 * <li>If the hash-ledger is corrupted (internal consistency broken).</li>
 * <li>If the hash-ledger's first row does not match the source-ledger's first row.</li>
 * </ol>
 * </p>
 * 
 * @see SourceLedger
 * @see HashLedger
 * @see State
 */
public class Ledger implements AutoCloseable {
  
  /**
   * Logger name. Forks and trims are logged as warnings; fatal errors (e.g. a tampered hashes
   * in the skip ledger) are logged before an exception is thrown.
   * 
   * @see State#FORKED
   * @see State#TRIMMED
   */
  public final static String LOG_NAME = "sldg";
  
  /**
   * The state of a valid (i.e. usable) {@linkplain Ledger} instance whose source-
   * and hash-ledgers are in agreement up to some non-zero row number (unless the
   * hash-ledger is empty).
   * <p>
   * There are 4 recognized states. 2 are <em>normal</em>: {@linkplain #PENDING} and
   * {@linkplain #COMPLETE}. The <em>abnormal</em> states are {@linkplain #TRIMMED} and
   * {@linkplain #FORKED}.
   * </p>
   * <h2>TODO: Additional State Info</h2>
   * <p>
   * Distinguish witnessed from unwitnessed state.
   * </p>
   * @see Ledger#getState()
   */
  public enum State {
    
    /**
     * The source- and hash-ledgers agree up to some positive row number followed by a
     * source-row whose hash conflicts with that recorded in the hash-ledger.
     */
    FORKED,
    /**
     * The source-ledger is a truncated version of (has fewer rows than) the one the hash-ledger has recorded.
     */
    TRIMMED,
    /**
     * The source-ledger contains new rows that the hash-ledger has yet to record.
     */
    PENDING,
    /**
     * The hash-ledger is up-to-date with the source-ledger.
     */
    COMPLETE;
    

    /**
     * @return {@code this == FORKED}
     */
    public boolean isForked() {
      return this == FORKED;
    }

    /**
     * @return {@code this == TRIMMED}
     */
    public boolean isTrimmed() {
      return this == TRIMMED;
    }
    
    /**
     * Determines if the hash-ledger needs mending (because rows previously recorded in
     * the source-ledger have since changed or are missing).
     * 
     * @return {@code this == FORKED || this == TRIMMED}
     */
    public boolean needsMending() {
      return this == FORKED || this == TRIMMED;
    }
    
    /**
     * Doesn't need mending.
     * 
     * @return {@code this == PENDING || this == COMPLETE}
     */
    public boolean ok() {
      return this == PENDING || this == COMPLETE;
    }
    

    /**
     * @return {@code this == PENDING}
     */
    public boolean isPending() {
      return this == PENDING;
    }
    
    /**
     * @return {@code this == COMPLETE}
     */
    public boolean isComplete() {
      return this == COMPLETE;
    }
  }
  
  
  
  
  private final SourceLedger srcLedger;
  private final HashLedger hashLedger;
  
  private Ticker ticker = Ticker.NOOP;
  
  private State state;
  private long firstConflict;
  
  
  /**
   * Creates an instance that <em>owns</em> both arguments. There's no progress ticker.
   * 
   * @param srcLedger the source
   * @param hashLedger the hash ledger matching the source
   * 
   * @see #setProgressTicker(Ticker)
   * @see #close()
   */
  public Ledger(SourceLedger srcLedger, HashLedger hashLedger) {
    this(srcLedger, hashLedger, null);
  }

  /**
   * Creates an instance that <em>owns</em> both arguments.
   * 
   * @param srcLedger the source
   * @param hashLedger the hash ledger matching the source
   * @param progress optional progress ticker, ticked once per source-row
   * 
   * @see #checkEndRows() Invokes {@code checkEndRows()}
   * @see #setProgressTicker(Ticker)
   * @see #close()
   */
  public Ledger(SourceLedger srcLedger, HashLedger hashLedger, Ticker progress) {
    this.srcLedger = Objects.requireNonNull(srcLedger, "null srcLedger");
    this.hashLedger = Objects.requireNonNull(hashLedger, "null hashLedger");
    setProgressTicker(progress);
    checkEndRows();
  }
  
  
  public SourceLedger getSourceLedger() {
    return srcLedger;
  }
  
  
  public HashLedger getHashLedger() {
    return hashLedger;
  }
  
  
  /**
   * Sets the progress ticker.
   * 
   * @param progress optional progress ticker, ticked once per source-row. May be set to {@code null}.
   */
  public void setProgressTicker(Ticker progress) {
    this.ticker = progress == null ? Ticker.NOOP : progress;
  }
  
  
  /**
   * Returns the state. Methods bearing the <em>check</em> moniker in their names
   * update the instance's state.
   * 
   * @return not null
   */
  public State getState() {
    return state;
  }
  
  
  /**
   * Returns the lowest known number at which the rows in the source- and hash-ledgers
   * conflict in hashes, when the {@linkplain #getState() state} is
   * {@linkplain State#FORKED forked}; zero, o.w.
   */
  public long getFirstConflict() {
    return firstConflict;
  }

  
  
  
  /**
   * Checks the first and last known good (or bad) row and sets the state. If the
   * instance's state is already forked ({@code getState().isForked()} is {@code true}),
   * then the {@linkplain #getFirstConflict() first conflict} is checked before the
   * first and last rows.
   * 
   * <h3>Implementation note</h3>
   * <p>
   * This method is invoked by the base class's constructor. It may be overridden, so long
   * as the new behavior does not depend on any new fields defined in the subclass (they will
   * be {@code null} when this method is invoked from the base constructor).
   * </p>
   * 
   * @return {@code getState().ok()}
   * 
   * @throws SldgException if the source ledger is empty, but the hash ledger is not
   * @throws HashConflictException if the source- and hash ledger conflict at the very first row
   */
  public boolean checkEndRows() {
    
    
    
    final long hSize = hashLedger.size();
    final long srcSize = srcLedger.size();
    
    assert hSize >= 0 && srcSize >= 0;
    
    // if the hash ledger is empty, there's nothing to check.
    if (hSize == 0) {
      state = srcSize == 0 ? State.COMPLETE : State.PENDING;
      firstConflict = 0;
      return true;
      
    // sanity check source ledger is not empty
    } else if (srcSize == 0) {
      throw new SldgException(
          "illegal configuration: source-ledger " + srcLedger +
          " is empty, while hash-ledger " + hashLedger + " is not");
    }
    
    // sanity check the first row
    if (firstConflict(1L) != 0) {
      setFork(1L);
      return false; // (never reached)
    }
    
    
    
    // determine the max row num that can be checked
    final long maxCheck = Math.min(hSize, srcSize);
    
    // if the state was forked, check the fork..
    // and if it's still there, abort the check;
    // o.w. (if fixed), clear the state.
    
    if (state == State.FORKED) {
      assert firstConflict != 0L;

      // check the old conflict, and if not fixed, abort the check
      if (firstConflict <= maxCheck && firstConflict(firstConflict) != 0)
          return false;
      
      
      // There may be higher numbered rows broken. For now, we
      // trust the administrator to run a full ledger check after
      // discovering a fork. It's not automated, because.. well if
      // it's a big ledger we don't want to get stuck with an exhaustive
      // check to test every fix.
      
      firstConflict = 0;
      state = null;
    }
    
    
    assert state != State.FORKED && firstConflict == 0; // (duh)
    
    if (srcSize < hSize) {
      state = State.TRIMMED;
    } else {
      state = null;
    }
    
    // check the 
    if (maxCheck > 1 && firstConflict(maxCheck) != 0) {
      setFork(maxCheck);
    
    } else if (state == null) {
      state = srcSize == hSize ? State.COMPLETE : State.PENDING;
      this.firstConflict = 0; // redundant
    }
    
    return state.ok();
  }
  
  
  private long setFork(long conflict) {
    if (conflict == 0)
      return 0;
    
    else if (conflict == 1)
      throw new HashConflictException(
          "illegal configuration: source-ledger " + srcLedger + " and hash-ledger " +
          hashLedger + " conflict at row [1]");
    
    else if (firstConflict == 0 || firstConflict > conflict) {
      String msg;
      if (firstConflict == 0)
        msg = "FORK (!): conflict discovered. First-conflict set to [" + conflict + "]";
      else
        msg = "FORK: First-conflict updated from [" + firstConflict + "] to [" + conflict + "]";
      
      firstConflict = conflict;
      log().warning(msg);
    }
    
    state = State.FORKED;
    return conflict;
  }
  
  
  
  /**
   * Checks the given range of row numbers for conflicts and returns the first conflicting
   * row number; zero if none are found. If a conflict is found, the state is set to
   * {@linkplain State#FORKED FORKED}, and the {@linkplain #getFirstConflict() first conflict}ing
   * row number is set to it, if it's non-zero and greater than the returned number.
   * 
   * @param lo &ge; 1
   * @param hi &le; {@linkplain #lesserSize()}
   * @param asc if {@code false}, then the range is checked in descending order; ascending, o.w.
   * @param progress optional progress ticker
   * 
   * @return the first conflicting row number found; 0 (zero) if none found
   */
  public long checkRowRange(long lo, long hi, boolean asc) {
    checkRangeArgs(lo, hi);
    
    List<Long> range = Lists.longRange(lo, hi);
    if (!asc)
      range = Lists.reverse(range);
    long conflict = firstConflict(range);
    return setFork(conflict);
  }
  
  
  private void checkRangeArgs(long lo, long hi) {
    if (lo < 1 || hi < lo || hi > lesserSize())
      throw new IllegalArgumentException("lo " + lo + ", hi " + hi);
  }
  
  

  /**
   * Checks the given range of row numbers for conflicts {@code count}-many times at random and
   * returns the first conflicting
   * row number; zero if none are found. If a conflict is found, the state is set to
   * {@linkplain State#FORKED FORKED}, and the {@linkplain #getFirstConflict() first conflict}ing
   * row number is set to it, if it's zero or if its greater than the returned number.
   * 
   * @param lo &ge; 1
   * @param hi &le; {@linkplain #lesserSize()}
   * @param count the number of times a random sample is taken
   * @param random    optional random number generator (for reproducibility)
   * 
   * @return the first conflicting row number found; 0 (zero) if none found
   */
  public long randomCheckRowRange(long lo, long hi, int count, Random random) {
    checkRangeArgs(lo, hi);
    
    if (count <= 0) {
      if (count == 0)
        return 0L;
      throw new IllegalArgumentException("count " + count);
    }
    
    
    final long rangeSize = hi - lo + 1;
    
    if (rangeSize / (double) count <= WHOLE_CHECK_RATIO && rangeSize <= Integer.MAX_VALUE)
      return checkRowRange(lo, hi, true);
    
    final Random rand = random == null ? new Random() : random;
    
    
    
    Iterator<Long> randomRangeIter = new Iterator<Long>() {
      
      int counter = count;

      @Override
      public boolean hasNext() {
        return counter > 0;
      }

      @Override
      public Long next() {
        if (--counter < 0)
          throw new NoSuchElementException();
        
        double uniform = rand.nextDouble();
        long delta = (long) (uniform * rangeSize);
        long next = lo + delta;
        if (next > hi)
          next = hi;
        return next;
      }
    };

    
    long conflict = firstConflict(randomRangeIter);
    
    return setFork(conflict);
    
  }
  
  
  /**
   * Rolls back the hash ledger if the {@linkplain #getState() state} needs mending.
   * This method should be invoked with caution, since the ledger loses history.
   * <p>
   * If the state is {@linkplain State#TRIMMED trimmed}, then the hash-ledger's
   * size is trimmed to the {@linkplain #sourceLedgerSize() source-ledger's size}. Otherwise,
   * if the state is {@linkplain State#FORKED forked}, then the hash-ledger's size
   * is trimmed to one less than the {@linkplain #getFirstConflict() first conflict}.
   * </p>
   * 
   * @see HashLedger#trimSize(long)
   */
  public void rollback() {
    if (state.ok())
      return;
    long trimmedSize = state.isForked() ? firstConflict - 1 : srcLedger.size();
    assert trimmedSize > 0;
    hashLedger.trimSize(trimmedSize);
    firstConflict = 0;
    state = null;
    checkEndRows();
  }
  
  
  
  
  /**
   * Return the lesser of the source- and hash-ledger sizes.
   */
  public final long lesserSize() {
    return Math.max(srcLedger.size(), hashLedger.size());
  }


  /**
   * Samples the source-ledger and hash-ledger in order to discover conflicts.
   * 
   * @param count sample count. If this is in the ballpark of the
   *              total number of existing rows, then all rows are checked.
   * @param random    optional random number generated used for sampling
   * 
   * @return {@code true} <b>iff</b> on return the ledger doesn't need mending
   */
  public boolean checkState(int count, Random random) {
    
    if (!checkEndRows())
      return false;
    
    long maxCheck = lesserSize();
    if (maxCheck < 3)
      return false;
    randomCheckRowRange(2, maxCheck - 1, count, random);
    return state.ok();
  }

  private final static double WHOLE_CHECK_RATIO = 0.5;
  
  
  
  
  
  
  
  
  
  
  private long firstConflict(Long... rows) {
    return firstConflict(Lists.asReadOnlyList(rows));
  }
  
  private long firstConflict(Collection<Long> rows) {
    return firstConflict(rows.iterator());
  }
  
  private long firstConflict(Iterator<Long> rows) {
    SkipLedger skipLedger = hashLedger.getSkipLedger();
    
    while (rows.hasNext()) {
      long rn = rows.next();
      SourceRow srcRow = srcLedger.getSourceRow(rn);
      Row row = skipLedger.getRow(rn);
      ticker.tick();
      
      // Note, if *row* is a lazy instance, then below cascades to multiple (on average 2)
      // additional calls to the skip-ledger to retrieve each row-hash referenced at that
      // row number
      if (! SerialRow.toInstance(row).hash().equals(row.hash())) {
        // this message applies to every subclass CompactSkipLedger
        // which, as of July '21, is the parent of every implementation--
        // so, presently, the message below is always apt
        String msg =
            "row-hash [" + rn + "] in skip ledger " + skipLedger +
            " conflicts with row-hash calculated from row's input-hash + " +
            "hashpointers. Either row [" + rn + "] is corrupted (its <input-hash>/<row-hash>" +
            " columns), or one of the rows it references has a corrupted <row-hash> column.";
        log().severe(msg);
        throw new HashConflictException(msg);
      }
      ByteBuffer expectHash = row.inputHash();
      if (! srcRow.rowHash().equals(expectHash))
        return rn;
    }
    return 0L;
  }
  
  
  
  /**
   * Closes the instance by closing the underlying source- and hash-ledgers.
   */
  public void close() {
    try (TaskStack closer = new TaskStack(this)) {
      closer.pushClose(srcLedger).pushClose(hashLedger);
   }
  }
  
  
  
  /**
   * Returns the number of rows in the (append-only) source ledger that haven't yet
   * been added to the hash ledger.
   *  
   * @return &ge; 0
   * 
   * @throws IllegalStateException
   *         if {@linkplain #sourceLedgerSize()} &lt; {@linkplain #hashLedgerSize()}
   */
  public long rowsPending() throws IllegalStateException {
    long pending = srcLedger.size() - hashLedger.size();
    checkPending(pending);
    return pending;
  }
  
  
  private void checkPending(long pending) {
    if (pending < 0) {
      throw new IllegalStateException("missing rows in source ledger: " + -pending);
    }
  }
  
  
  
  
  /**
   * Returns the number of rows in the {@linkplain HashLedger hash ledger}.
   */
  public final long hashLedgerSize() {
    return hashLedger.size();
  }
  

  /**
   * Returns the number of rows in the {@linkplain SourceLedger source ledger}.
   */
  public final long sourceLedgerSize() {
    return srcLedger.size();
  }
  
  
  public long update() {
    return update(0, null);
  }
  
  
  
  
  public long update(int commitSlack, Ticker progress) {
    if (state.needsMending())
      throw new IllegalStateException("Ledger needs mending. State: " + state);
    if (commitSlack < 0)
      throw new IllegalArgumentException("commitSlack " + commitSlack);
    
    long lastCommit = hashLedger.size();
    long lastSrcRow = srcLedger.size();
    checkPending(lastSrcRow - lastCommit);
    long lastTargetCommit = lastSrcRow - commitSlack;
    
    final long count = lastTargetCommit - lastCommit;
    if (count <= 0)
      return 0;
    
    Ticker ticker = progress == null ? Ticker.NOOP : progress;
    
    SkipLedger skipLedger = hashLedger.getSkipLedger();
    for (long nextRow = lastCommit + 1; nextRow <= lastTargetCommit; ++nextRow) {
      SourceRow srcRow = srcLedger.getSourceRow(nextRow);
      skipLedger.appendRows(srcRow.rowHash());
      ticker.tick();
    }
    
    return count;
  }
  
  
  
  public WitnessReport witness() {
    return hashLedger.witness();
  }
  
  
  
  public File writeMorselFile(File target, List<Long> rowNumbers, String note) throws IOException {
    var builder = loadBuilder(rowNumbers, note);
    return MorselFile.createMorselFile(target, builder);
  }
  
  
  public int writeMorselFile(WritableByteChannel ch, List<Long> rowNumbers, String note) throws IOException {
    Objects.requireNonNull(ch, "null channel");
    var builder = loadBuilder(rowNumbers, note);
    return MorselFile.writeMorselFile(ch, builder);
  }
  
  
  
  private MorselPackBuilder loadBuilder(List<Long> rowNumbers, String note) {
    Objects.requireNonNull(rowNumbers, "null rowNumbers");
    final long maxRow = hashLedgerSize();
    LedgerMorselBuilder builder = new LedgerMorselBuilder(hashLedger, note);
    for (long rn : rowNumbers) {
      SkipLedger.checkRealRowNumber(rn);
      if (rn > maxRow)
        throw new IllegalArgumentException("rowNumber " + rn + " out-of-bounds; max " + maxRow);
      SourceRow srcRow = srcLedger.getSourceRow(rn);
      builder.addSourceRow(srcRow);
    }
    return builder;
  }
  
  
  /**
   * @return {@code Logger.getLogger(LOG_NAME)}
   * @see #LOG_NAME
   */
  protected Logger log() {
    return Logger.getLogger(LOG_NAME);
  }
  

}
