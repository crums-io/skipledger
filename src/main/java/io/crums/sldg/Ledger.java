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

import io.crums.sldg.cache.HashFrontier;
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
 * <h2>Single Thread Model</h2>
 * <p>
 * An instance is an <em>oberserver</em> if {@linkplain SourceLedger} state. It cannot
 * control or enforce the append-only protocol the source-ledger is supposed to observe,
 * so it can only report if the source-ledger fails to do so. This requires an inherent
 * single-thread model. 
 * </p><p>
 * Furthermore, since instances update their {@linkplain HashLedger}s, it's a bad idea
 * to run 2 or the same instance concurrently against the same backing data.
 * </p>
 * 
 * @see SourceLedger
 * @see HashLedger
 * @see State
 */
public class Ledger implements AutoCloseable {
  
  /**
   * Logger name. Forks and trims are logged as warnings; fatal errors (e.g. tampered hashes
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
  
  /**
   * There are 2 columns in a skipledger's internal table: input-hash and row-hash.
   * If we suspect someone other than this program may have somehow modified the
   * the table, then this value is {@code true}. In that event, we must also
   * check the input-hash "column" in the skip ledger jives with its row-hash column.
   */
  private final boolean trustHashLedger;
  
  private Ticker ticker = Ticker.NOOP;
  private int validateCheckpointSize = 1000;
  
  private State state;
  private long firstConflict;
  
  
  /**
   * Creates an instance that <em>owns</em> both arguments. The hash-ledger is
   * trusted. There's no progress ticker.
   * 
   * @param srcLedger the source
   * @param hashLedger the hash ledger matching the source
   * 
   * @see #setProgressTicker(Ticker)
   * @see #close()
   */
  public Ledger(SourceLedger srcLedger, HashLedger hashLedger) {
    this(srcLedger, hashLedger, null, true);
  }

  /**
   * Creates an instance that <em>owns</em> both arguments.
   * 
   * @param srcLedger the source
   * @param hashLedger the hash ledger matching the source
   * @param progress optional progress ticker, ticked once per source-row
   * @param trustHashLedger if {@code false}, then on checks, the hash ledger
   *                        is also verified not to have been tampered with or otherwise broken
   * 
   * @see #checkEndRows() Invokes {@code checkEndRows()}
   * @see #setProgressTicker(Ticker)
   * @see #close()
   */
  public Ledger(SourceLedger srcLedger, HashLedger hashLedger, Ticker progress, boolean trustHashLedger) {
    this.srcLedger = Objects.requireNonNull(srcLedger, "null srcLedger");
    this.hashLedger = Objects.requireNonNull(hashLedger, "null hashLedger");
    this.trustHashLedger = trustHashLedger;
    setProgressTicker(progress);
    checkEndRows();
  }
  
  
  public SourceLedger getSourceLedger() {
    return srcLedger;
  }
  
  
  public HashLedger getHashLedger() {
    return hashLedger;
  }
  
  
  public boolean trustHashLedger() {
    return trustHashLedger;
  }
  
  
  /**
   * Sets the progress ticker. Do not set under concurrent access.
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
   * Returns the last known valid row. It may not in fact be valid.
   * <p>
   * The semantics of this method seem less strange when you consider the following:
   * every existing row in the ledger is assumed to be valid until demonstrated otherwise.
   * </p>
   * 
   * @return  the size of the hash-ledger, if the state is {@linkplain State#ok() OK};
   *          {@linkplain #getFirstConflict()}{@code  - 1}, if {@linkplain State#FORKED forked};
   *          the source-ledger size if {@linkplain State#TRIMMED trimmed}
   */
  public long lastValidRow() {
    if (state.ok())
      return hashLedger.size();
    if (state.isForked())
      return firstConflict - 1;
    else  // (isTrimmed)
      return srcLedger.size();
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
      throw emptySourceException().fillInStackTrace();
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
  
  
  private SldgException emptySourceException() {
    return new SldgException(
        "illegal configuration: source-ledger " + srcLedger +
        " is empty, while hash-ledger " + hashLedger + " is not");
  }
  
  
  private long setFork(long conflict) {
    if (conflict == 0)
      return 0L;
    
    else if (conflict == 1)
      failRowOne();
    
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
   * @throws HashConflictException every time
   */
  private void failRowOne() throws HashConflictException {
    throw new HashConflictException(
        "illegal configuration: source-ledger " + srcLedger + " and hash-ledger " +
        hashLedger + " conflict at row [1]");
  }
  
  
  
  /**
   * Checks the given range of row numbers for conflicts and returns the first conflicting
   * row number; zero if none are found. If a conflict is found, the state is set to
   * {@linkplain State#FORKED FORKED}, and the {@linkplain #getFirstConflict() first conflict}ing
   * row number is set to it (if it was previously zero or greater than the returned number).
   * 
   * @param lo &ge; 1
   * @param hi &le; {@linkplain #lesserSize()}
   * @param asc if {@code false}, then the range is checked in descending order; ascending, o.w.
   * @param progress optional progress ticker
   * 
   * @return the first conflicting row number found; 0 (zero) if none found
   */
  public long checkRowRange(long lo, long hi) {
    checkRangeArgs(lo, hi);
    
    long conflict = firstConflictInRange(lo, hi);
    return setFork(conflict);
  }
  
  
  
  public State validateState() {
    return validateState(this.trustHashLedger);
  }
  
  
  public State validateState(boolean trustHashLedger) {
    
    final long hLedgeSz = hashLedger.size();
    final long sLedgeSz = srcLedger.size();
    final long hi = Math.min(hLedgeSz, sLedgeSz);
    
    if (hi == 0) {
      if (hLedgeSz != 0)
        throw emptySourceException().fillInStackTrace();
      state = sLedgeSz == 0 ? State.COMPLETE : State.PENDING;
      firstConflict = 0;
      return state;
    }
    
    // set the state provisionally
    firstConflict = 0;
    if (hLedgeSz == sLedgeSz)
      state = State.COMPLETE;
    else if (hLedgeSz < sLedgeSz)
      state = State.PENDING;
    else
      state = State.TRIMMED;
    

    if (!trustHashLedger) {
      long conflict = firstConflictInRange(1, hi, true);
      setFork(conflict);
      return state;
    }
    
    var skipLedger = hashLedger.getSkipLedger();
    var digest = SldgConstants.DIGEST.newDigest();
    HashFrontier frontier;
    {
      ByteBuffer inputHash = srcLedger.getSourceRow(1L).rowHash();
      ticker.tick();
      frontier = HashFrontier.firstRow(inputHash, digest);

      if (! skipLedger.rowHash(1L).equals(frontier.frontierHash()))
        failRowOne();
    }
    
    var goodFrontier = frontier;
    for (long rn = 2; rn <= hi; ) {
      
      // compute the hash of row [checkRn] from source ledger only
      // (represented as the frontier hash)
      final long checkRn = Math.min(rn + validateCheckpointSize, hi);
      for (; rn <= checkRn; ++rn) {
        ByteBuffer inputHash = srcLedger.getSourceRow(rn).rowHash();
        ticker.tick();
        frontier = frontier.nextFrontier(inputHash, digest);
      }
      
      // verify this hash matches the one in the ledger
      if (skipLedger.rowHash(checkRn).equals(frontier.frontierHash())) {
        goodFrontier = frontier;
      } else {
        // ahh.. the hash for row [checkRn] doesn't checkout
        // narrow down which it must be..
        long conflict = firstConflictInRange(goodFrontier, skipLedger, checkRn, false);
        assert conflict != 0;
        setFork(conflict);
        break;
      }
    }
    
    
    return state;
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
   * row number is set to it (if it was previously zero or greater than the returned number).
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
      return checkRowRange(lo, hi);
    
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
  
  /**
   * Returns the first given row number where <em>input-hash</em> in the hash-ledger conflicts
   * with the source's row-hash; zero if none conflict.
   */
  private long firstConflict(Iterator<Long> rows) {
    SkipLedger skipLedger = hashLedger.getSkipLedger();
    
    while (rows.hasNext()) {
      long rn = rows.next();
      SourceRow srcRow = srcLedger.getSourceRow(rn);
      ticker.tick();
      Row row = skipLedger.getRow(rn);
      
      // check the skip ledger input hash matches the row hash
      ByteBuffer expectHash = row.inputHash();
      if (! srcRow.rowHash().equals(expectHash))
        return rn;
    }
    return 0L;
  }
  
  private HashFrontier prepareFrontier(SkipLedger skipLedger, long lo) {
    HashFrontier frontier;
    if (lo == 1) {
      ByteBuffer inputHash = srcLedger.getSourceRow(1L).rowHash();
      ticker.tick();
      frontier = HashFrontier.firstRow(inputHash);
      if (conflict(inputHash, frontier, skipLedger, false))
        failRowOne();
    } else
      frontier = HashFrontier.loadFrontier(skipLedger, lo - 1);
    
    return frontier;
  }
  
  
  private long firstConflictInRange(long lo, long hi) {
    return firstConflictInRange(lo, hi, this.trustHashLedger);
  }
  
  
  private long firstConflictInRange(long lo, long hi, boolean trustHashLedger) {
    var skipLedger = hashLedger.getSkipLedger();
    HashFrontier frontier = prepareFrontier(skipLedger, lo);
    return firstConflictInRange(frontier, skipLedger, hi, trustHashLedger);
  }
  
  
  private long firstConflictInRange(HashFrontier frontier, SkipLedger skipLedger, long hi, boolean trustHashLedger) {

    var digest = SldgConstants.DIGEST.newDigest();
    
    for (long rn = frontier.rowNumber() + 1; rn <= hi; ++rn) {
      ByteBuffer inputHash = srcLedger.getSourceRow(rn).rowHash();
      ticker.tick();
      frontier = frontier.nextFrontier(inputHash, digest);
      if (conflict(inputHash, frontier, skipLedger, trustHashLedger))
        return rn;
    }
    
    return 0L;
  }
  
  
  private boolean conflict(ByteBuffer inputHash, HashFrontier frontier, SkipLedger skipLedger, boolean trustHashLedger) {
    long rn = frontier.rowNumber();
    boolean ok;
    if (trustHashLedger) {
      ok = skipLedger.rowHash(rn).equals(frontier.frontierHash());
    } else {
      Row row = skipLedger.getRow(rn);
      ok =
          row.inputHash().equals(inputHash.clear()) &&
          row.hash().equals(frontier.frontierHash());
    }
    
    return !ok;
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
  
  
  /**
   * Updates the hash-ledger with new inputs from the source-ledger.
   * 
   * @return the number of rows appended to the hash-ledger
   */
  public long update() {
    return update(Long.MAX_VALUE);
  }
  
  
  

  /**
   * Updates the hash-ledger with new inputs from the source-ledger.
   * 
   * @param maxNewRows the maximum number of rows to record from the source-ledger
   * 
   * @return the number of rows appended to the hash-ledger
   */
  public long update(long maxNewRows) {
    if (state.needsMending())
      throw new IllegalStateException("Ledger needs mending. State: " + state);
    
    if (maxNewRows <= 0)
      return 0L;
    
    final long lastCommit = hashLedger.size();
    final long lastSrcRow = srcLedger.size();
    checkPending(lastSrcRow - lastCommit);
    
    
    final long lastTargetCommit;
    {
      long max = lastCommit + maxNewRows;
      lastTargetCommit = max < 0 ? lastSrcRow : Math.min(lastSrcRow, max);
    }
    
    SkipLedger skipLedger = hashLedger.getSkipLedger();
    for (long nextRow = lastCommit + 1; nextRow <= lastTargetCommit; ++nextRow) {
      SourceRow srcRow = srcLedger.getSourceRow(nextRow);
      skipLedger.appendRows(srcRow.rowHash());
      ticker.tick();
    }
    
    return lastTargetCommit - lastCommit;
  }
  
  
  
  public WitnessReport witness() {
    return hashLedger.witness();
  }
  
  
  public long lastWitnessedRowNumber() {
    return hashLedger.lastWitnessedRowNumber();
  }
  
  
  public long unwitnessedRowCount() {
    return hashLedger.unwitnessedRowCount();
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
