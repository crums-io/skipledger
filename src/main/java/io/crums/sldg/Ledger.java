/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.io.File;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.packs.LedgerMorselBuilder;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.time.WitnessReport;
import io.crums.util.TaskStack;

/**
 * A <em>source</em>-ledger paired with a <em>hash</em>-ledger that tracks it.
 * 
 */
public class Ledger implements AutoCloseable {
  
  private final SourceLedger srcLedger;
  private final HashLedger hashLedger;

  /**
   * Constructor creates an instance that <em>owns</em> both arguments.
   * 
   * @param srcLedger the source
   * @param hashLedger the hash ledger matching the source
   * 
   * @see #close()
   */
  public Ledger(SourceLedger srcLedger, HashLedger hashLedger) {
    this.srcLedger = Objects.requireNonNull(srcLedger, "null srcLedger");
    this.hashLedger = Objects.requireNonNull(hashLedger, "null hashLedger");
    
    try {
      rowsPending();
    } catch (IllegalStateException isx) {
      throw new IllegalArgumentException(
          isx.getMessage() + ", hash ledger size is" + hashLedger.size());
    }
          
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
    if (pending < 0)
      throw new IllegalStateException("missing rows in source ledger: " + -pending);
  }
  
  
  /**
   * Validates the entire hash ledger. Note this is linear (2X) in the number of rows, so if it's
   * a big ledger this may take a while.
   * 
   * @see #validateRows(long, long)
   */
  public final void validateRows() {
    validateRows(0, hashLedger.size());
  }
  
  
  /**
   * Validates the rows in the given range.
   * 
   * @param lo the low bound row number &ge; 1
   * @param hi the high bound row number &ge; {@code lo} and &le; {@linkplain #hashLedgerSize()}
   * 
   * @throws IllegalStateException
   *         if {@linkplain #sourceLedgerSize()} &lt; {@linkplain #hashLedgerSize()}
   * @throws HashConflictException
   *         if the {@linkplain SourceRow#rowHash() hash} of a {@linkplain SourceRow source-row}
   *         does not match the {@linkplain Row#inputHash() input-hash} in the
   *         {@linkplain SkipLedger skip ledger}; or if the hash-pointers in the skip ledger are
   *         not self-consistent.
   */
  public final void validateRows(long lo, long hi) throws IllegalStateException, HashConflictException {
    SkipLedger.checkRealRowNumber(lo);
    if (hi < lo)
      throw new IllegalArgumentException("lo " + lo + "; hi " + hi);
    long size = hashLedger.size();
    if (hi > size)
      throw new IllegalArgumentException("lo " + lo + "; hi " + hi + "; hash ledger size " + size);
    
    rowsPending();
    
    SkipLedger skipLedger = hashLedger.getSkipLedger();
    for (long rn = lo; rn <= hi; ++rn) {
      SourceRow srcRow = srcLedger.getSourceByRowNumber(rn);
      Row hashRow = skipLedger.getRow(rn);
      if (!srcRow.rowHash().equals(hashRow.inputHash()))
        throw new HashConflictException(
            "on row [" + rn + "]; source-row: " + srcRow.safeDebugString());
      for (long refRn : hashRow.coveredRowNumbers()) {
        if (!hashRow.hash(refRn).equals(skipLedger.rowHash(refRn)))
            throw new HashConflictException(
                "skip ledger is corrupted on row [" + rn + "]: ref-row [" + refRn + "]");
      }
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
    return update(0);
  }
  
  
  
  
  public long update(int commitSlack) {
    if (commitSlack < 0)
      throw new IllegalArgumentException("commitSlack " + commitSlack);
    long lastCommit = hashLedger.size();
    long lastSrcRow = srcLedger.size();
    checkPending(lastSrcRow - lastCommit);
    long lastTargetCommit = lastSrcRow - commitSlack;
    
    final long count = lastTargetCommit - lastCommit;
    if (count <= 0)
      return 0;
    
    for (long nextRow = lastCommit + 1; nextRow <= lastTargetCommit; ++nextRow) {
      SourceRow srcRow = srcLedger.getSourceByRowNumber(nextRow);
      hashLedger.getSkipLedger().appendRows(srcRow.rowHash());
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
      SourceRow srcRow = srcLedger.getSourceByRowNumber(rn);
      builder.addSourceRow(srcRow);
    }
    return builder;
  }
  

}
