/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc;


import java.util.ConcurrentModificationException;

import io.crums.sldg.SkipLedger;
import io.crums.sldg.src.SourceLedger;
import io.crums.sldg.src.SourceRow;

/**
 * 
 */
public class Microchain {
  
  /**
   * Creates and returns an instance in "fix-mode".
   * 
   * @param ledger
   * @param chain
   * @return
   * 
   * @throws MicrochainException
   *         
   */
  public static Microchain openFixMode(SourceLedger ledger, SkipLedger chain)
      throws MicrochainException {
    
    return new Microchain(ledger, chain, true);
  }
  
  public final static int DEFAULT_MAX_FORK_LOOKBACKS = 10;
  
  
  protected final SourceLedger ledger;
  protected final SkipLedger chain;
  
  private boolean fixMode;
  
  
  private int maxForkLookbacks = DEFAULT_MAX_FORK_LOOKBACKS;
  private SyncStatus status;
  
  
  /**
   * Constructs an instance.
   * 
   * @param ledger      the source-ledger
   * @param chain       skipledger commitment chain
   * 
   * @throws MicrochainException
   *         if {@code ledger} and commitment {@code chain} disagree. If thrown,
   *         then an instance can be created in "fix-mode" with the same
   *         arguments {@link Microchain#openFixMode(SourceLedger, SkipLedger)
   *         Microchain.openFixMode(ledger, chain)}
   */
  public Microchain(SourceLedger ledger, SkipLedger chain)
      throws MicrochainException {
    this(ledger, chain, false);
  }

  /**
   * Full constructor.
   * 
   * @param ledger      the source-ledger
   * @param chain       skipledger commitment chain
   * @param fixMode     if {@code true}, then the commitment {@code chain}
   *                    and ledger are not in sync (acknowledging the error
   *                    condition); otherwise ({@code false}), if {@code ledger}
   *                    and {@code chain} disagree, a
   *                    {@code MicrochainException} is thrown
   *                    
   * 
   * @throws MicrochainException
   *         if {@code ledger} and commitment {@code chain} disagree <em>and</em>
   *         {@code fixMode} is {@code false}
   */
  protected Microchain(SourceLedger ledger, SkipLedger chain, boolean fixMode)
      throws MicrochainException {
    this.ledger = ledger;
    this.chain = chain;
    this.status = computeSyncStatus();
    this.fixMode = fixMode;
    if (status.hasError() && !fixMode) {
      throw new MicrochainException(
          "ledger (%s) and commitment chain (%s) disagree: ledger %s row [%d]"
          .formatted(
              ledger, chain,
              status.state().isForked() ? "forked at" : "trimmed to",
              status.forkNo().orElse(status.trimNo().get())));
    }
    
  }
  
  
  
  
  
  public void setMaxForkLookBacks(int maxLookbacks)
      throws IllegalArgumentException {
    
    if (maxLookbacks < 0)
      throw new IllegalArgumentException(
          "negative maxLookbacks: " + maxLookbacks);
    
    this.maxForkLookbacks = maxLookbacks;
  }
  
  
  public void fixMode(boolean on) {
    fixMode = on;
  }
  
  public final boolean fixMode() {
    return fixMode;
  }
  
  
  /** Returns the status of the ledger relative to its commitment chain. */
  public SyncStatus status() {
    return status;
  }
  
  
  
  public synchronized boolean rollback() throws UnsupportedOperationException {
    checkFixMode("rollback");
    if (!status.hasError())
      return false;
    chain.trimSize(status.lastValidCommit());
    status = SyncStatus.createOk(status.lastValidCommit(), status.rowsInSource());
    return true;
  }
  
  
  
  
  
  public synchronized SyncStatus updateStatus() {
    return status = computeSyncStatus();
  }
  
  
  
  
  public synchronized SyncStatus update(long maxRows) {
    if (updateStatus().hasError())
      throw new MicrochainException(
          "attempt to append to broken commit chain: " + status);
    
    if (maxRows < 0L)
      throw new IllegalArgumentException("negative maxRows");

    if (maxRows == 0L || status.isComplete())
      return status;

    
    final long lastNo;
    { 
      long sum = status.rowsCommitted() + maxRows;
      lastNo = sum < 0L ?
          status.rowsInSource() : Math.min(sum, status.rowsInSource());
    }
    
    
    
    
    for (long rowNo = status.rowsCommitted() + 1; rowNo <= lastNo; ++rowNo) {
      SourceRow srcRow = ledger.getSourceRow(rowNo);
      chain.appendRows(srcRow.hash());
    }
    
    guardOutsideRace(lastNo);
    
    return status = SyncStatus.createOk(lastNo, status.rowsInSource());
  }
  
  
  
  
  
  
  
  
  
  
  
  
  // TODO: make skipledger idempotent so this not an issue
  private void guardOutsideRace(long expectedChainSize) {
    long actualSize = chain.size();
    if (expectedChainSize != actualSize)
      throw new ConcurrentModificationException(
          "commitment chain size modified from outside: expected %d; actual %d"
          .formatted(expectedChainSize, actualSize));
  }
  
  
  
  
  private void checkFixMode(String operation)
      throws UnsupportedOperationException {
    if (!fixMode)
      throw new UnsupportedOperationException(
          operation + " not supported; instance not in fix-mode");
  }
  
  private SyncStatus computeSyncStatus() {
    final long rowsInSource = ledger.size();
    if (rowsInSource < 0L) {
      throw new MicrochainException(
          "bad SourceLedger implementation (%s) returned negative size: %d"
          .formatted(ledger.getClass(), rowsInSource));
    }
    
    final long rowsCommitted = chain.size();
    
    if (rowsCommitted == 0L)
      return SyncStatus.createOk(0, rowsInSource);
    
    assert rowsCommitted > 0L;
    
    if (rowsCommitted <= rowsInSource) {
      if (sourceMatchesCommit(rowsCommitted))
        return SyncStatus.createOk(rowsCommitted, rowsInSource);
      
      long forkNo = lookbackForkNo(rowsCommitted);
      
      return SyncStatus.createForked(rowsCommitted, rowsInSource, forkNo);
    }
    
    
    if (sourceMatchesCommit(rowsInSource))
      return SyncStatus.createTrimmed(rowsCommitted, rowsInSource);
    
    long forkNo = lookbackForkNo(rowsInSource);
    
    return SyncStatus.createForked(rowsCommitted, rowsInSource, forkNo);
  }
  
  
  private long lookbackForkNo(long forkNo) {
    return lookbackForkNo(forkNo, maxForkLookbacks);
  }
  
  private long lookbackForkNo(long forkNo, long maxLookBacks) {
    for (
        long count = maxLookBacks;
        count-- > 0L && forkNo > 1L && !sourceMatchesCommit(forkNo - 1L);
        --forkNo);
    
    return forkNo;
  }
  
  
  private boolean sourceMatchesCommit(long rowNo) {
    var inputHash = chain.getRow(rowNo).inputHash();
    var sourceRowHash = ledger.getSourceRow(rowNo).hash();
    return inputHash.equals(sourceRowHash);
  }
  

}

























