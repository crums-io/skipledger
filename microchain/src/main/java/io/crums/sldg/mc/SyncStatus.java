/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc;


import java.util.Optional;
import java.util.function.Predicate;

/**
 * Synchronization-status of source-ledger with its commitment chain.
 * 
 * @see #rowsCommitted()
 * @see #rowsInSource()
 * @see #state()
 * @see #hasError()
 * @see #rowsPending()
 */
public final class SyncStatus {
  
  
//  public static SyncStatus createComplete(long rowCount) {
//    if (rowCount < 0L)
//      throw new IllegalArgumentException("negative rowCount: " + rowCount);
//    
//    return new SyncStatus(SyncState.COMPLETE, rowCount, rowCount, 0L);
//  }
//  
//  public static SyncStatus createPending(
//      long rowsCommitted, long rowsInSource) {
//    
//    if (rowsCommitted < 0L)
//      throw new IllegalArgumentException(
//          "negative rowsCommitted: " + rowsCommitted);
//    if (rowsInSource <= rowsCommitted)
//      throw new IllegalArgumentException(
//          "rowsInSource (%d) must be greater than rowsCommitted (%d)"
//          .formatted(rowsInSource, rowsCommitted));
//    
//    return
//        new SyncStatus(SyncState.PENDING, rowsCommitted, rowsInSource, 0L);
//  }
  
  
  public static SyncStatus createOk(long rowsCommitted, long rowsInSource) {
    
    if (rowsCommitted < 0L)
      throw new IllegalArgumentException(
          "negative rowsCommitted: " + rowsCommitted);
    if (rowsInSource < rowsCommitted)
      throw new IllegalArgumentException(
        "rowsInSource (%d) must be greater than or equal to rowsCommitted (%d)"
        .formatted(rowsInSource, rowsCommitted));
    
    return 
        new SyncStatus(
            rowsInSource == rowsCommitted ? SyncState.COMPLETE : SyncState.PENDING,
            rowsCommitted, rowsInSource, 0L);
  }
  
  
  public static SyncStatus createTrimmed(
      long rowsCommitted, long rowsInSource) {
    
    if (rowsInSource < 0L)
      throw new IllegalArgumentException(
          "negative rowsInSource: " + rowsInSource);
    
    if (rowsCommitted <= rowsInSource)
      throw new IllegalArgumentException(
          "rowsInSource (%d) must be less than rowsCommitted (%d)"
          .formatted(rowsInSource, rowsCommitted));
    
    return
        new SyncStatus(SyncState.TRIMMED, rowsCommitted, rowsInSource, 0L);
  }
  
  
  public static SyncStatus createForked(
      long rowsCommitted, long rowsInSource, long forkNo) {
    
    if (forkNo < 2L)
      throw new IllegalArgumentException("forkNo must be 2 or greater: " + forkNo);
    if (rowsCommitted < forkNo)
      throw new IllegalArgumentException(
          rowsCommitted < 0L ?
              "negative rowsCommitted: " + rowsCommitted :
                "forkNo (%d) greater than rowsCommitted (%d)"
                .formatted(forkNo, rowsCommitted));
    if (rowsInSource < forkNo)
      throw new IllegalArgumentException(
          rowsInSource < 0L ?
              "negative rowsInSource: " + rowsInSource :
                "forkNo (%d) greater than rowsInSource (%d)"
                .formatted(forkNo, rowsInSource));
    
    return
        new SyncStatus(SyncState.FORKED, rowsCommitted, rowsInSource, forkNo);
  }
  
  
  private final SyncState syncState;
  private final long rowsCommitted;
  private final long rowsInSource;
  private final long forkNo;
  
  
  private SyncStatus(
      SyncState syncState,
      long rowsCommitted,
      long rowsInSource,
      long forkNo) {
    this.syncState = syncState;
    this.rowsCommitted = rowsCommitted;
    this.rowsInSource = rowsInSource;
    this.forkNo = forkNo;
  }
  
  /**
   * Returns the number of rows in the source-ledger (which has been
   * trimmed since it was last committed).
   */
  public Optional<Long> trimNo() {
    return getValue(SyncState::isTrimmed, rowsInSource);
  }
  
  
  /**
   * Returns the first (lowest) row number at which the source-ledger 
   * conflicts with its already committed value in the chain. Needless,
   * to say, this is not good: someone (or some-process) has broken the
   * cardinal rule that ledger rows may never be modified.
   */
  public Optional<Long> forkNo() {
    return getValue(SyncState::isForked, forkNo);
  }
  
  
  public long lastValidCommit() {
    return syncState.ok() ? rowsCommitted : trimNo().orElse(forkNo - 1L);
  }
  
  /**
   * Returns the number of rows pending to be committed. The returned
   * number is only present if the instance has no error.
   */
  public Optional<Long> rowsPending() {
    return getValue(SyncState::ok, rowsInSource - rowsCommitted);
  }
  
  /**
   * Returns the number of rows committed to the skipledger chain.
   */
  public long rowsCommitted() {
    return rowsCommitted;
  }
  
  /**
   * Returns the number of rows in the source-ledger.
   */
  public long rowsInSource() {
    return rowsInSource;
  }
  
  
  
  
  /**
   * Returns {@code true} if there's a synchronization error.
   * 
   * @return {@code state().hasError()}
   */
  public boolean hasError() {
    return syncState.hasError();
  }
  
  /**
   * Returns {@code true} if all rows in the source-ledger are
   * committed to the skipledger chain.
   * 
   * @return {@code state().isComplete()}
   */
  public boolean isComplete() {
    return syncState.isComplete();
  }
  
  
  /**
   * Returns the sync-state.
   * 
   * @see SyncState
   */
  public SyncState state() {
    return syncState;
  }
  
  
  
  private Optional<Long> getValue(Predicate<SyncState> predicate, long value) {
    return predicate.test(syncState) ? Optional.of(value) : Optional.empty();
  }
  
  
  /** Member-wise equality (<em>value</em> semantics). */
  @Override
  public boolean equals(Object o) {
    return o == this ||
        o instanceof SyncStatus other &&
        other.syncState == syncState &&
        other.rowsCommitted == rowsCommitted &&
        other.rowsInSource == rowsInSource &&
        other.forkNo == forkNo;
  }
  
  
  @Override
  public int hashCode() {
    int hash = Long.hashCode(rowsCommitted);
    hash = hash * 31 + Long.hashCode(rowsInSource);
    hash = hash * 31 + Long.hashCode(forkNo);
    return hash ^ syncState.hashCode();
  }
  
}














