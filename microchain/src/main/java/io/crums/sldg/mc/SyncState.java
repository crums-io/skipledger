/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc;


/**
 * Categorization of "sync" states a microchain can find itself in.
 * 
 * @see #hasError()
 */
public enum SyncState {
  /**
   * The commitment chain and source-ledger are at the
   * same row no.
   */
  COMPLETE,
  /**
   * The commitment chain contains fewer rows than source-ledger.
   */
  PENDING,
  /**
   * The source-ledger has fewer rows than the commitment chain.
   * This an error condition.
   */
  TRIMMED,
  /**
   * A source-ledger row committed to the skipledger chain has changed.
   * This an error condition.
   */
  FORKED;
  
  
  /** @return {@code this == COMPLETE} */
  public boolean isComplete() {
    return this == COMPLETE;
  }

  /** @return {@code this == PENDING} */
  public boolean isPending() {
    return this == PENDING;
  }

  /** @return {@code this == TRIMMED} */
  public boolean isTrimmed() {
    return this == TRIMMED;
  }

  /** @return {@code this == FORKED} */
  public boolean isForked() {
    return this == FORKED;
  }
  
  /** @return {@code isTrimmed() || isForked()} */
  public boolean hasError() {
    return isTrimmed() || isForked();
  }
  
  /** @return {@code !hasError()} */
  public boolean ok() {
    return isPending() || isComplete();
  }
  
}

