/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


/**
 * Classification of ledgers recognized by the morsel format.
 */
public enum LedgerType {
  
  /** A timechain <em>is a ledger</em>. Timechains don't know about any other
   * ledger type; but all the ledger types know about timechains. */
  TIMECHAIN,
  /** An SQL, or SQL-like, read-only view of a ledger. */
  TABLE,
  /** A journal or log-based ledger. A row is typically a line of text with
   * whitespace delimited tokens serving as cell values. */
  LOG,
  /** A stream "composed" of fixed-size blocks. This may be suitable for
   * certain media formats that allow a media player "to pickup" playing
   * from the middle of the stream. */
  BSTREAM;
  
  
  /**
   * {@linkplain #TIMECHAIN}s and {@linkplain #BSTREAM}s only record proofs
   * of their [skip ledger] commitments. With timechains, their <em>crumtrails</em>
   * are recorded as 2 parts: the block-proof that constitutes the state of the
   * timechain and the cargo-proof (the proof asserting the witnessed hash was
   * used to compute the corresponding timechain-block's input hash) recorded
   * as an <em>annotation in another ledger</em>.
   * 
   * @return
   */
  public boolean commitsOnly() {
    return this == TIMECHAIN || this == BSTREAM;
  }

}
