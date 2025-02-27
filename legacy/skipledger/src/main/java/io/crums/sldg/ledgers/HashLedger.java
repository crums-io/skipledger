/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.ledgers;

import io.crums.client.ClientException;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.time.WitnessReport;

/**
 * An opaque skip ledger, but with time (witness) information annotated.
 */
public interface HashLedger extends WitnessedRowRepo {
  

  
  
  /**
   * Returns the skip ledger.
   */
  SkipLedger getSkipLedger();
  
  

  /**
   * Returns the number of rows in the ledger.
   */
  default long size() {
    return getSkipLedger().size();
  }
  
  
  
  

  /**
   * Returns the number of unwitnessed rows in the ledger.
   * 
   * @return {@code size() - lastWitnessedRowNumber()}
   */
  default long unwitnessedRowCount() {
    return size() - lastWitnessedRowNumber();
  }
  
  
  /**
   * @return {@code WitnessReport.witness(this)}
   * @see WitnessReport#witness(HashLedger)
   */
  default WitnessReport witness() {
    return WitnessReport.witness(this);
  }
  

  /**
   * @return {@code WitnessReport.witness(this, includeLast)}
   * @see WitnessReport#witness(HashLedger, boolean)
   */
  default WitnessReport witness(boolean includeLast) throws ClientException {
    return WitnessReport.witness(this, includeLast);
  }
  

  /**
   * @return {@code WitnessReport.witness(this, exponent, includeLast)}
   * @see WitnessReport#witness(HashLedger, int, boolean)
   */
  default WitnessReport witness(int exponent, boolean includeLast) throws ClientException {
    return WitnessReport.witness(this, exponent, includeLast);
  }
  
  /**
   * Trims the ledger to the new given size. <em>Optional operation.</em>
   */
  default void trimSize(long newSize) {
    throw new UnsupportedOperationException("on trimSize(" + newSize + ")");
  }

}
