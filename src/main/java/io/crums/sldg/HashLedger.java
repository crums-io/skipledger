/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import io.crums.client.ClientException;
import io.crums.model.CrumTrail;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.sldg.time.WitnessReport;

/**
 * An opaque skip ledger, but with time (witness) information annotated.
 */
public interface HashLedger extends AutoCloseable {
  
  
  /**
   * <p>Does not throw checked exceptions.</p>
   * 
   * {@inheritDoc}
   */
  void close();
  
  
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
   * Adds the crumtrail in given witness record, if it's not already saved.
   * 
   * @param trailedRecord a <em>trailed</em> record (i.e. {@linkplain WitnessRecord#isTrailed()
   *        trailedRecord.isTrailed()} returns {@code true})
   *        
   * @return {@code true} if it wasn't already saved
   */
  boolean addTrail(WitnessRecord trailedRecord);
  
  
  /**
   * Returns the number of rows that have crumtrails.
   * 
   * @return &ge; 0
   */
  int getTrailCount();
  
  
  /**
   * Returns the trailed row by index.
   * 
   * @param index &ge; 0 and &lt; {@linkplain #getTrailCount()}
   */
  TrailedRow getTrailByIndex(int index);
  
  
  /**
   * Returns the nearest witnessed row <em>on or after</em> the given row number, if any; {@code null} o.w.
   * A non-null return value is evidence how <em>old</em> the given row is (since it must have been created
   * before it was witnessed).
   * 
   * @param rowNumber &ge; 1
   */
  TrailedRow nearestTrail(long rowNumber);
  
  
  /**
   * Returns the last row number witnessed, if any; 0 (zero) otherwise. Here,
   * a witnessed row is just a ledger row with a {@linkplain CrumTrail} annotation
   * (represented as a {@linkplain TrailedRow}).
   */
  long lastWitnessedRowNumber();
  
  

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
