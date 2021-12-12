/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.util.Objects;

import io.crums.sldg.HashConflictException;
import io.crums.sldg.HashLedger;
import io.crums.sldg.Path;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.EasyList;

/**
 * 
 */
public class LedgerMorselBuilder extends MorselPackBuilder {
  
  public final static String DEFAULT_PREAMBLE = "state";
  
  private final HashLedger ledger;

  /**
   * 
   */
  public LedgerMorselBuilder(HashLedger ledger, String preamble) {
    this.ledger = Objects.requireNonNull(ledger, "null ledger");
    if (preamble == null || preamble.isEmpty())
      preamble = "state";
    initPath(ledger.getSkipLedger().statePath(), preamble);
  }
  
  
  /**
   * Adds the given source row. Skip ledger rows are added as necessary
   * in order to connect this row both to row [1] and row [{@linkplain #hi()}].
   * <h3>Note</h3>
   * <p>
   * Before version {@code 0.0.4}, we only required each source row in a morsel
   * be only connected to the hi row number. From a proof-of-membership standpoint,
   * this was sufficient, since the highest row <em>must</em> be connected to row [1].
   * However, this makes transforming morsels unnecessarily difficult--and all for
   * meager gains in space.
   * </p>
   * <p><pre>
   * TODO: optimize. If adding a lot of source rows, a lot of roundtrips to
   *       the skip ledger can be avoided if a <em>list</em> of rows were taken
   *       as argument.
   *       
   *       Additionally, there's much we that can optimized right here in this
   *       implementation. (Get it right first.)
   * </pre>
   * </p>
   */
  public boolean addSourceRow(SourceRow srcRow) throws HashConflictException {
    final long rn = Objects.requireNonNull(srcRow, "null srcRow").rowNumber();
    synchronized (lock()) {
      final long hi = hi();
      final long lo = lo();   // (it's always 1, currently.. but it needn't be so)
      checkRowNum(rn, lo, hi);
      SkipLedger skipLedger = ledger.getSkipLedger();
      if (!skipLedger.getRow(rn).inputHash().equals(srcRow.rowHash()))
          throw new HashConflictException("srcRow hash conflicts with skipledger's row-input hash");
      
      // fill in the necessary skip ledger rows
      TrailedRow trailed = ledger.nearestTrail(rn);
      EasyList<Long> targets = new EasyList<>(4);
      if (rn > lo) {
        // the rule is that every full row
        // is linked from [hi] *and (new requirement)
        // links to [lo]
        
        targets.add(lo);
      }
      targets.add(rn);
      if (trailed != null && trailed.rowNumber() != rn)
        targets.add(trailed.rowNumber());
      if (targets.last() != hi())
        targets.add(hi());
      
      Path path = ledger.getSkipLedger().getPath(targets);
      addPath(path);
      
      if (trailed != null)
        trailPackBuilder.addTrail(trailed.rowNumber(), trailed.trail());
      return sourcePackBuilder.addSourceRow(srcRow);
    }
    
    
    
  }
  

  
  
  
  private void checkRowNum(long rowNumber, long lo, long hi) {
    if (hi == 0)
      throw new IllegalStateException("builder not initialized. rowNumber: " + rowNumber);
    if (rowNumber > hi)
      throw new IllegalArgumentException("rowNumber " + rowNumber + " > builder hi: " + hi);
    if (rowNumber < lo)
      throw new IllegalArgumentException("rowNumber " + rowNumber + " < builder lo " + hi);
  }

}


















