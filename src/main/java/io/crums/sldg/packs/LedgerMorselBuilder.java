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
  
  
  
  public boolean addSourceRow(SourceRow srcRow) throws HashConflictException {
    long rn = Objects.requireNonNull(srcRow, "null srcRow").rowNumber();
    synchronized (lock()) {
      checkRowNum(rn);
      SkipLedger skipLedger = ledger.getSkipLedger();
      if (!skipLedger.getRow(rn).inputHash().equals(srcRow.rowHash()))
          throw new HashConflictException("srcRow hash conflicts with skipledger's row-input hash");
      
      TrailedRow trailed = ledger.nearestTrail(rn);
      EasyList<Long> targets = new EasyList<>(3);
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
  

  
  
  
  private void checkRowNum(long rowNumber) {
    SkipLedger.checkRealRowNumber(rowNumber);
    if (rowNumber > hi())
      throw new IllegalArgumentException("rowNumber " + rowNumber + " > ledger size " + hi());
  }

}


















