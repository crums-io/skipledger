/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 
 */
public class PathPackTest extends RowBagTest {

  @Override
  protected PathPack newBag(SkipLedger ledger, List<Long> rowNumbers) {
    
    var refOnlyRns = SkipLedger.refOnlyCoverage(rowNumbers).tailSet(1L);
    
    ByteBuffer refHashes = ByteBuffer.allocate(
        refOnlyRns.size() * SldgConstants.HASH_WIDTH);
    
    
    refOnlyRns.forEach(rn -> refHashes.put(ledger.rowHash(rn)));
    
    assert !refHashes.hasRemaining();
    
    refHashes.flip();
    
    ByteBuffer inputHashes = ByteBuffer.allocate(
        rowNumbers.size() * SldgConstants.HASH_WIDTH);
    
    rowNumbers.forEach(rn -> inputHashes.put(ledger.getRow(rn).inputHash()));
    
    assert !inputHashes.hasRemaining();
    
    inputHashes.flip();
    
    return new PathPack(rowNumbers, refHashes, inputHashes);
  }

}
