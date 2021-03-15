/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.db;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.Ledger;
import io.crums.sldg.SldgConstants;
import io.crums.util.Lists;

/**
 * 
 */
public class SortedBagBuilder {
  
  
  
  
  
  public SortedBag createBag(Ledger ledger, List<Long> rowNumbers) {
    return createBag(ledger, rowNumbers, true);
  }
  
  
  /**
   * Creates and returns a new bag.
   * 
   * @param ledger the source
   * @param rowNumbers the row numbers for which full row information will be available
   * @param copy determines if {@code rowNumbers} is defensively copied
   * @return
   */
  public SortedBag createBag(Ledger ledger, List<Long> rowNumbers, boolean copy) {
    if (Objects.requireNonNull(rowNumbers, "null rowNumbers").isEmpty())
      throw new IllegalArgumentException("empty rowNumbers");
    
    Objects.requireNonNull(ledger, "null ledger");
    
    if (copy)
      rowNumbers = Lists.readOnlyCopy(rowNumbers);
    
    final long lastRowNum = ledger.size();
    final int count = rowNumbers.size();
    
    final long lastRn = rowNumbers.get(count - 1);
    
    // some quick init bounds checks..
    if (lastRowNum < lastRn)
      throw new IllegalArgumentException(
          "last rowNumber out-of-bounds: " + lastRn + " > ledger size "+ lastRowNum);
    
    final long firstRn = rowNumbers.get(0);
    
    if (firstRn < 1)
      throw new IllegalArgumentException("first rowNumber out-of-bounds: " + firstRn);
    
    
    ByteBuffer inputs = ByteBuffer.allocate(count * SldgConstants.HASH_WIDTH);
    long prevRn = 0;
    for (int index = 0; index < count; ++index) {
      long rn = rowNumbers.get(index);
      if (rn <= prevRn)
        throw new IllegalArgumentException(
            "out-of-sequence rowNumber " + rn + " at index " + index + ": " + rowNumbers);
      prevRn = rn;
      ByteBuffer e = ledger.getRow(rn).inputHash();
      inputs.put(e);
    }
    
    assert !inputs.hasRemaining();
    
    inputs.flip();
    
    SortedSet<Long> coverage = Ledger.coverage(rowNumbers).tailSet(1L).headSet(lastRn);
    
    
    ByteBuffer hashes;
    List<Long> hashRns;
    {
      int hashCount = coverage.size() - count + 1;
      if (hashCount == 0) {
        hashes = BufferUtils.NULL_BUFFER;
        hashRns = Collections.emptyList();
      } else {
        hashes = ByteBuffer.allocate(hashCount * SldgConstants.HASH_WIDTH);
        hashRns = new ArrayList<>(hashCount);
        
        int rnIndex = 0;
        long rn = firstRn;
        
        for (Long coveredRn : coverage) {
          if (coveredRn == rn) {
            rn = rowNumbers.get(++rnIndex); // nice it blows if there's a bug
            continue;
          }
          ByteBuffer rowHash = ledger.rowHash(coveredRn);
          
          hashes.put(rowHash);
          hashRns.add(coveredRn);
        }
        
        assert !hashes.hasRemaining();
        
        hashes.flip();
        hashRns = Collections.unmodifiableList(hashRns);
      }
    }
    
    
    
    return new SortedBag(hashRns, rowNumbers, hashes, inputs);
  }

}
