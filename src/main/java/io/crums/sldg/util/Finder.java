/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.util;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.util.Lists;

/**
 * Finds entries in a ledger.
 */
public class Finder {
  
  /**
   * Compares 2 rows firstly by their {@linkplain Row#inputHash() input hash}es,
   * and secondly by their row numbers as tie-breakers. 
   */
  public final static Comparator<Row> ENTRY_NUMBER_COMP = new Comparator<Row>() {

    @Override
    public int compare(Row a, Row b) {
      ByteBuffer aEntry = a.inputHash();
      ByteBuffer bEntry = b.inputHash();
      int comp = aEntry.compareTo(bEntry);
      return comp == 0 ? Long.compare(a.rowNumber(), b.rowNumber()) : comp;
    }
  };


  public final static int DEFAULT_LIMIT = 8;
  
  
  private final SkipLedger ledger;
  
  

  /**
   * Creates an instance with a <tt>ledger</tt>.
   */
  public Finder(SkipLedger ledger) {
    this.ledger = Objects.requireNonNull(ledger, "null ledger");
  }
  

  
  

  /**
   * Returns an ordered list of rows with matching entry hashes starting frow row 1.
   * 
   * @param inputHashPrefix has remaining bytes but no more than 32
   * 
   * @return non-null (but possibly empty) list of rows in {@linkplain #ENTRY_NUMBER_COMP} order.
   */
  public List<Row> byInputPrefix(ByteBuffer inputHashPrefix) {
    return byInputPrefix(inputHashPrefix, 1);
  }
  
  

  /**
   * Returns an ordered list of rows with matching entry hashes.
   * 
   * @param inputHashPrefix has remaining bytes but no more than 32
   * @param startRowNumber  inclusive
   * 
   * @return non-null (but possibly empty) list of rows in {@linkplain #ENTRY_NUMBER_COMP} order.
   */
  public List<Row> byInputPrefix(ByteBuffer inputHashPrefix, long startRowNumber) {
    return byInputPrefix(inputHashPrefix, startRowNumber, DEFAULT_LIMIT);
  }
  
  
  /**
   * Returns an ordered list of rows with matching entry hashes.
   * 
   * @param inputHashPrefix has remaining bytes but no more than 32
   * @param startRowNumber  inclusive
   * @param limit           the maximum size of the returned list (&ge; 1)
   * 
   * @return non-null (but possibly empty) list of rows in {@linkplain #ENTRY_NUMBER_COMP} order.
   */
  public List<Row> byInputPrefix(ByteBuffer inputHashPrefix, long startRowNumber, int limit) {
    return byInputPrefix(inputHashPrefix, startRowNumber, ledger.size() + 1, limit);
  }
  
  
  /**
   * Returns an ordered list of rows with matching entry hashes.
   * 
   * @param inputHashPrefix has remaining bytes but no more than 32
   * @param startRowNumber  inclusive
   * @param endRowNumber    <em>exclusive</em>
   * @param limit           the maximum size of the returned list (&ge; 1)
   * 
   * @return non-null (but possibly empty) list of rows in {@linkplain #ENTRY_NUMBER_COMP} order.
   */
  public List<Row> byInputPrefix(
      ByteBuffer inputHashPrefix, long startRowNumber, long endRowNumber, int limit) {
    
    final int prefixSize =
        Objects.requireNonNull(inputHashPrefix, "null entryHashPrefix").remaining();
    
    if (prefixSize == 0 || prefixSize > SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException(
          "entryHashPrefix has " + prefixSize + " remaining bytes");
    
    if (startRowNumber <= 0)
      throw new IllegalArgumentException(
          "startRowNumber " + startRowNumber);
    if (limit <= 0)
      throw new IllegalArgumentException("limit " + limit);
    if (endRowNumber <= startRowNumber) {
      if (endRowNumber == startRowNumber)
        return Collections.emptyList();
      else
        throw new IllegalArgumentException(
            "startRowNumber " + startRowNumber + " >= endRowNumber " + endRowNumber);
    }
    
    long size = ledger.size();
    
    if (startRowNumber > size)
      return Collections.emptyList();

    endRowNumber = Math.min(endRowNumber, size + 1);
    ArrayList<Row> hits = new ArrayList<>(Math.min(8, limit));
    for (long rn = startRowNumber; rn < endRowNumber; ++rn) {
      Row row = ledger.getRow(rn);
      ByteBuffer entryHash = row.inputHash();
      entryHash.limit(entryHash.position() + prefixSize);
      if (entryHash.equals(inputHashPrefix)) {
        hits.add(row);
        if (hits.size() == limit)
          break;
      }
    }
    
    switch (hits.size()) {
    case 0:
      return Collections.emptyList();
    case 1:
      return Collections.singletonList(hits.get(0));
    }
    
    Row[] rows = hits.toArray(new Row[hits.size()]);
    Arrays.sort(rows, ENTRY_NUMBER_COMP);
    
    return Lists.asReadOnlyList(rows);
  }
  
  
  
  /**
   * Returns the ledger this instance searches.
   */
  public SkipLedger getLedger() {
    return ledger;
  }

}
