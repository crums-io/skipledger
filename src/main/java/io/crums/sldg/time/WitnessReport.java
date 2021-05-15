/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.time;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.client.Client;
import io.crums.client.ClientException;
import io.crums.model.CrumRecord;
import io.crums.sldg.HashLedger;
import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.SldgException;
import io.crums.util.Lists;

/**
 * Summary of a witnessing a {@linkplain HashLedger}.
 */
public final class WitnessReport {
  
  

  
  /**
   * Witnesses unwitnessed rows in the given ledger database and returns a report.
   * 
   * @return {@code witness(hashLedger, true)}
   */
  public static WitnessReport witness(HashLedger hashLedger) {
    return witness(hashLedger, true);
  }
  
  
  /**
   * Witnesses unwitnessed rows in the given ledger database and returns a report.
   * 
   * <h2>Algorithm for which rows are witnessed</h2>
   * <p>
   * TODO
   * </p>
   * 
   * @param includeLast if {@code true}
   */
  public static WitnessReport witness(HashLedger hashLedger, boolean includeLast) {
    
    
    final long unwitnessedRows;
    {
      long lastWitNum = hashLedger.lastWitnessedRowNumber();
      final long size = hashLedger.getSkipLedger().size();
      unwitnessedRows = size - lastWitNum;
      
      if (unwitnessedRows == 0)
        return new WitnessReport(lastWitNum);
      
      else if (unwitnessedRows < 0)
        throw new SldgException(
            "size/lastWitnessNum : " + hashLedger.getSkipLedger().size() + "/" + lastWitNum);
    }
    
    int toothExponent;
    {
      int p = 1;
      for (; (unwitnessedRows >> p) > SldgConstants.DEF_TOOTHED_WIT_COUNT; ++p);
      toothExponent = p - 1;
    }
    
    return witness(hashLedger, toothExponent, true);
  }
  
  
  /**
   * Witnesses unwitnessed rows in the given ledger database and returns a report.
   * 
   * @param db the ledger database.
   * @param exponent witness rows numbered a multiple of 2<sup>exponent<sup>
   * @param includeLast if {@code true}, then the last unwitnessed row is also witnessed
   */
  public static WitnessReport witness(HashLedger hashLedger, int exponent, boolean includeLast) {
    
    if (exponent < 0)
      throw new IllegalArgumentException("negative exponent " + exponent);
    if (exponent > SldgConstants.MAX_WITNESS_EXPONENT)
      throw new IllegalArgumentException("out-of-bounds exponent " + exponent);
    
    
    
    final SkipLedger ledger = hashLedger.getSkipLedger();
    
    final long lastRowNum = ledger.size();
    final long lastWitNum = hashLedger.lastWitnessedRowNumber();
    if (lastWitNum == lastRowNum)
      return new WitnessReport(lastWitNum);
    
    final long witNumDelta = 1L << exponent;

    final boolean includeUntoothed = lastRowNum % witNumDelta != 0 && includeLast;
    
    List<Row> rowsToWitness;
    {
      ArrayList<Row> rows = new ArrayList<>(SldgConstants.MAX_BLOCK_WITNESS_COUNT);
      final int maxLoopCount =
          includeUntoothed ? SldgConstants.MAX_BLOCK_WITNESS_COUNT : SldgConstants.MAX_BLOCK_WITNESS_COUNT - 1;
      for (
          long toothedNum = ((lastWitNum + witNumDelta) / witNumDelta) * witNumDelta;
          // i.e. toothedNum % witNumDelta == 0
          toothedNum <= lastRowNum && rows.size() < maxLoopCount;
          toothedNum += witNumDelta) {
        
        rows.add(ledger.getRow(toothedNum));
      }
      
      if (includeUntoothed)
        rows.add(ledger.getRow(lastRowNum));
      
      
      if (rows.isEmpty())
        return new WitnessReport(lastWitNum);
            
      rowsToWitness = Lists.reverse(rows);
    }
    

    Client remote = new Client();
    rowsToWitness.forEach(r -> remote.addHash(r.hash()));
    
    List<CrumRecord> records = remote.getCrumRecords();
    
    List<WitnessRecord> zip = zip(records, rowsToWitness);
    
    SortedSet<WitnessRecord> trailedRecords = filterTrailed(zip);
    
    long witnessedRowNumber = 0;
    
    List<WitnessRecord> stored = new ArrayList<>(trailedRecords.size());
    
    for (WitnessRecord trailed : trailedRecords) {
      
      final long rn = trailed.row().rowNumber();
      
      assert rn > lastWitNum;
      
      if (rn <= witnessedRowNumber) {
        assert rn != witnessedRowNumber;
        continue;
      }
      
      hashLedger.addTrail(trailed);
      stored.add(trailed);
    }
    
    stored = stored.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(stored);
    return new WitnessReport(Lists.reverse(zip), stored, lastWitNum);
  }
  
  

  private static List<WitnessRecord> zip(List<CrumRecord> records, List<Row> rowsToWitness) {

    if (records.size() != rowsToWitness.size())
      throw new ClientException(
          "response length mismatch: expected " + rowsToWitness.size() +
          "; actual was " + records.size());
    
    ArrayList<WitnessRecord> zip = new ArrayList<>(records.size());
    for (int index = 0; index < records.size(); ++index)
      zip.add(new WitnessRecord( rowsToWitness.get(index), records.get(index)) );
    return zip;
  }
  

  private static SortedSet<WitnessRecord> filterTrailed(Collection<WitnessRecord> records) {
    TreeSet<WitnessRecord> trailed = new TreeSet<>();
    for (WitnessRecord record : records)
      if (record.isTrailed())
        trailed.add(record);
    return trailed;
  }
  
  
  
  
  private final long localQueryTime = System.currentTimeMillis();
  
  private final List<WitnessRecord> records;
  private final List<WitnessRecord> stored;
  
  private final long prevLastWitNum;
  
  
  
  
  private WitnessReport(long prevLastWitNum) {
    records = stored = Collections.emptyList();
    this.prevLastWitNum = prevLastWitNum;
  }
  
  
  private WitnessReport(
      List<WitnessRecord> records, List<WitnessRecord> stored, long prevLastWitNum) {
    this.records = records;
    this.stored = stored;
    this.prevLastWitNum = prevLastWitNum;
  }
  
  /**
   * Returns the list of witnessed records.
   */
  public List<WitnessRecord> getRecords() {
    return records;
  }
  
  
  /**
   * Returns the list of witnessed records that were stored.
   * 
   * @return immutable ordered list
   */
  public List<WitnessRecord> getStored() {
    return stored;
  }
  
  
  public boolean nothingDone() {
    return records.isEmpty();
  }
  
  
  /**
   * Returns the highest previously witnessed (and recorded) row number.
   */
  public long prevLastWitNum() {
    return prevLastWitNum;
  }
  
  
  
  /**
   * Determines if <em>any</em> witness {@linkplain #getRecords() record} in this report
   * indicates an the object (hashes) was witnessed before this query. This is an inherently fuzzy idea,
   * subject to races, network speed, host clock skew, etc. 
   * 
   * @return {@code seenBefore(3 seconds before local query time)}
   * @see #localQueryTime()
   */
  public boolean seenBefore() {
    return seenBefore(localQueryTime - 3_000);
  }
  
  
  
  /**
   * Determines if <em>any</em> witness {@linkplain #getRecords() record} in this report
   * indicates an of the object (hashes) was witnessed before the specifed date.
   * Determines if <em>any</em> witness {@linkplain #getRecords() records} in this report
   * indicate any of the objects (hashes) were witnessed before the specifed date.
   * 
   * @param utc
   */
  public boolean seenBefore(long utc) {
    return records.stream().anyMatch(r -> r.utc() < utc);
  }
  
  
  public long localQueryTime() {
    return localQueryTime;
  }
  
  
}
