/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.time;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
   * By at most ({@linkplain SldgConstants#DEF_TOOTHED_WIT_COUNT}) or 9 unwitnessed
   * rows are chosen to be witnessed. With the possible exception of the last row,
   * these rows are numbered at multiples of some power of 2. This is motivated by the
   * following:
   * <ol>
   * <li>Rows with higher powers of 2 show up more commonly in our hash proofs. Therefore
   * a trail for that row enjoys efficiencies for a greater number of use cases.</li>
   * <li>If new rows in the ledger are constantly in flux (constantly being appended to
   * the ledger and getting witnessed), then this scheme allows a stable way to query for
   * witnessed rows. (Recall it's a 2 step process: it takes minutes to retrieve a new crumtrail.)</li>
   * </ol>
   * </p>
   * 
   * <h2>Algorithm for which trails (witnessed rows) are stored</h2>
   * <p>
   * As a general rule, trails are stored in order of <em>both</em> row number and
   * non-decreasing UTC witness-time. That is, the row number is always increasing, and the time is
   * never decreasing. (It is possible to contrive a set up where higher row numbers were
   * somehow witnessed before lower ones, but our storage model disallows such cases.)
   * </p><p>
   * However not all trails are stored in the database. If 2 or more trails share the same
   * witness time, then at most 2 trails with that witness time are stored:
   * <ol>
   * <li>The trail of the row with the most hash pointers (determined by the row number).</li>
   * <li>The trail of the row with the highest row number.</li>
   * </ol>
   * In some cases the same row may satisfy both conditions, so that if there are 2 or more rows
   * witnessed at the same time, only 1 them (the last) is stored.
   * </p>
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
      toothExponent = p;
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

    final boolean includeUntoothed = includeLast && lastRowNum % witNumDelta != 0;
    
    final List<Row> rowsToWitness;
    {
      ArrayList<Row> rows = new ArrayList<>(SldgConstants.MAX_BLOCK_WITNESS_COUNT);
      final int maxLoopCount =
          includeUntoothed ? SldgConstants.MAX_BLOCK_WITNESS_COUNT : SldgConstants.MAX_BLOCK_WITNESS_COUNT - 1;
      for (
          long toothedNum = ((lastWitNum + witNumDelta) / witNumDelta) * witNumDelta;
          // i.e. toothedNum % witNumDelta == 0
          // PS this is key (!) .. we witness row numbers that are multiples
          //    of 2^exponent
          toothedNum <= lastRowNum && rows.size() < maxLoopCount;
          toothedNum += witNumDelta) {
        
        rows.add(ledger.getRow(toothedNum));
      }
      
      if (includeUntoothed)
        rows.add(ledger.getRow(lastRowNum));
      
      
      if (rows.isEmpty())
        return new WitnessReport(lastWitNum);
            
      rowsToWitness = Collections.unmodifiableList(rows);
    }
    

    Client remote = new Client();
    rowsToWitness.forEach(r -> remote.addHash(r.hash()));
    
    List<CrumRecord> records = remote.getCrumRecords();
    
    List<WitnessRecord> zip = zip(records, rowsToWitness);
    
    SortedSet<WitnessRecord> trailedRecords = filterTrailed(zip);
    
    List<WitnessRecord> stored = new ArrayList<>(trailedRecords.size());
    WitnessRecord last = null; // monotonically increasing row number
    boolean lastSkipped = false;
    for (WitnessRecord trailed : trailedRecords) {
      
      final long rn = trailed.rowNum();
      
      if (last != null && rn <= last.rowNum()) {
        assert rn != last.rowNum();
        continue;
      }
      
      if (last != null && fuzzyTimeDiff(last, trailed) == 0) {
        lastSkipped = true;
        last = trailed;
        continue;
      }
      
      if (lastSkipped) {
        hashLedger.addTrail(last);
        stored.add(last);
        lastSkipped = false;
      }
      
      last = trailed;
      
      hashLedger.addTrail(trailed);
      stored.add(trailed);
    }
    
    if (lastSkipped && hashLedger.addTrail(last))
      stored.add(last);
    
    stored = stored.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(stored);
    return new WitnessReport(zip, stored, lastWitNum);
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
  

  private final static int FUZZ_MILLIS = 10_000;
  
  private final static int fuzzyTimeDiff(WitnessRecord a, WitnessRecord b) {
    return (int) (a.utc() / FUZZ_MILLIS - (b.utc() / FUZZ_MILLIS ));
  }
  
  private static Comparator<WitnessRecord> TRAILED_REC_COMP = new Comparator<>() {
    @Override
    public int compare(WitnessRecord a, WitnessRecord b) {
      if (a == b)
        return 0;
      // the first witnessed comes first, but only if by a sufficient margin (2 sec)
      // (we work in modulo FUZZ_MILLIS in order to maintain transititity:
      // so the margin works out to something like 2.5 +/- 0.5 sec, actually)
      int comp = fuzzyTimeDiff(a, b);
      if (comp != 0)
        return comp;
      // the row with more skip pointers comes first
      comp = b.row().prevLevels() - a.row().prevLevels();
      if (comp != 0)
        return comp;
      // the greater row number comes first
      return Long.compare(b.rowNum(), a.rowNum());
    }
  };
  
  

  private static SortedSet<WitnessRecord> filterTrailed(Collection<WitnessRecord> records) {
    TreeSet<WitnessRecord> trailed = new TreeSet<>(TRAILED_REC_COMP);
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
