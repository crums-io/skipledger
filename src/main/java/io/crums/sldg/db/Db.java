/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.db;


import static io.crums.sldg.SldgConstants.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.client.Client;
import io.crums.client.ClientException;
import io.crums.client.repo.TrailRepo;
import io.crums.io.Opening;
import io.crums.model.CrumRecord;
import io.crums.model.CrumTrail;
import io.crums.sldg.Ledger;
import io.crums.sldg.Nugget;
import io.crums.sldg.Row;
import io.crums.sldg.SkipPath;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.SldgException;
import io.crums.sldg.TrailedPath;
import io.crums.util.Lists;
import io.crums.util.TaskStack;

/**
 * 
 */
public class Db implements Closeable {
  
  private final File dir;
  private final Ledger ledger;
  private final TrailRepo witnessRepo;

  /**
   * 
   */
  public Db(File dir, Opening mode) throws IOException {
    this.dir = Objects.requireNonNull(dir, "null directory");
    
    this.ledger = new CompactFileLedger(new File(dir, DB_LEDGER), mode);
    this.witnessRepo = new TrailRepo(dir, mode);
    
    // TODO: do some sanity checks we loaded plausible files
    //       delaying cuz it's sure break anyway, if so.
  }
  




  @Override
  @SuppressWarnings("resource")
  public void close() {
    new TaskStack(this).pushClose(ledger).pushClose(witnessRepo).close();
  }
  
  
  public File getDir() {
    return dir;
  }
  
  
  public Ledger getLedger() {
    return ledger;
  }
  
  
  
  public long size() {
    return ledger.size();
  }
  
  
  
  
  public Optional<Nugget> getNugget(long rowNumber) {
    Optional<TrailedPath> trailOpt = getTrail(rowNumber);
    if (trailOpt.isEmpty())
      return Optional.empty();
    SkipPath path = ledger.skipPath(rowNumber, ledger.size());
    Nugget nug = new Nugget(path, trailOpt.get());
    return Optional.of(nug);
  }
  
  
  
  /**
   * Returns a <tt>TrailedPath</tt> to the given row number, if there is yet
   * evidence (a {@linkplain CrumTrail}) that it has been witnessed . (There
   * is no evidence that a row number has been witnessed until either that row
   * or a subsequent row is witnessed.)
   * 
   * @param rowNumber &ge; 1 and &lte; {@linkplain #size()}
   */
  public Optional<TrailedPath> getTrail(long rowNumber) {
    
    // find the oldest (smallest) row number that is 
    // greater than or equal to rowNumber
    final int witnessedIndex;
    final long witnessedRowNumber;
    {
      List<Long> witnessedRows = witnessRepo.getIds();
      int wIndex = Collections.binarySearch(witnessedRows, rowNumber);
      if (wIndex < 0) {
        int insertionIndex = -1 - wIndex;
        if (insertionIndex == witnessedRows.size())
          return Optional.empty();
        wIndex = insertionIndex;
      }
      witnessedIndex = wIndex;
      witnessedRowNumber = witnessedRows.get(witnessedIndex);
    }
    
    CrumTrail trail = witnessRepo.getTrail(witnessedIndex);
    SkipPath path = ledger.skipPath(rowNumber, witnessedRowNumber);
    
    return Optional.of(new TrailedPath(path, trail));
  }
  
  
  
  public CrumTrail getCrumTrail(int index) {
    return witnessRepo.getTrail(index);
  }
  
  
  
  
  public List<Long> getRowNumbersWitnessed() {
    return witnessRepo.getIds();
  }
  
  
  
  public long lastWitnessNum() {
    List<Long> ids = witnessRepo.getIds();
    return ids.isEmpty() ? 0 : ids.get(ids.size() - 1);
  }
  
  
  
  public WitnessReport witness() {
    return witness(true);
  }
  
  
  public WitnessReport witness(boolean includeLast) {
    
    
    final long unwitnessedRows;
    {
      long lastWitNum = lastWitnessNum();
      unwitnessedRows = ledger.size() - lastWitNum;
      
      if (unwitnessedRows == 0)
        return new WitnessReport(lastWitNum);
      
      else if (unwitnessedRows < 0)
        throw new SldgException(
            "size/lastWitnessNum : " + ledger.size() + "/" + lastWitNum);
    }
    
    int toothExponent;
    {
      int p = 0;
      for (; unwitnessedRows >> (p + 1) > SldgConstants.DEF_TOOTHED_WIT_COUNT; ++p);
      toothExponent = p;
    }
    
    return witness(toothExponent, true);
  }
  
  
  public WitnessReport witness(int exponent, boolean includeLast) {

    if (exponent < 0)
      throw new IllegalArgumentException("negatve exponent " + exponent);
    if (exponent > SldgConstants.MAX_WITNESS_EXPONENT)
      throw new IllegalArgumentException("out of bounds exponent " + exponent);
    
    final long lastRowNum = ledger.size();
    
    final long lastWitNum = lastWitnessNum();
    
    if (lastWitNum == lastRowNum)
      return new WitnessReport(lastWitNum);
    
    sanityCheckWitNum(lastWitNum, lastRowNum);
    
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
      
      final long rn = trailed.row.rowNumber();
      
      assert rn > lastWitNum;
      
      if (rn <= witnessedRowNumber) {
        assert rn != witnessedRowNumber;
        continue;
      }
      
      witnessedRowNumber = rn;
      witnessRepo.putTrail(trailed.record.trail(), rn);
      stored.add(trailed);
    }
    
    stored = stored.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(stored);
    return new WitnessReport(Lists.reverse(zip), stored, lastWitNum);
  }




  private SortedSet<WitnessRecord> filterTrailed(Collection<WitnessRecord> records) {
    TreeSet<WitnessRecord> trailed = new TreeSet<>();
    for (WitnessRecord record : records)
      if (record.isTrailed())
        trailed.add(record);
    return trailed;
  }


  
  
  private void sanityCheckWitNum(long lastWitNum, long lastRowNum) {

    if (lastWitNum < 0)
      throw new SldgException(
          "last witnessed row number " + lastWitNum + " is negative");
    else if (lastWitNum > lastRowNum)
      throw new SldgException(
          "last witnessed row number " + lastWitNum +
          " > last row number " + lastRowNum);
  }
  
  
  
  
  
  private List<WitnessRecord> zip(List<CrumRecord> records, List<Row> rowsToWitness) {

    if (records.size() != rowsToWitness.size())
      throw new ClientException(
          "response length mismatch: expected " + rowsToWitness.size() +
          "; actual was " + records.size());
    
    ArrayList<WitnessRecord> zip = new ArrayList<>(records.size());
    for (int index = 0; index < records.size(); ++index)
      zip.add(new WitnessRecord( rowsToWitness.get(index), records.get(index)) );
    return zip;
  }
  
  
  
  public final static class WitnessRecord implements Comparable<WitnessRecord> {
    
    private final Row row;
    private final CrumRecord record;
    
    private WitnessRecord(Row row, CrumRecord record) {
      this.row = row;
      this.record = record;
      if (!record.crum().hash().equals(row.hash()))
        throw new ClientException("hash mismatch from remote: " + record + " / " + row);
    }
    
    @Override
    public int compareTo(WitnessRecord o) {
      int comp = Long.compare(utc(), o.utc());
      return comp == 0 ? - Long.compare(rowNum(), o.rowNum()) : comp;
    }
    
    public long utc() {
      return record.crum().utc();
    }
    
    public long rowNum() {
      return row.rowNumber();
    }
    
    public boolean isTrailed() {
      return record.isTrailed();
    }
    
    
    public Row row() {
      return row;
    }
    
    public CrumRecord record() {
      return record;
    }
    
    /**
     * Equality semantics decided solely by {@linkplain #row}.
     */
    public boolean equals(Object o) {
      return o == this || (o instanceof WitnessRecord) && ((WitnessRecord) o).row.equals(row);
    }
    
    public int hashCode() {
      return row.hashCode();
    }
    
  }
  
  
  /**
   * Summary of a ledger {@linkplain Db#witness() witness} action.
   */
  public final static class WitnessReport {
    
    private final List<WitnessRecord> records;
    private final List<WitnessRecord> stored;
    
    private final long prevLastWitNum;
    
    /**
     * Empty instance.
     * 
     * @param prevLastWitNum
     */
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
    
    
  }

}
































