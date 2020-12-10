/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.db;


import static io.crums.sldg.SldgConstants.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.client.Client;
import io.crums.client.ClientException;
import io.crums.client.repo.TrailRepo;
import io.crums.io.FileUtils;
import io.crums.model.CrumRecord;
import io.crums.sldg.Ledger;
import io.crums.sldg.Row;
import io.crums.sldg.SldgException;
import io.crums.util.IntegralStrings;

/**
 * 
 */
public class Db {
  
  public final static int MAX_BLOCK_WITNESS_COUNT = 65;
  
  
  public final static int MAX_WITNESS_EXPONENT = 62;
  
  
  
  
  private final File dir;
  private final Ledger ledger;
  private final TrailRepo witnessRepo;

  /**
   * 
   */
  public Db(File dir, boolean readOnly) throws IOException {
    this.dir = Objects.requireNonNull(dir, "null directory");
    if (!readOnly)
      FileUtils.ensureDir(dir);
    
    this.ledger = new CompactFileLedger(new File(dir, DB_LEDGER), readOnly);
    this.witnessRepo = new TrailRepo(dir, readOnly);
    
    // TODO: do some sanity checks we loaded plausible files
    //       delaying cuz it's sure break anyway, if so.
  }
  
  
  
  
  public File getDir() {
    return dir;
  }
  
  
  public Ledger getLedger() {
    return ledger;
  }


  public long lastWitnessNum() {
    List<Long> ids = witnessRepo.getIds();
    return ids.isEmpty() ? 0 : ids.get(ids.size() - 1);
  }
  
  
  public int witness(int exponent, boolean includeLast) {

    if (exponent < 0)
      throw new IllegalArgumentException("negatve exponent " + exponent);
    if (exponent > MAX_WITNESS_EXPONENT)
      throw new IllegalArgumentException("out of bounds exponent " + exponent);
    
    final long lastRowNum = ledger.size();
    
    final long lastWitNum = lastWitnessNum();
    
    sanityCheckWitNum(lastWitNum, lastRowNum);
    
    final long witNumDelta = 1L << exponent;

    final boolean includeUntoothed = lastRowNum % witNumDelta != 0 && includeLast;
    
    
    
    ArrayList<Row> rowsToWitness = new ArrayList<>(MAX_BLOCK_WITNESS_COUNT);
    {
      final int maxLoopCount =
          includeUntoothed ? MAX_BLOCK_WITNESS_COUNT : MAX_BLOCK_WITNESS_COUNT - 1;
      for (
          long toothedNum = lastWitNum + witNumDelta;
          toothedNum <= lastRowNum && rowsToWitness.size() < maxLoopCount;
          toothedNum += witNumDelta) {
        
        rowsToWitness.add(ledger.getRow(toothedNum));
      }
      
      if (includeUntoothed)
        rowsToWitness.add(ledger.getRow(lastRowNum));
    }
    
    
    Client remote = new Client();
    rowsToWitness.forEach(r -> remote.addHash(r.hash()));
    
    List<CrumRecord> records = remote.getCrumRecords();
    
    sanityCheckRemote(records, rowsToWitness);
    
    SortedSet<CrumRecord> trailedRecords = filterTrailed(records);
    
    Map<ByteBuffer, Row> hashMappedRows = toHashMappedRows(rowsToWitness);
    
    long witnessedRowNumber = 0;
    int count = 0;
    for (CrumRecord trailed : trailedRecords) {
      ByteBuffer rowHash = trailed.crum().hash();
      Row row = hashMappedRows.get(rowHash);
      if (row == null)
        throw new SldgException(
            "assertion failure (1) on hash lookup " + IntegralStrings.toHex(rowHash));
      
      long rn = row.rowNumber();
      
      if (rn <= witnessedRowNumber) {
        if (rn == witnessedRowNumber)
          throw new SldgException("assertion failure (2) row number " + rn);
        else
          continue;
      }
      witnessedRowNumber = rn;
      witnessRepo.putTrail(trailed.trail(), witnessedRowNumber);
      ++count;
    }
    return count;
  }
  
  
  private Map<ByteBuffer, Row> toHashMappedRows(ArrayList<Row> rowsToWitness) {
    HashMap<ByteBuffer, Row> map = new HashMap<>(rowsToWitness.size());
    rowsToWitness.forEach(r -> map.put(r.hash(), r));
    return map;
  }




  private SortedSet<CrumRecord> filterTrailed(List<CrumRecord> records) {
    TreeSet<CrumRecord> trailed = new TreeSet<>(REC_UTC_COMP);
    for (CrumRecord record : records)
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
  
  
  private void sanityCheckRemote(List<CrumRecord> records, List<Row> rowsToWitness) {
    if (records.size() != rowsToWitness.size())
      throw new ClientException(
          "response length mismatch: expected " + rowsToWitness.size() +
          "; actual was " + records.size());
    
    for (int index = 0; index < records.size(); ++index) {
      CrumRecord record = records.get(index);
      Row row = rowsToWitness.get(index);
      if (!row.hash().equals(record.crum().hash()))
          throw new ClientException(
              "hash mismatch from remote at index " + index +
              ": " + record + " / " + row);
    }
  }
  
  
  private final static Comparator<CrumRecord> REC_UTC_COMP = new Comparator<CrumRecord>() {

    @Override
    public int compare(CrumRecord a, CrumRecord b) {
      return Long.compare(a.crum().utc(), b.crum().utc());
    }
  };

}
































