/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import io.crums.client.Client;
import io.crums.client.ClientException;
import io.crums.client.repo.TrailRepo;
import io.crums.io.FileUtils;
import io.crums.model.CrumRecord;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.TaskStack;
import io.crums.util.ledger.Constants;
import io.crums.util.ledger.DirectLedger;
import io.crums.util.ledger.LedgerNavigator;
import io.crums.util.ledger.Row;

/**
 * 
 */
public class Db implements Closeable {
  
  public final static int MAX_BLOCK_WITNESS_COUNT = 65;
  
  
  public final static int MAX_WITNESS_EXPONENT = 62;
  
  private final File dir;

  
  private final LedgerNavigator ledgerNavigator;
  
  private final TrailRepo witnessRepo;
  
  
  
  public Db(File dir) throws IOException {
    this(dir, false);
  }
  
  public Db(File dir, boolean readOnly) throws IOException {
    this.dir = Objects.requireNonNull(dir, "null directory");
    if (!readOnly)
      FileUtils.ensureDir(dir);

    this.ledgerNavigator = new LedgerNavigator(
        new DirectLedger(
            new File(dir, Constants.DB_LEDGER), readOnly) );
    
    this.witnessRepo = new TrailRepo(dir);
  }
  
  
  
  
  public File getDir() {
    return dir;
  }
  
   
  
  
  
  
  public long putEntryHash(ByteBuffer entryHash) throws UncheckedIOException {
    return ledgerNavigator.getLedger().appendRow(entryHash);
  }  
  
  
  public long putEntryHashes(ByteBuffer entryHashes) throws UncheckedIOException {
    return ledgerNavigator.getLedger().appendRowsEnBloc(entryHashes);
  }
  
  
  public ByteBuffer getEntryHash(long rowNumber) throws UncheckedIOException {
    return ledgerNavigator.getRow(rowNumber).entryHash();
  }
  
  
  
  public int witnessNewRows(int exponent) {
    return witnessNewRows(exponent, false);
  }
  
   
  /**
   * 
   * @param exponent &ge; 0 and &le; 62
   * 
   * @return the number of rows witnessed
   * 
   * @see #MAX_WITNESS_EXPONENT
   */
  public int witnessNewRows(int exponent, boolean includeLast) {
    if (exponent < 0)
      throw new IllegalArgumentException("negatve exponent " + exponent);
    if (exponent > MAX_WITNESS_EXPONENT)
      throw new IllegalArgumentException("out of bounds exponent " + exponent);
    
    final long lastRowNum = ledgerNavigator.size();
    
    
    if (lastRowNum == 0)
      return 0;
    
    final long lastWitNum;
    {
      List<Long> ids = witnessRepo.getIds();
      lastWitNum = ids.isEmpty() ? 0 : ids.get(ids.size() - 1);
      
      // sanity check
      if (lastWitNum < 0)
        throw new SldgException(
            "negative id " + lastWitNum +
            " at witnessRepo index " + (ids.size() - 1));
      else if (lastWitNum > lastRowNum)
        throw new SldgException(
            "last witnessed row number " + lastWitNum +
            " > last row number " + lastRowNum);
    }
    
    final long witNumDelta = 1L << exponent;
    
    
    final long maxDnum;
    {
      long max = (lastRowNum / witNumDelta) * witNumDelta;
      maxDnum = max >= lastWitNum ? 0 : max;
    }
    
    includeLast &= maxDnum != lastRowNum;
    
    final long minDnum;
    
    if (maxDnum == 0)
      minDnum = 0;
    else {
      long min = ((lastWitNum + witNumDelta) / witNumDelta) * witNumDelta;
      if (min >= maxDnum)
        minDnum = maxDnum;
      else {
        int maxCount = MAX_BLOCK_WITNESS_COUNT - (includeLast ? 2 : 1);
        long minMin = maxDnum - (maxCount - 1) * witNumDelta;
        if (minMin > min)
          min = minMin;
        minDnum = min;
      }
    }
    
    Client remote = new Client();
    
    ArrayList<Row> rowsToWitness = new ArrayList<>();
    
    if (maxDnum != 0) {
      for (long rowNumber = minDnum; rowNumber <= maxDnum; rowNumber+= witNumDelta) {
        Row row = ledgerNavigator.getRow(rowNumber);
        rowsToWitness.add(row);
        remote.addHash(row.rowHash());
      }
    }
    
    if (includeLast) {
      Row row = ledgerNavigator.getRow(lastRowNum);
      rowsToWitness.add(row);
      remote.addHash(row.rowHash());
    }
    
    
    List<CrumRecord> records = remote.getCrumRecords();
    
    if (records.size() != rowsToWitness.size())
      throw new ClientException(
          "response length mismatch: expected " + rowsToWitness.size() +
          "; actual was " + records.size());
    
    RowRecord[] rr = new RowRecord[records.size()];
    for (int index = 0; index < rr.length; ++index)
      rr[index] = new RowRecord(rowsToWitness, records, index);
    
    Arrays.sort(rr);  // this is a stable sort
    
    boolean outOfSeq;
    {
      List<ByteBuffer> expected = Lists.map(rowsToWitness, r -> r.rowHash());
      List<ByteBuffer> actual = Lists.map(records, r -> r.crum().hash());
      outOfSeq = expected.equals(actual);
      if (outOfSeq)
        getLogger().warning(
            "out-of-sequence crumtrails detected: usually a result of having " +
            "previously run at a lower exponent. Invoked exponent is " + exponent);
    }
    
    
    
    long maxRowNumberWitnessed = 0;
    int count = 0;
    
    for (int index = 0; index < rr.length; ++index) {
      
      RowRecord rowRecord = rr[index];
      if (!rowRecord.isTrailed())
        break;
      
      if (rowRecord.rowNumber() <= maxRowNumberWitnessed) {
        assert rowRecord.rowNumber() != maxRowNumberWitnessed;
        continue;
      }
      
      witnessRepo.putTrail(rowRecord.record.trail(), rowRecord.rowNumber());
      maxRowNumberWitnessed = rowRecord.rowNumber();
      ++count;
    }
    
    
    
    return count;
  }
  

  @SuppressWarnings("resource")
  @Override
  public void close() throws IOException {
    new TaskStack(this).pushClose(witnessRepo)
      .pushClose((Closeable) ledgerNavigator.getLedger()).close();
  }
  
  
  protected Logger getLogger() {
    return Logger.getLogger(Db.class.getName());
  }
  
  
  
  private static class RowRecord implements Comparable<RowRecord> {
    final Row row;
    final CrumRecord record;
    
    RowRecord(List<Row> rows, List<CrumRecord> records, int index) {
      this.row = rows.get(index);
      this.record = records.get(index);
      
      ByteBuffer expected = row.entryHash();
      ByteBuffer actual = record.crum().hash();
      if (!expected.equals(actual))
        throw new ClientException(
            "hash mismatch at index " + index +
            ": expected " + IntegralStrings.toHex(expected) +
            " but actual was " + IntegralStrings.toHex(actual));
    }

    @Override
    public int compareTo(RowRecord o) {
      int comp = Long.compare(record.crum().utc(), o.record.crum().utc());
      
      // sanity the server contract, namely that its untrailed records
      // always occur *after its trailed records
      if (comp != 0 && (isTrailed() != o.isTrailed())) {
        if (comp > 0) {
          if (isTrailed())
            throw new ClientException(
                "trailed/untrailed: " + record.crum() + ", " + o.record.crum());
        } else {
          if (o.isTrailed())
            throw new ClientException(
                "trailed/untrailed: " + o.record.crum() + ", " + record.crum());
        }
      }
      return comp;
    }
    
    long rowNumber() {
      return row.rowNumber();
    }
    
    boolean isTrailed() {
      return record.isTrailed();
    }
  }
  

}





















