/*
 * Copyright 2020-2021 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.cache.RowCache;
import io.crums.util.Lists;

/**
 * Base implementation. This uses a pluggable storage abstraction
 * {@linkplain SkipTable}.
 * 
 * @see SldgConstants#DIGEST
 */
public class CompactSkipLedger extends SkipLedger {
  
  private final SkipTable table;
  protected final RowCache cache;
  
  public CompactSkipLedger(SkipTable table) {
    this(table, null);
  }
  
  public CompactSkipLedger(SkipTable table, RowCache cache) {
    this.table = Objects.requireNonNull(table, "null table");
    this.cache = cache;
  }
  
  // protected void primeCache() {
  //   if (cache == null)
  //     return;
    
  //   // cache.clearAll();  (since it never did anything)
  //   long lastRn = size();
  //   if (lastRn != 0)
  //     getRow(lastRn);
  // }
  
  @Override
  public final int hashWidth() {
    return HASH_WIDTH;
  }

  @Override
  public final String hashAlgo() {
    return DIGEST.hashAlgo();
  }

  @Override
  public final MessageDigest newDigest() {
    return DIGEST.newDigest();
  }

  @Override
  public final ByteBuffer sentinelHash() {
    return DIGEST.sentinelHash();
  }

  @Override
  public long appendRows(ByteBuffer entryHashes) {
    
    int count = entryHashes.remaining() / HASH_WIDTH;
    if (count == 0 || entryHashes.remaining() % HASH_WIDTH != 0)
      throw new IllegalArgumentException(
          "remaining bytes not a positive multiple of " + HASH_WIDTH + ": " + entryHashes);
    
    if (count == 1)
      return appendRow(entryHashes, table);
    
    TxnTable txn = new TxnTable(table, count);
    
    for (int index = 0, basePos = entryHashes.position(); index < count; ++index) {
      int pos = basePos + index * HASH_WIDTH;
      int limit = pos + HASH_WIDTH;
      appendRow(entryHashes.limit(limit).position(pos), txn);
    }
    
    return txn.commit();
  }
  
  
  private long appendRow(ByteBuffer entryHash, SkipTable table) {
    ByteBuffer ehCopy = entryHash.slice();
    
    final long nextRowNum = table.size() + 1;

    final int levels = skipCount(nextRowNum);
    final List<Long> prevRns = Lists.functorList(
        levels,
        level -> nextRowNum - (1L << level));

    var prevHashes = Lists.map(prevRns, rn -> rowHash(rn, table));
    
    var rowHash = SkipLedger.rowHash(nextRowNum, entryHash, prevHashes);
    
    ByteBuffer tableRow =
        ByteBuffer.allocate(SkipTable.ROW_WIDTH).put(ehCopy).put(rowHash).flip();
    
    return table.addRows(tableRow, nextRowNum - 1);
    
  }

  @Override
  public long size() {
    return table.size();
  }

  @Override
  public Row getRow(long rowNumber) {
    Row row;
    
    if (cache != null) {
      row = cache.getRow(rowNumber);
      if (row != null)
        return row;
      
      row = new LazyRow(rowNumber, table.readRow(rowNumber - 1));
      cache.pushRow(row);
    
    } else
      row = new LazyRow(rowNumber, table.readRow(rowNumber - 1));
    
    return row;
  }

  @Override
  public ByteBuffer rowHash(long rowNumber) {
    if (cache != null) {
      if (rowNumber == 0)
        return sentinelHash();
      SerialRow row = cache.getRow(rowNumber);
      if (row != null)
        return row.hash();
    }
    return rowHash(rowNumber, table);
  }
  
  
  @Override
  public void close() {
    table.close();
  }
  

  @Override
  public void trimSize(long newSize) {
    long size = size();
    if (newSize > size)
      throw new IllegalArgumentException("newSize " + newSize + " > size " + size);
    
    if (newSize < 1)
      throw new IllegalArgumentException("newSize " + newSize + " < 1");
    
    table.trimSize(newSize);
  }
  
  
  
  private ByteBuffer rowHash(long rowNumber, SkipTable table) {
    if (rowNumber == 0)
      return sentinelHash();
    
    ByteBuffer tableRow = table.readRow(rowNumber - 1);
    return tableRow.position(tableRow.position() + hashWidth()).slice();
  }
  
  
  /**
   * Lazy-loading row.
   */
  final class LazyRow extends Row {
    
    private final long rowNumber;
    private final ByteBuffer row;
    
    LazyRow(long rowNumber, ByteBuffer row) {
      this.rowNumber = rowNumber;
      this.row = row.remaining() == row.capacity() ? row : row.slice();

      assert rowNumber > 0 && row.remaining() == SkipTable.ROW_WIDTH;
    }

    @Override
    public long no() {
      return rowNumber;
    }

    @Override
    public ByteBuffer inputHash() {
      return row.asReadOnlyBuffer().limit(hashWidth());
    }

    @Override
    public ByteBuffer hash() {
      return row.asReadOnlyBuffer().position(hashWidth()).slice();
    }

    @Override
    public ByteBuffer prevHash(int level) {
      Objects.checkIndex(level, prevLevels());
      long referencedRowNum = rowNumber - (1L << level);
      return rowHash(referencedRowNum);
    }
    
  }

}



