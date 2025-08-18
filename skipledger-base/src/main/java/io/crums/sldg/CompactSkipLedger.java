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
  private final boolean fastTable;
  
  
  /**
   * Constructs an instance with no caching. The storage layer is set to
   * "not-fast".
   * 
   * @param table       storage layer (assumed <em>not fast</em>)
   */
  public CompactSkipLedger(SkipTable table) {
    this(table, null, false);
  }
  
  /**
   * Constructs an instance with optional caching. The storage layer is set to
   * "not-fast".
   * 
   * @param table       storage layer (assumed <em>not fast</em>)
   * @param cache       optional (may be {@code null})
   */
  public CompactSkipLedger(SkipTable table, RowCache cache) {
    this(table, cache, false);
  }
  
  /**
   * Full constructor.
   * 
   * @param table       storage layer
   * @param cache       optional (may be {@code null})
   * @param fastTable   if {@code true}, then <em>en bloc</em> commits (see
   *                    {@linkplain #commitRows(long, ByteBuffer)}) are <em>not
   *                    </em> first written to an in-memory transaction table
   */
  public CompactSkipLedger(SkipTable table, RowCache cache, boolean fastTable) {
    this.table = Objects.requireNonNull(table, "null table");
    this.cache = cache;
    this.fastTable = fastTable;
  }
  
  
  
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
  protected final void writeRowsImpl(long startRn, ByteBuffer inputHashes) {
    
    final int count = hashCount(inputHashes);
    
    if (count == 1) {
      writeRow(inputHashes, table, startRn);
      return;
    }
    
    TxnTable txn;
    SkipTable t;
    if (fastTable) {
      txn = null;
      t = table;
    } else {
      txn = new TxnTable(table, startRn - 1L, count);
      t = txn;
    }
    
    long rn = startRn;
    for (
        int index = 0, basePos = inputHashes.position();
        index < count;
        ++index, ++rn) {
      
      int pos = basePos + index * HASH_WIDTH;
      int limit = pos + HASH_WIDTH;
      writeRow(inputHashes.limit(limit).position(pos), t, rn);
    }
    
    if (!fastTable)
      txn.commit();
  }
  
  
  
  
  private void writeRow(ByteBuffer inputHash, SkipTable table, final long rowNo) {
    
    final int levels = skipCount(rowNo);
    final List<Long> prevRns = Lists.functorList(
        levels,
        level -> rowNo - (1L << level));

    var prevHashes = Lists.map(prevRns, rn -> rowHash(rn, table));
    
    var rowHash = SkipLedger.rowHash(rowNo, inputHash.slice(), prevHashes);
    
    ByteBuffer tableRow =
        ByteBuffer.allocate(SkipTable.ROW_WIDTH)
        .put(inputHash.slice()).put(rowHash).flip();
    
    table.writeRows(tableRow, rowNo - 1);
    
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
    public ByteBuffer inputHash() {
      return row.asReadOnlyBuffer().limit(hashWidth());
    }

    @Override
    public ByteBuffer hash() {
      return row.asReadOnlyBuffer().position(hashWidth()).slice();
    }

    @Override
    public LevelsPointer levelsPointer() {
      List<ByteBuffer> levelHashes =
          Lists.functorList(
              skipCount(rowNumber),
              lvl -> rowHash(rowNumber - (1L << lvl)));
      return new LevelsPointer(rowNumber, levelHashes);
    }
    
  }

}



