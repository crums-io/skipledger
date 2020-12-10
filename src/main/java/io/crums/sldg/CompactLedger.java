/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import static io.crums.sldg.SldgConstants.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * Base implementation. This uses a pluggable storage abstraction
 * {@linkplain Table}.
 * 
 * @see SldgConstants#DIGEST
 */
public class CompactLedger extends Ledger {
  
  private final Table table;
  
  
  public CompactLedger(Table table) {
    this.table = Objects.requireNonNull(table, "null table");
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
  
  
  private long appendRow(ByteBuffer entryHash, Table table) {
    ByteBuffer ehCopy = entryHash.slice();
    
    final long nextRowNum = table.size() + 1;
    
    byte[] nextRowHash;
    {
      int skipCount = skipCount(nextRowNum);
      
      ByteBuffer nextRowData = ByteBuffer.allocate((1 + skipCount) * HASH_WIDTH);
      
      nextRowData.put(entryHash);
      for (int p = 0; p < skipCount; ++p) {
        long referencedRowNum = nextRowNum - (1L << p);
        ByteBuffer hashPtr = rowHash(referencedRowNum, table);
        assert hashPtr.remaining() == HASH_WIDTH;
        nextRowData.put(hashPtr);
      }
      nextRowData.flip();
      
      MessageDigest digest = newDigest();
      digest.update(nextRowData);
      nextRowHash = digest.digest();
    }
    
    ByteBuffer tableRow =
        ByteBuffer.allocate(Table.ROW_WIDTH).put(ehCopy).put(nextRowHash).flip();
    
    return table.addRows(tableRow, nextRowNum - 1);
    
  }

  @Override
  public long size() {
    return table.size();
  }

  @Override
  public Row getRow(long rowNumber) {
    return new LedgerRow(rowNumber, table.readRow(rowNumber - 1));
  }

  @Override
  public ByteBuffer rowHash(long rowNumber) {
    return rowHash(rowNumber, table);
  }
  
  
  @Override
  public void close() {
    table.close();
  }
  
  
  private ByteBuffer rowHash(long rowNumber, Table table) {
    if (rowNumber == 0)
      return sentinelHash();
    
    ByteBuffer tableRow = table.readRow(rowNumber - 1);
    return tableRow.position(tableRow.position() + hashWidth()).slice();
  }
  
  
  /**
   * Lazy-loading
   */
  final class LedgerRow extends BaseRow {
    
    private final long rowNumber;
    private final ByteBuffer row;
    
    LedgerRow(long rowNumber, ByteBuffer row) {
      this.rowNumber = rowNumber;
      this.row = row.remaining() == row.capacity() ? row : row.slice();

      assert rowNumber > 0;
      assert row.remaining() == Table.ROW_WIDTH;
    }

    @Override
    public long rowNumber() {
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



