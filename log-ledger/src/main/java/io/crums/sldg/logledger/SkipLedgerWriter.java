/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import static io.crums.sldg.SldgConstants.HASH_WIDTH;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.logledger.Hasher.RowHashListener;

/**
 * Writes the skipledger chain: this is a {@linkplain Hasher} "plug-in".
 */
public class SkipLedgerWriter implements RowHashListener {
  
  public final int CHAIN_BLOCK_SIZE = 2 * HASH_WIDTH;
  
  private final FileChannel chain;
  private final boolean verify;
  
  private long rowCount;
  

  
  public SkipLedgerWriter(FileChannel chain) {
    this(chain, true);
  }
  

  /**
   * Full constructor.
   * 
   * @param chain       open, read-write channel to chain file
   * @param verify      if {@code true}, existing chain blocks are verified
   */
  public SkipLedgerWriter(FileChannel chain, boolean verify) {
    this.chain = chain;
    this.verify = verify;
    if (!chain.isOpen())
      throw new IllegalArgumentException("chain is closed: " + chain);
    try {
      rowCount = checkedRowCount();
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  
  public long rowCount() {
    return rowCount;
  }
  
  
  private long checkedRowCount() throws IOException {
    long size = chain.size();
    long count = size / CHAIN_BLOCK_SIZE;
    if (count * CHAIN_BLOCK_SIZE != size) {
      System.getLogger(LglConstants.LOG_NAME).log(
          Level.WARNING,
          "ignoring partially written last block (%d): %d / 64 bytes written"
          .formatted(count + 1L, size % CHAIN_BLOCK_SIZE));
    }
    return count;
  }
  
  
  
  
  public ByteBuffer rowInputHash(long rowNo) {
    return read(rowOffset(rowNo), HASH_WIDTH, "rowInputHash", rowNo);
  }
  
  
  
  private ByteBuffer read(long offset, int length, String method, long rowNo) {
    try {
      return
          ChannelUtils.readRemaining(
              chain,
              offset,
              ByteBuffer.allocate(length))
          .flip().asReadOnlyBuffer();
    
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "%s(%d) failed: %s".formatted(method, rowNo, iox),
          iox);
    }
  }
  
    
  
  public ByteBuffer rowHash(long rowNo) {
    return read(rowOffset(rowNo) + HASH_WIDTH, HASH_WIDTH, "rowHash", rowNo);
  }
  
  
  public ByteBuffer rowBlock(long rowNo) {
    return read(rowOffset(rowNo), CHAIN_BLOCK_SIZE, "rowBlock", rowNo);
  }
  
  
  
  
  
  
  
  private long rowOffset(long rowNo) {
    // code is i/o bound; not worth bit shift optimization
    return (rowNo - 1L) * CHAIN_BLOCK_SIZE;
  }
  

  @Override
  public void rowHashParsed(
      ByteBuffer inputHash, HashFrontier frontier, HashFrontier prevFrontier) {
    
    final long rowNo = frontier.rowNo();
    final long lastRow = rowCount();
    // the most common code path, if parse is set up right
    if (rowNo == lastRow + 1L) {
      long offset = rowOffset(rowNo);
      try {
        ChannelUtils.writeRemaining(chain, offset, inputHash);
        ChannelUtils.writeRemaining(
            chain, offset + HASH_WIDTH, frontier.frontierHash());
        ++rowCount;
      } catch (IOException iox) {
        throw new UncheckedIOException(iox);
      }
    
    // throw a wrench if we skip a row during parsing (a bug)
    } else if (rowNo > lastRow) {
      throw new IllegalStateException(
          "row [%d] skipped; given row no. %d".formatted(lastRow + 1L, rowNo));
    
    // o.w. verify the values calculated from the parse are the same in the chain
    } else if (verify) {
      var rowBlock = rowBlock(rowNo);
      if (!rowBlock.limit(HASH_WIDTH).equals(inputHash))
        throw new HashConflictException(
            "input hash for row [%d] conflicts with that in chain"
            .formatted(rowNo));
      if (!rowBlock.clear().position(HASH_WIDTH).equals(frontier.frontierHash()))
        throw new HashConflictException(
            "frontier (commitment) hash for row [%d] conflicts with that in chain"
            .formatted(rowNo));
    }
  }

}
