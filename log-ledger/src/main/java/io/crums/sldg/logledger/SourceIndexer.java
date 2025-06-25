/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import io.crums.io.Opening;
import io.crums.io.sef.Alf;
import io.crums.sldg.logledger.LogParser.Listener;

/**
 * Indexes log-file row offsets in a compact format. It plugs in directly to a
 * {@linkplain LogParser} by implementing its {@linkplain Listener Listener}
 * interface. It's independent of hashing ({@linkplain Hasher}, etc.), but
 * <em>may</em> depend on {@linkplain Grammar}, if certain lines don't count
 * as ledgerable (usually not the case for conventional log files); otherwise,
 * line-boundaries are defined independent of grammar.
 * 
 * @see SourceIndex
 */
public class SourceIndexer implements Listener, AutoCloseable {
  
  /** Since we're single-threaded, we can do this. */
  private final ByteBuffer work = ByteBuffer.allocate(64);
  /** Alf indexes start from 0. */
  private final Alf alf;
  private final boolean verify;
  
  /** Last row no in the {@linkplain #alf}. */
  private long lastRowNo;
  
  
  
  /**
   * Main constructor.
   * 
   * @param indexFile   offsets index file
   * @param mode        opening mode (e.g. {@code CREATE} or {@code READ_ONLY})
   * @param verify      if {@code true}, then existing row offsets are verified when
   *                    encountered
   */
  public SourceIndexer(File indexFile, Opening mode, boolean verify) {
    if (mode == Opening.CREATE && verify)
      throw new IllegalArgumentException(
          "illegal mode (CREATE) with verify=true; indexFile " + indexFile);
    try {
      boolean writeHeader = !indexFile.exists();
      var ch = mode.openChannel(indexFile);
      if (writeHeader)
        ch.position(0L).write(Files.fileHeader());
      else
        Files.readVersion(ch, indexFile);
      
      this.alf = new Alf(ch, Files.HEADER_LENGTH);
      this.verify = verify;
      this.lastRowNo = alf.size();
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on attempt to create OffsetIndex (mode=%s); %s -- detaiL: %s"
          .formatted(mode, indexFile, iox.getMessage()), iox);
    }
  }
  
  

  /**
   * Package-private constructor because argument types are not "exported"
   * by module.
   * 
   * @param alf         offsets index
   * @param verify      if {@code true}, then existing offsets are verified when
   *                    encountered
   */
  SourceIndexer(Alf alf, boolean verify) {
    this.alf = alf;
    if (!alf.isOpen())
      throw new IllegalArgumentException(
          "alf is closed: " + alf);
    this.verify = verify;
    lastRowNo = alf.size();
  }
  
  private long getOffset(long rowNo) {
    try {
      return alf.get(rowNo - 1L, work);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on offset lookup for row [%d]: %s"
          .formatted(rowNo, iox), iox);
    }
  }
  
  private void putOffset(long rowNo, long offset) {
    assert alf.size() + 1 == rowNo;
    try {
      alf.addNext(offset, work);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on writing offset (%d) for row [%d]: %s"
          .formatted(offset, rowNo, iox), iox);
    }
  }
  
  
  protected final void observeOffset(long rowNo, long offset) {
    assert lastRowNo == alf.size();
    if (rowNo < lastRowNo) {
      if (verify) {
        long recordedEol = getOffset(rowNo);
        if (offset != recordedEol) {
          
          throw new AlfException(
              "[%d]: offset in alf (%d) != computed offset (%d)"
              .formatted(rowNo, recordedEol, offset));
        }
      }      
      return;
    }
    
    if (rowNo != lastRowNo + 1L)
      throw new IllegalStateException(
          "rowNo %d ahead of alf (%d)".formatted(rowNo, lastRowNo));
    
    putOffset(rowNo, offset);
    ++lastRowNo;
  }

  @Override
  public void lineOffsets(long offset, long lineNo) {   }

  @Override
  public void ledgeredLine(long rowNo, Grammar grammar, long offset, long lineNo, ByteBuffer line) {
    observeOffset(rowNo, offset);
  }

  @Override
  public void skippedLine(long offset, long lineNo, ByteBuffer line) {  }
  
  
  /** Commits the changes. */
  @Override
  public void parseEnded() {
    commit();
  }
  

  /** Commits the changes. */
  public void commit() {
    try {
      alf.commit();
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }

  @Override
  public void close() {
    try {
      alf.close(false);
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  

}
