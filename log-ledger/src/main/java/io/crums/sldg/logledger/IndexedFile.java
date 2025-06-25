/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;

import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.io.sef.Alf;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * Constant-time, random access to rows (or lines) in a log file using
 * an offsets index. If certain lines
 * don't count as rows (whether because the grammar disallows blank lines
 * or comment-lines are defined), then the content of some "rows" may be
 * postpended with that "neutral" content.
 * 
 * <h2>API Note</h2>
 * <p>
 * This class's only outside dependency is the {@code io.crums.io.sef.Alf}
 * package; Excepting the pseudo constructor, it has no dependencies on
 * <em>this</em> module. Since storing such offsets is a common use case of
 * {@linkplain Alf}, this class may move to a different package / module in
 * future versions.
 * </p>
 * 
 * @see #rowBytes(long)
 * @see #rowString(long)
 * @see #rowCount()
 * @see #newInstance(File, File)
 * @see SourceIndexer
 */
public class IndexedFile implements Channel {
  
  /**
   * Creates and returns a new instance.
   * 
   * @param logFile             the log file
   * @param offsetsFile         the offsets index file
   */
  public static IndexedFile newInstance(File logFile, File offsetsFile)
      throws UncheckedIOException {
    
    try (var onFail = new TaskStack()) {
      
      var offCh = Files.openVersioned(offsetsFile, Opening.READ_ONLY);
      onFail.pushClose(offCh);
      
      Alf offsets = new Alf(offCh, Files.HEADER_LENGTH);
      
      var log = Opening.READ_ONLY.openChannel(logFile);
      onFail.pushClose(log);
      
      var indexedSource = new IndexedFile(log, offsets);
      
      onFail.clear();
      return indexedSource;
      
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on loading log file %s ; offsets file %s : %s"
          .formatted(logFile, offsetsFile, iox.getMessage()),
          iox);
    }
  }
  
  
  /**
   * As a sanity check, the maximum byte-length per-row can
   * be no greater than 8 MB. (One way this condition can be violated,
   * is if the log file is appended without updating the offsets index.)
   */
  public final static long MAX_ROW_SIZE = 1024 * 1024 * 8;
  
  
  
  
  
  
  
  private final FileChannel log;
  private final Alf offsets;
  private final long maxRowNo;
  private final long logSize;  
  
  /**
   * Package-private constructor because argument types are not "exported"
   * by module.
   * 
   * @param log         the log file
   * @param offsets     starting offsets of rows
   * 
   * @see #newInstance(File, File)
   */
  IndexedFile(FileChannel log, Alf offsets) {
    this.log = log;
    this.offsets = offsets;
    this.maxRowNo = offsets.size();
    try {
      this.logSize = log.size();
    } catch (IOException iox) {  throw new UncheckedIOException(iox); }
    
    if (offsets.isEmpty())
      throw new IllegalArgumentException("offsets index is empty: " + offsets);
    
    
    long lastOffset = rowOffset(maxRowNo);
    
    if (logSize <= lastOffset)
      throw new IllegalArgumentException(
          "minimum log size implied by offsets index is %d bytes;" +
          " actual is %d; index row-count is %d"
          .formatted(lastOffset + 1L, logSize, maxRowNo));
    
    if (logSize - lastOffset > MAX_ROW_SIZE)
      throw new IllegalArgumentException(
          "last row [%d] size %d = %d (log size) - %d (last offset) > %d MAX_ROW_SIZE"
          .formatted(
              maxRowNo,
              logSize - lastOffset,
              logSize,
              lastOffset,
              MAX_ROW_SIZE));
  }
  
  
  /**
   * Returns the no. of indexed rows. Rows are numbered from 1 to this number,
   * inclusive.
   */
  public long rowCount() {
    return maxRowNo;
  }
  
  
  /**
   * Returns the raw byte contents of the row at the specified no. There are
   * no empty rows. With the possible exception of the last row, every row
   * has a new-line character ({@code '\n'}).
   * <p>
   * Note, depending on the grammar, the returned buffer's <em>content may
   * contain "neutral" bytes</em> following the new-line character
   * ({@code '\n'}).
   * </p>
   * 
   * @param rowNo &ge; 1
   * @return non-empty buffer, with backing-array (<em>not read-only</em>)
   * 
   * @throws IndexOutOfBoundsException
   *         if {@code rowNo} is &lt; 1 or &gt; {@linkplain #rowCount()}
   */
  public ByteBuffer rowBytes(long rowNo)
      throws IndexOutOfBoundsException, UncheckedIOException {
    
    final long startOff, endOff;
    if (rowNo <= 0L)
      throw new IndexOutOfBoundsException("rowNo " + rowNo + " <= 0");
    
    if (rowNo < maxRowNo) {
      startOff = rowOffset(rowNo);
      endOff = rowOffset(rowNo + 1);
    } else if (rowNo == maxRowNo) {
      startOff = rowOffset(rowNo);
      endOff = logSize;
    } else
      throw new IndexOutOfBoundsException(
          "rowNo % > max indexed row (%d)".formatted(rowNo, maxRowNo));
    
    long length = endOff - startOff;
    sanityCheckOffsets(rowNo, startOff, length);
    
    try {
      return
          ChannelUtils.readRemaining(
              log, startOff, ByteBuffer.allocate((int)length))
          .flip();
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on rowByte(%d): %s".formatted(rowNo, iox.getMessage()),
          iox);
    }
  }
  
  

  /**
   * Returns the contents of the row at the specified no. as a UTF-8 string.
   * There are no empty rows. With the possible exception of the last row, every row
   * ends with a new-line char ({@code '\n'}.
   * 
   * @param rowNo       &ge; 1 and &le; {@linkplain #rowCount()}
   * @return a not-empty string
   * 
   * @throws IndexOutOfBoundsException
   *         if {@code rowNo} is &lt; 1 or &gt; {@linkplain #rowCount()}
   */
  public String rowString(long rowNo)
      throws IndexOutOfBoundsException, UncheckedIOException {
    
    return Strings.utf8String(rowBytes(rowNo));
  }
  
  
  private long rowOffset(long rowNo) {
    try {
      return offsets.get(rowNo - 1L);
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on reading offset for row [" + rowNo + "]: " + iox.getMessage(),
          iox);
    }
  }
  
  
  private void sanityCheckOffsets(long rowNo, long startOff, long length) {
    if (length > MAX_ROW_SIZE)
      throw new IllegalStateException(
          "Panic: row [%d] is huuuuge: offset %d; length %d"
          .formatted(rowNo, startOff, length));
    if (length < 1L)
      throw new IllegalStateException(
          "Assertion failed: row [%d] length is illegal: offset %d; length %d"
          .formatted(rowNo, startOff, length));
  }
  
  
  

  @Override
  public boolean isOpen() {
    return log.isOpen() && offsets.isOpen();
  }

  @Override
  public void close() {
    if (!isOpen())
      return;
    try (var closer = new TaskStack()) {
      closer.pushClose(log);
      closer.pushClose(offsets);
    }
  }

}
