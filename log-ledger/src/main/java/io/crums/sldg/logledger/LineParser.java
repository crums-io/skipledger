/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * A text log parser designed for event-driven side-effects. The processing
 * model is a bit like an XML SAX parser. This is not very FP, but it's
 * cognitively simple, at least.
 * 
 * <h2>Single Thread Access</h2>
 * <p>
 * Since this consumes a byte stream (set at instantiation), access to the
 * {@linkplain #parse()} method is synchronzied. So too are the parser 
 * configuration methods {@linkplain #lineEndOffset(long)} and
 * {@linkplain #lineNo(long)}. Therefore, if one attempts to "configure"
 * a parser while its {@code parse()} method is running, the "configuring
 * thread" will block unit the {@code parse()} completes. Subclasses too
 * use this pattern.
 * </p>
 */
public class LineParser implements Closeable {
  
  private final static int MAX_LINE_SIZE = 1024 * 1024;
  
  private final ReadableByteChannel log;
  
  
  private long lineEndOffset;
  private long lineNo;
  
  private boolean stop;
  
  

  /**
   * Creates a new instance with intial offset and line-nuumber set to zero. 
   * 
   * @param log         the log stream
   */
  public LineParser(ReadableByteChannel log) {
    this(log, 0L, 0L);
  }
  
  
  /**
   * Creates a new instance with the given starting stastics.
   * 
   * @param log         the log stream
   * @param offset      the initial offset (&ge; 0)
   * @param lineNo      the initial line no. (&ge; 0)
   */
  public LineParser(ReadableByteChannel log, long offset, long lineNo) {
    this.log = log;
    this.lineEndOffset = offset;
    this.lineNo = lineNo;
    
    if (lineNo < 0L)
      throw new IllegalArgumentException("lineNo " + lineNo);
    if (offset < 0L)
      throw new IllegalArgumentException("offset " + offset);
    
    if (!log.isOpen())
      throw new IllegalArgumentException("input log closed: " + log);
  }
  
  
  
  /**
   * Parses the log. This is a potentially long-running operation.
   * 
   * @see #stop()
   */
  public synchronized void parse() throws IOException {
    stop = false;
    ByteBuffer buffer = ByteBuffer.allocate(initBufferSize());
    ByteBuffer line = buffer.asReadOnlyBuffer();
    
    while (!stopSignaled()) {
      int eol = loadLine(buffer);
      if (eol == -1) {
        buffer = expandAndCopy(buffer);
        line = buffer.asReadOnlyBuffer();
        continue;
      }
      if (eol == 0)
        break;
      
      do {
        line.limit(eol).position(buffer.position());
        int len = line.remaining();
        ++lineNo;
        observeLine(lineEndOffset, lineNo, line);
        lineEndOffset += len;
        buffer.position(buffer.position() + len);
      } while (!stopSignaled() && (eol = eol(buffer)) != 0);
      
      buffer.compact();
    }
  }
  
  
  
  public synchronized LineParser lineNo(long lineNo) {
    if (lineNo < 0L)
      throw new IllegalArgumentException("lineNo " + lineNo);
    this.lineNo = lineNo;
    return this;
  }
  
  
  
  public synchronized LineParser lineEndOffset(long offset) {
    if (offset < 0L)
      throw new IllegalArgumentException("offset " + offset);
    this.lineEndOffset = offset;
    return this;
  }
  
  
  
  /**
   * Closes the underlying log stream. Invokes the stop method (but
   * does not wait for the parsing to stop).
   */
  public void close() throws IOException {
    stop();
    log.close();
  }
  
  
  /**
   * Signals the parsing to stop.
   */
  public void stop() {
    stop = true;
  }
  
  
  protected final boolean stopSignaled() {
    return stop;
  }
  
  
  
  /**
   * Returns the last parsed line no.
   * 
   * @return &ge; 0
   */
  public long lineNo() {
    return lineNo;
  }
  
  
  /**
   * Returns the ending (exclusive) offset of the last parsed
   * {@linkplain #lineNo() line no.}
   * 
   * @return &ge; 0
   */
  public long lineEndOffset() {
    return lineEndOffset;
  }
  
  
  /**
   * Returns the initial buffer size. Defaults to 64kB.
   */
  protected int initBufferSize() {
    return 64 * 1024;
  }
  
  
  /**
   * Subclass hook for side-effects. The base implementation is a noop.
   * 
   * @param lineOffset  the line's starting offset
   * @param lineNo      the 1-based line no.
   * @param line        UTF-8 line contents in remaining bytes
   */
  protected void observeLine(long lineOffset, long lineNo, ByteBuffer line) {  }
  
  
  
  ByteBuffer expandAndCopy(ByteBuffer buffer) {
    if (buffer.capacity() >= MAX_LINE_SIZE)
      throw new IllegalStateException(
          "buffer capacity at hard limit: " + buffer);
    
    ByteBuffer expand = ByteBuffer.allocate(buffer.capacity() * 2);
    expand.put(buffer);
    return expand;
  }

  
  /**
   * Returns the first index beyond {@code '\n'} in the given buffer starting
   * from its current position, up to (but excluding) the buffer's limit; if
   * {@code '\n'} does not occur in this range, then {@code 0} is returned.
   */
  private int eol(ByteBuffer buffer) {
    int pos = buffer.position();
    int limit = buffer.limit();
    for (; pos < limit && buffer.get(pos) != '\n'; ++pos);
    return pos == limit  ? 0 : pos + 1;
  }

  /**
   * 
   * 
   * @param buffer      not necessarily positioned at zero (may be compacted)
   * 
   * @return EOL position in {@code buffer} (1 beyond {@code '\n'} or at
   *         end-of-stream), or {@code -1}, if the buffer was filled to
   *         capacity w/o hitting EOL
   */
  private int loadLine(ByteBuffer buffer)
      throws IOException {
    
    int pos = buffer.position();
    
    int bytesRead = 0;
    while (buffer.hasRemaining()) {
      
      bytesRead = log.read(buffer);
      if (bytesRead == -1)
        break;
      
      final int nextPos = pos + bytesRead;
      assert nextPos == buffer.position();
      
      while (pos < nextPos && buffer.get(pos) != '\n')
        ++pos;
      
      if (pos < nextPos) {
        ++pos;
        break;
      }
    }
    
    if (pos == buffer.limit() && buffer.get(pos - 1) != '\n' && bytesRead != -1)
      pos = -1;

    buffer.flip();
    
    return pos;
  }

}























