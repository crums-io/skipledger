/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Simple line-by-line parser.
 */
public class LogParser implements Runnable {
  
  /**
   * Listener interface for line parse events. The EOL character {@code '\n'}
   * is <em>included</em> in the fired events.
   */
  public interface SourceListener {
    
    /**
     * Consumes a line of text, typically to generate an appended
     * ledger row. The return value indicates whether it a ledgered row
     * is formed.
     * <p>
     * Note the data in the given {@code line} buffer is <em>volatile</em>
     * (it's the same buffer instance across invocations), so you must
     * write it somewhere (in memory or externally) if you later need it.
     * </p>
     * 
     * @param line    with the posssible exception of the last line,
     *                includes the EOL token {@code \n})
     * @param offset  beginning of line offset
     * 
     * @return {@code true} if a ledger row was formed
     */
    boolean acceptRow(ByteBuffer line, long offset);
  }
  
  
  private long rn;
  private long offset;
  
  private final SourceListener listener;
  private final InputStream source;
  
  
  /**
   * Constructs an instance with zero initial offset.
   * 
   * @param listener    line listener
   * @param source      log stream
   */
  public LogParser(SourceListener listener, InputStream source) {
    this(listener, source, 0L, 0L);
  }
  
  /**
   * Constructs an instance with the given initial offset.
   * This is used when a previously logged file was appended to and the logger
   * is picking off somewhere near where it last ended.
   * 
   * @param listener    line listener
   * @param source      log stream
   * @param initOffset  the initial offset at which the {@code source} stream begins
   */
  public LogParser(SourceListener listener, InputStream source, long initOffset) {
    this(listener, source, initOffset, 0L);
  }

  
  /**
   * Full Constructor.
   * 
   * @param listener    line listener
   * @param source      log stream
   * @param initOffset  initial offset (&ge; 0)
   * @param initRn      initial row number (&ge; 0). (Default is zero.)
   */
  public LogParser(SourceListener listener, InputStream source, long initOffset, long initRn) {
    this.rn = initRn;
    this.offset = initOffset;
    this.listener = Objects.requireNonNull(listener, "null listener");
    this.source = Objects.requireNonNull(source, "null source");
    
    if (initRn < 0)
      throw new IllegalArgumentException("initRn: " + initRn);
    if (initOffset < 0)
      throw new IllegalArgumentException("initOffset: " + initOffset);
  }
  
  
  /**
   * Returns the row number of the last row seen.
   * <p>
   * Note the instance may have been constructed with non-zero initial row number.
   * In any event, this is just informational: the listener should have little use for it.
   * </p>
   */
  public long rowNumber() {
    return rn;
  }
  
  
  /**
   * Returns one beyond the last offset read.
   * <p>
   * Note the instance may have been constructed with non-zero initial offset.
   * </p>
   */
  public long offset() {
    return offset;
  }
  
  
  /** Consumes the log stream. <em>Note the stream is not closed.</em> */
  @Override
  public void run() {
    try {
      runImpl();
    } catch (IOException iox) {
      throw new UncheckedIOException(
          "on running " + this + ": " + iox.getMessage(), iox);
    }
    
  }
  
  private void runImpl() throws IOException {
    ByteBuffer rowBuffer = ByteBuffer.wrap(new byte[maxRowBytes()]);
    while (true) {
      
      int b = source.read();
      if (b == -1)
        break;
      ++offset;
      rowBuffer.put((byte) b);
      
      if (b == '\n') {
        fireRemaining(rowBuffer);
      }
    }
    
    fireRemaining(rowBuffer);
  }
  
  
  protected void fireRemaining(ByteBuffer rowBuffer) {
    int len = rowBuffer.flip().remaining();
    
    if (len != 0 && listener.acceptRow(rowBuffer, offset - len)) 
      ++rn;
    
    rowBuffer.clear();
  }
  
  
  /**
   * The maximum row byte size is 4 kB. Override to increase.
   */
  protected int maxRowBytes() {
    return 4 * 1024;
  }
  
  
  
  public String toString() {
    return getClass().getSimpleName() +
        "[rn=" + rn + ",off=" + offset + ",listener=" + listener + ",src=" + source + "]";
  }

}
