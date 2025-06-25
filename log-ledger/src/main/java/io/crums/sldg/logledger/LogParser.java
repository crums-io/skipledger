/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import io.crums.util.Lists;

/**
 * Parses a log, distinguishing ledgerable lines from the ones
 * that aren't. Altho this uses a {@linkplain Grammar} object
 * to distinguish such lines, the ledgerable lines are not
 * directly manipulated by this class; the actual function of an
 * instance is determined by its {@linkplain Listener listener}s.
 * 
 * <h2>Configuration</h2>
 * 
 * <p>
 * Instances should not be configured while {@linkplain #parse() parsing}.
 * Note, this class knows nothing about the position of the log-stream:
 * if not positioned at the beginning, then {@linkplain #lineEndOffset(long)}
 * must be set <em>before</em> {@linkplain #parse()} is invoked.
 * </p>
 * 
 * <h2>Note</h2>
 * <p>
 * The rationale for breaking up the implementation into this
 * and its base class {@linkplain LineParser} is to reduce cognitive
 * load.
 * </p>
 * 
 * @see LogParser.Listener
 * @see #pushListener(Listener)
 */
public class LogParser extends LineParser {
  
  private final static Predicate<ByteBuffer> NO_COMMENT =
      new Predicate<ByteBuffer>() {
        @Override
        public boolean test(ByteBuffer t) {   return false;   }
      };
  
      
  
  /**
   * Callbacks invoked when lines are crossed.
   */
  public interface Listener {
    
    /**
     * Invoked for every line (ledgered or not).
     * 
     * @param offset    the byte offset the line begins
     * @param lineNo    the 1-based line number
     */
    void lineOffsets(long offset, long lineNo);
    
    /**
     * Invoked for ledgered lines only.
     * 
     * @param rowNo     this line's (ledger) row number
     * @param grammar   line tokenizer
     * @param offset    line's beginning offset
     * @param lineNo    line no.
     * @param line      line contents (assumed UTF-8)
     */
    void ledgeredLine(
        long rowNo, Grammar grammar, long offset, long lineNo, ByteBuffer line);
    
    /**
     * Invoked when a line is not ledgered (skipped).
     * 
     * @param offset    line's beginning offset
     * @param lineNo    line no.
     * @param line      line contents
     */
    void skippedLine(long offset, long lineNo, ByteBuffer line);
    
    
    /**
     * Invoked on the conclusion of a parse run. There will be no more
     * calls to this instance during this run.
     */
    default void parseEnded() {  }
    
  }
  

  private Listener[] listeners = { };
  
  protected final Grammar grammar;
  
  private final Predicate<ByteBuffer> commentMatcher;
  
  
  
  
  // (inclusive)
  private long maxRowNo = Long.MAX_VALUE;
  
  private long rowNo;

  /**
   * Creates a new instance with the given grammar.
   * 
   * @param grammar     the grammar used to parse lines
   * @param log         the log stream
   */
  public LogParser(Grammar grammar, ReadableByteChannel log) {
    super(log);
    this.grammar = grammar;
    this.commentMatcher = grammar.commentMatcher().orElse(NO_COMMENT);
  }


  
  /**
   * Returns the last row number parsed. If not {@linkplain #parse() parse}d yet,
   * then, then the next ledgerable line's row no. will be 1 greater
   * than the returned value.
   * 
   * @see #rowNo(long)
   */
  public long rowNo() {
    return rowNo;
  }
  
  
  /**
   * Sets the last row no parsed; if not yet parsed, then the first
   * ledgerable line encountered during parsing will assume a row no. one
   * greater than the given {@code no}.
   * 
   * @param no  &ge; 0 and &le; {@linkplain #maxRowNo()}
   * 
   * @return    {@code this} instance
   * @see #rowNo()
   */
  public synchronized LogParser rowNo(long no) {
    
    if (no < 0L)
      throw new IllegalArgumentException("negative no " + no);
    if (no > maxRowNo)
      throw new IllegalArgumentException(
          "rowNo (%d) > maxRowNo (%d)".formatted(no, maxRowNo));
   
    this.rowNo = no;
    return this;
  }
  
  
  /**
   * Sets the maximum row no. parsed. This is set to stop parsing
   * before necessarily reaching the end of the stream.
   * <p>
   * This method blocks if the instance is concurrently {@linkplain #parse() parsing}.
   * </p>
   * 
   * @param max &ge; {@linkplain #rowNo()}
   * 
   * @return this instance
   * @see #rowNo(long)
   */
  public synchronized LogParser maxRowNo(long max) {
    if (max < 0L) 
      throw new IllegalArgumentException("max " + max);
    if (max < rowNo)
      throw new IllegalArgumentException(
          "max (%d) < rowNo (%d)".formatted(max, rowNo));
    maxRowNo = max;
    return this;
  }
  
  
  /**
   * Returns the maximum row no. that may be parsed (inclusive).
   */
  public long maxRowNo() {
    return maxRowNo;
  }
  
  
  /**
   * Pushes the given listener onto the "listener stack": listeners
   * are invoked in reverse order of how they were "pushed".
   * <p>
   * This method blocks if the instance is concurrently {@linkplain #parse() parsing}.
   * </p>
   * 
   * @param listener    not {@code null} and not already pushed
   * 
   * @return this instance
   * 
   * @see #listeners()
   */
  public synchronized LogParser pushListener(Listener listener) {
    Objects.requireNonNull(listener);
    if (listeners().contains(listener))
      throw new IllegalArgumentException(
          "listener already added: " + listener);
    Listener[] current = listeners;
    Listener[] update = new Listener[current.length + 1];
    update[current.length] = listener;
    for (int index = current.length; index-- > 0; )
      update[index] = current[index];
    listeners = update;
    return this;
  }
  
  
  
  /**
   * Returns the listener stack. The returned list is in reverse of the order
   * that they were "pushed".
   * 
   * @see #pushListener(Listener)
   */
  public List<Listener> listeners() {
    return Lists.asReadOnlyList(listeners).reversed();
  }
  
  
  /**
   * {@inheritDoc}
   * <p>
   * Note all mutator (set up) methods are also synchronized.
   * </p>
   */
  @Override
  public synchronized void parse() throws IOException {
    
    if (listeners.length == 0)
      System.getLogger(LglConstants.LOG_NAME).log(
          Level.WARNING, "parsing with no listeners");
    
    if (rowNo >= maxRowNo)
      return;
    super.parse();
    for (int index = listeners.length; index-- > 0; )
      listeners[index].parseEnded();
  }


  
  @Override
  protected void observeLine(long lineOffset, long lineNo, ByteBuffer line) {
    
    final int pos = line.position();
    final int limit = line.limit();
    assert line.get(limit - 1) == '\n';
    
    
    final boolean ledgered;
    if (grammar.skipBlankLines() && isBlank(line))
      ledgered = false;
    else {
      ledgered = !commentMatcher.test(line);
      line.limit(limit).position(pos);  // in case the matcher touched
    }
    
    if (ledgered) {
      
      ++rowNo;
      if (rowNo > maxRowNo)
        stop();
      
      for (int index = listeners.length; index-- > 0; ) {
        var l = listeners[index];
        l.lineOffsets(lineOffset, lineNo);
        l.ledgeredLine(rowNo, grammar, lineOffset, lineNo, line);
        line.limit(limit).position(pos);
      }
      
      if (rowNo >= maxRowNo)
        stop();
      
    } else {
      
      for (int index = listeners.length; index-- > 0; ) {
        var l = listeners[index];
        l.lineOffsets(lineOffset, lineNo);
        l.skippedLine(lineOffset, lineNo, line);
        line.limit(limit).position(pos);
      }
      
    }
    
  }
  
  
  private boolean isBlank(ByteBuffer line) {
    int s = line.limit();
    final int p = line.position();
    while (s-- > p) {
      switch (line.get(s)) {
      case ' ':
      case '\f':
      case '\t':
      case '\r':
      case '\n':
        break;
      default:
        return false;
      }
    }
    return true;
  }
  

}












