/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringTokenizer;
import java.util.function.Predicate;

import io.crums.sldg.SldgConstants;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.NullValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.src.TableSalt;
import io.crums.util.Strings;

/**
 * A state-less log hasher.
 * 
 * <h2>Model</h2>
 * <p>
 * Instead of returning a simple hash of the log, this returns a
 * {@linkplain HashFrontier} object representing the current state of the log.
 * This is designed to allow continuously appending a log and incrementally
 * updating the hash representation of its state.
 * </p>
 * 
 * <h2>Stateless</h2>
 * <p>
 * This base class is purely "functional": invoking its methods produces
 * no side effects (that is, no side effects beyond consuming and advancing
 * the position of the stream argument). However, it does contain a few hooks
 * that a subclass can exploit to record work and so on.
 * </p>
 */
public class StateHasher {


  private final MessageDigest work = SldgConstants.DIGEST.newDigest();
  private final MessageDigest work2 = SldgConstants.DIGEST.newDigest();

  private final TableSalt salter;
  private final Predicate<ByteBuffer> commentFilter;
  private final String tokenDelimiters;

  /**
   * 
   */
  public StateHasher(
      TableSalt salt, Predicate<ByteBuffer> commentFilter, String tokenDelimiters) {
    
    this.salter = Objects.requireNonNull(salt, "null salter");
    this.commentFilter = commentFilter;
    if (tokenDelimiters != null && tokenDelimiters.isEmpty())
      tokenDelimiters = null;
    this.tokenDelimiters = tokenDelimiters;
    
    try {
      newTokens("test line: xyz abbacas \n");
    } catch (Exception x) {
      throw new IllegalArgumentException(
          "bad token delimiters: " + tokenDelimiters, x);
    }
  }
  
  

  
  /** Returns an 8k buffer. This is the max number of bytes per line. Override to increase. */
  protected ByteBuffer newLineBuffer() {
    return ByteBuffer.allocate(8192);
  }
  

  /** Returns the tokenizer using the given {@code line}. */
  protected StringTokenizer newTokens(String line) {
    return tokenDelimiters == null ?
        new StringTokenizer(line) :
          new StringTokenizer(line, tokenDelimiters);
  }
  

  
  /**
   * Invocation hook when a line of text forms a ledgered row. Noop, by default
   * (and not used yet).
   * 
   * @param inputHash   the <em>input hash</em> that goes in the skip ledger.
   * @param rn          &ge; 1
   * 
   * @see #advanceByLine(ByteBuffer, HashFrontier)
   */
  protected void observeInputHash(ByteBuffer inputHash, long rn) {
    // noop
  }
  
  
  
  
  /**
   * Determines whether empty text lines count. If an empty line counts, then its row
   * is recorded as a single null column.
   * 
   * @return {@code false} By default, empty lines don't count
   */
  protected boolean allowEmptyLines() {
    return false;
  }
  
  
  /**
   * Invoked after a row (line) is ledgered. A subclass can optionally
   * record this state. The base implementation is a noop.
   * 
   * @param frontier  state of the ledger up to (and including) the row
   *                  (The row number can be inferred from this argument.)
   * @param offset    the starting offset of the row in the stream (inclusive)
   * @param eolOff    the ending offset of the row in the stream (exclusive)
   * @param lineNo    line number (informational only, not used)
   */
  protected void observeLedgeredLine(
      HashFrontier frontier, long offset, long eolOff, long lineNo) throws IOException {
  
  }
  
  

  /**
   * <p>
   * Observes the given line of {@code text} and if it forms a ledger row
   * computes and returns the next {@linkplain HashFrontier hash frontier}
   * following the given {@code frontier}; if the text doesn't form a ledger
   * row, the given frontier is returned.
   * </p><p>
   * 2 factors contribute to whether the given text counts as a ledger row:
   * <p>
   * <ol>
   * <li>Not a comment (as determined by the optional filter set at construction)</li>
   * <li>Whether empty lines count (by default, they don't)</li>
   * </ol>
   * <h4>Possible side effects</h4>
   * <p>
   * The base implementation hash no side effects: it's a pure computation.
   * However, if the line of text does form a ledgered row, then
   * {@linkplain #observeInputHash(ByteBuffer, long)} is invoked, and if that method
   * is overridden (a noop, by default), then there may be side effects.
   * </p>
   * 
   * @param text      line of text (usually not empty since trailing {@code '\n'} is included)
   * @param frontier  state of the ledger <em>before</em> this line of text
   * @return          state of the ledger <em>after</em> this line of text
   */
  protected HashFrontier advanceByLine(ByteBuffer text, HashFrontier frontier) {
    if (commentFilter != null && commentFilter.test(text))
      return frontier;
    
    var line = Strings.utf8String(text);
    
    if (line.isBlank() && !allowEmptyLines())
      return frontier;
    
    var tokenizer = newTokens(line);

    final long rn = frontier.rowNumber() + 1;
    
    List<ColumnValue> columns;
    
    if (!tokenizer.hasMoreTokens()) {
      
      if (allowEmptyLines())
        columns = List.of(new NullValue(salter.salt(rn, 1)));
      else
        return frontier;
    
    } else {
    
      columns = new ArrayList<ColumnValue>(32);
      int colNo = 0;
      
      do {
        columns.add(
            new StringValue(
                tokenizer.nextToken(),
                salter.salt(rn, ++colNo)));
      } while (tokenizer.hasMoreTokens());
    }
    
    var inputHash = SourceRow.rowHash(columns, work, work2);
    
    observeInputHash(inputHash, rn);
    
    return frontier.nextFrontier(inputHash);
  }
  
  
  
  /**
   * Plays the given log stream and returns its state.
   * 
   * @param log
   * @return
   * @throws IOException
   */
  public HashFrontier play(InputStream log) throws IOException {
    return play(Channels.newChannel(log));
  }
  
  
  public HashFrontier play(ReadableByteChannel logChannel) throws IOException {
    return play(logChannel, HashFrontier.SENTINEL, 0, 0);
  }
  
  
  public HashFrontier play(
      ReadableByteChannel logChannel, HashFrontier frontier, long offset, long lineNo)
          throws IOException {
    
    Objects.requireNonNull(logChannel, "null log channel");
    Objects.requireNonNull(frontier, "null hash frontier");
    
    ByteBuffer lineBuffer = newLineBuffer();

    int eol = loadLine(lineBuffer, logChannel);
    
    while (eol != 0) {
      
      final int loadedLimit = lineBuffer.limit();
      
      lineBuffer.limit(eol);
      
      final long endOff = offset + lineBuffer.remaining();
      
      final HashFrontier oldFrontier = frontier;
      
      frontier = advanceByLine(lineBuffer, oldFrontier);
      
      if (frontier != oldFrontier) {
        assert frontier.rowNumber() == oldFrontier.rowNumber() + 1;
        
        observeLedgeredLine(frontier, offset, endOff, lineNo);
      }
      
      offset = endOff;
      ++lineNo;
      
      if (loadedLimit > eol) {
        lineBuffer.limit(loadedLimit).position(eol);
        eol = eol(lineBuffer);
        if (eol == -1) {
          lineBuffer.compact();
          eol = loadLine(lineBuffer, logChannel);
        }
      } else {
        assert loadedLimit == eol;
        
        lineBuffer.clear();
        eol = loadLine(lineBuffer, logChannel);
      }
    }
    return frontier;
  }

  
  private int eol(ByteBuffer buffer) {
    int pos = buffer.position();
    int limit = buffer.limit();
    for (; pos < limit && buffer.get(pos) != '\n'; ++pos);
    return pos == limit  ? -1 : pos + 1;
  }
  
  
  /**
   * 
   * @param buffer      not necessarily positioned at zero (may be compacted)
   * @return EOL offset (1 beyond {@code '\n'} or at end-of-stream)
   */
  private int loadLine(ByteBuffer buffer, ReadableByteChannel logChannel) throws IOException {
    int pos = buffer.position();
    
    int bytesRead = 0;
    while (buffer.hasRemaining()) {
      
      bytesRead = logChannel.read(buffer);
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
      throw new BufferUnderflowException();

    buffer.flip();
    
    return pos;
  }

}















