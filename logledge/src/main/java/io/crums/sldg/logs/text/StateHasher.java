/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.function.Predicate;

import io.crums.io.Opening;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.NullValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.src.TableSalt;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * A stateless log hasher and parser.
 * 
 * <h2>Model</h2>
 * <p>
 * Instead of returning a simple hash of the log, this returns a
 * {@linkplain State} object representing the current state of the log.
 * This is designed to allow continuously appending a log and incrementally
 * updating the hash representation of its state.
 * </p>
 * 
 * <h2>Stateless; sorta</h2>
 * <p>
 * This base class is purely "functional": invoking its methods produce
 * no side effects (that is, no side effects beyond consuming and advancing
 * the position of the stream argument). However, it does contain a few hooks
 * that a subclass can exploit to record work and so on.
 * </p><p>
 * Also, even tho "stateless", <em>instances are not safe under concurrent access.</em>
 * This is because, for efficiency, each instance sports a couple of state-ful
 * SHA-256 digesters. Use the {@linkplain #StateHasher(StateHasher) copy constructor}
 * for concurrent scenarios.
 * </p>
 */
public class StateHasher {
  
  
  /**
   * Creates and returns a simple, prefix-based, comment-matching predicate.
   * 
   * @param cPrefix comment prefix (e.g. "{@code //}" or "{@code #}")
   * 
   * @return {@code (b) -> false} if {@code cPrefix} is empty or {@code null};
   *         prefix-matching predicate, otherwise.
   */
  public static Predicate<ByteBuffer> commentPrefixMatcher(String cPrefix) {
    
    if (cPrefix == null || cPrefix.isEmpty())
      return (b) -> false;

    byte[] pbytes = Strings.utf8Bytes(cPrefix);
    
    if (pbytes.length == 1) {
      final byte c = pbytes[0];
      return (b) -> b.get(b.position()) == c;
    }
    
    return new Predicate<ByteBuffer>() {
      
      @Override
      public boolean test(ByteBuffer t) {
        final int len = pbytes.length;
        if (t.remaining() < len)
          return false;
        int pos = t.position();
        for (int index = 0; index < len; ++index)
          if (t.get(pos + index) != pbytes[index])
            return false;
        return true;
      }
      
    };
  }


  private final MessageDigest work = SldgConstants.DIGEST.newDigest();
  private final MessageDigest work2 = SldgConstants.DIGEST.newDigest();

  private final TableSalt salter;
  private final Predicate<ByteBuffer> commentMatcher;
  private final String tokenDelimiters;
  
  
  

  /**
   * Full constructor.
   * 
   * @param salt            required salter
   * @param commentMatcher  optional comment matcher (may be {@code null})
   * @param tokenDelimiters optional token delimiter chars (if {@code null}
   *                        then whitespace delimited tokens)
   * 
   * @see #commentPrefixMatcher(String)
   */
  public StateHasher(
      TableSalt salt, Predicate<ByteBuffer> commentMatcher, String tokenDelimiters) {
    
    this.salter = Objects.requireNonNull(salt, "null salter");
    this.commentMatcher = commentMatcher;
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
  
  
  /**
   * Copy construtor. All fields, except for the internal digesters
   * are shared. (This assumes the {@linkplain #commentMatcher()} predicate
   * is stateless.)
   */
  public StateHasher(StateHasher copy) {
    this.salter = copy.salter;
    this.commentMatcher = copy.commentMatcher;
    this.tokenDelimiters = copy.tokenDelimiters;
  }
  
  
  /**
   * No comment, whitespace delimited tokenizer constructor.
   * 
   * @param salt required salter
   */
  public StateHasher(TableSalt salt) {
    this(salt, null, null);
  }
  
  
  
  public TableSalt salter() {
    return salter;
  }
  
  
  public Optional<Predicate<ByteBuffer>> commentMatcher() {
    return Optional.ofNullable(commentMatcher);
  }
  
  
  public Optional<String> tokenDelimiters() {
    return Optional.ofNullable(tokenDelimiters);
  }
  

  
  private ByteBuffer newLineBuffer() {
    return ByteBuffer.allocate(lineBufferSize());
  }
  

  /** Returns the max number of bytes per line. Defaults to 8k. Override to increase. */
  protected int lineBufferSize() {
    return 8192;
  }
  

  /** Returns the tokenizer using the given {@code line}. */
  protected StringTokenizer newTokens(String line) {
    return tokenDelimiters == null ?
        new StringTokenizer(line) :
          new StringTokenizer(line, tokenDelimiters);
  }
  

  
  /**
   * Invocation hook when a line of text forms a ledgered row.
   * This contains sufficient information to construct a
   * {@code FrontieredRow} instance. Noop, by default.
   * 
   * @param preFrontier hash frontier <em>before</em> new row is created
   *                    the row's number is one greater than the pre-frontier's
   * @param cols        the row's column values (not empty)
   * @param inputHash   resultant <em>input hash</em> (from {@linkplain
   *                    SourceRow#rowHash(List, MessageDigest, MessageDigest)
   *                    SourceRow.rowHash(cols, null, null)})
   *                    that goes in the skip ledger
   * @param offset      offset the row (line) begins in the log (inc.)
   * @param endOffset   end offset row ends (exc.)
   * @param lineNo      number of {@code '\n'} chars preceding this row
   * 
   * @see #advanceByLine(ByteBuffer, HashFrontier, long, long)
   * @see FrontieredRow
   */
  protected void observeRow(
      HashFrontier preFrontier, List<ColumnValue> cols, ByteBuffer inputHash,
      long offset, long endOffset, long lineNo) {
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
   * {@linkplain #observeRow(HashFrontier, List, ByteBuffer, long, long, long)} is invoked, and if that method
   * is overridden (a noop, by default), then there may be side effects.
   * </p>
   * 
   * @param text      line of text (usually not empty since trailing {@code '\n'} is included)
   * @param frontier  state of the ledger <em>before</em> this line of text
   * @param offset    the starting offset of {@code text}
   * @param lineNo    informational only (not used in logic)
   * 
   * @return          state of the ledger <em>after</em> this line of text
   */
  protected HashFrontier advanceByLine(
      ByteBuffer text, HashFrontier frontier, long offset, long lineNo) {
    if (commentMatcher != null && commentMatcher.test(text))
      return frontier;
    
    long endOffset = offset + text.remaining();
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
    
    observeRow(frontier, columns, inputHash, offset, endOffset, lineNo);
    
    return frontier.nextFrontier(inputHash);
  }
  
  
  
  /** Plays the given file and returns its state. */
  public State play(File file) throws IOException {
    try (ReadableByteChannel ch = Opening.READ_ONLY.openChannel(file)) {
      return play(ch);
    }
  }
  
  
  /**
   * Plays the given log stream and returns its state.
   */
  public State play(InputStream log) throws IOException {
    return play(Channels.newChannel(log));
  }
  
  /** Plays the given log stream and returns its state. */
  public State play(ReadableByteChannel logChannel) throws IOException {
    return play(logChannel, State.EMPTY);
  }
  

  
  
  
  
  
  public State play(
      ReadableByteChannel logChannel, State state)
          throws IOException {
    
    
    HashFrontier frontier = state.frontier();
    long offset = state.eolOffset();
    long lineNo = state.lineNo();
    
    Objects.requireNonNull(logChannel, "null log channel");
    Objects.requireNonNull(frontier, "null hash frontier");
    
    
    ByteBuffer lineBuffer = newLineBuffer();

    int eol = loadLine(lineBuffer, logChannel);
    
    long lendOff = offset;
    
    while (eol != 0 && !stopPlay()) {
      
      final int loadedLimit = lineBuffer.limit();
      
      lineBuffer.limit(eol);
      
      final long endOff = offset + lineBuffer.remaining();
      
      final HashFrontier oldFrontier = frontier;
      
      frontier = advanceByLine(lineBuffer, oldFrontier, offset, lineNo);
      
      if (frontier != oldFrontier) {
        assert frontier.rowNumber() == oldFrontier.rowNumber() + 1;
        
        observeLedgeredLine(frontier, offset, endOff, lineNo);
        lendOff = endOff;
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
    return new State(frontier, lendOff, lineNo);
  }
  
  /** Stop play short circuit method. Defaults to {@code false}. */
  protected boolean stopPlay() {
    return false;
  }
  
  
  /**
   * Plays the given log and returns the specified row.
   * 
   * @param rn          target row number
   * @param state       state <em>before</em> {@code rn}; {@code state.eolOffset}
   *                    expected to be absolute
   * @param logChannel  seekable byte stream
   * 
   * @return everything to know about the given row
   * 
   * @throws IOException
   *    if an I/O error occurs on reading the log
   * @throws IllegalArgumentException
   *    if {@code rn <= frontier.rowNumber()}
   * @throws NoSuchElementException
   *    if the target row number is never reached
   */
  public FrontieredRow getFrontieredRow(
      long rn, State state, SeekableByteChannel logChannel)
          throws IOException, IllegalArgumentException, NoSuchElementException {

    if (state.eolOffset() == -1)
      throw new IllegalArgumentException("EOL offset not set: " + state);
    
    logChannel.position(state.eolOffset());
    
    return getFrontieredRow(rn, state, (ReadableByteChannel) logChannel);
  }
  
  
  
  /**
   * Plays the given log forward and returns a {@linkplain FrontieredRow}
   * for the given row number. The given log channel is assumed to be positioned at the
   * beginning of content following the {@linkplain HashFrontier#rowNumber()
   * frontier row number}.
   * 
   * @param rn         target row number
   * @param state      state <em>before</em> target row, i.e. {@code state.rowNumber() < rn}
   * @param logChannel positioned at end of state row [{@code state.rowNumber()}]
   * 
   * @return not null
   * @throws IOException
   * @throws IllegalArgumentException if {@code rn <= state.rowNumber()}
   * @throws NoSuchElementException if the target row number is never reached
   */
  public FrontieredRow getFrontieredRow(
      long rn, State state, ReadableByteChannel logChannel)
          throws IOException, IllegalArgumentException, NoSuchElementException {
    
    if (rn <= state.rowNumber())
      throw new IllegalArgumentException(
          "row [%d] <= state [%d]".formatted(rn, state.rowNumber()));
    
    
    class Collector extends StateHasher {
      FrontieredRow row;
      Collector() {
        super(StateHasher.this);
      }
      @Override
      protected boolean stopPlay() {
        return row != null;
      }
      @Override
      protected void observeRow(
          HashFrontier preFrontier, List<ColumnValue> cols, ByteBuffer inputHash,
          long offset, long endOff, long lineNo) {
        if (preFrontier.rowNumber() + 1 == rn) {
          this.row = new FrontieredRow(cols, preFrontier, endOff, lineNo);
        }
      }
      
      boolean failed() {
        return row == null;
      }
    }
    
    Collector c = new Collector();
    var rState = c.play(logChannel, state);
    
    if (c.failed())
      throw new NoSuchElementException(
          "row [%d] never reached; final row [%d]".formatted(rn, rState.rowNumber()));
    
    return c.row;
  }
  
  

  /**
   * Returns the specified list of ordered rows.
   * 
   * @param rns   ascending list of row numbers &ge; 1
   * @param log   seekable log stream
   * 
   * @throws NoSuchElementException
   *    if the {@code log} stream has too few rows
   */
  public List<Row> getRows(List<Long> rns, SeekableByteChannel log)
      throws IOException, NoSuchElementException {
    if (rns.isEmpty())
      return List.of();
    if (rns.get(0) < 1)
      throw new IllegalArgumentException("rns [0]: " + rns.get(0));
    
    Row[] rows = new Row[rns.size()];
    
    State state = State.EMPTY;
    
    for (int index = 0; index < rows.length; ++index) {
      long rn = rns.get(index);
      if (rn <= state.rowNumber())
        throw new IllegalArgumentException(
            "out-of-sequence/illegal row number %d <= last %d (at index %d)"
            .formatted(rn, state.rowNumber(), index));
      
      state = nextStateAhead(state, rn);
      
      var fr = getFrontieredRow(rn, state, log);
      rows[index] = fr.row();
      state = fr.toState();
    }
    
    return Lists.asReadOnlyList(rows);
  }
  
  
  /**
   * Returns the next saved state ahead of row number {@code rn}, if any;
   * {@code state}, otherwise.
   * 
   * @param state fallback state
   * @param rn    target row number (&gt; {@code state.rowNumber()})
   * @return      next state with row number &ge; {@code state.rowNumber()}
   *              <em>and</em> &lt; {@code rn}
   */
  protected State nextStateAhead(State state, long rn) throws IOException {
    return state;
  }
  
  
  
  /**
   * Returns the ledger skip path connecting the specified rows.
   * 
   * @param loRn    &ge; 1
   * @param hiRn    &ge; {@code loRn}
   * @param log     seekable log stream
   * @return        not null
   * 
   * @throws NoSuchElementException
   *    if there are fewer ledgerable rows in the {@code log} stream than {@code hiRn}
   */
  public Path getPath(long loRn, long hiRn, SeekableByteChannel log)
      throws IOException, NoSuchElementException {
    
    var rns = SkipLedger.skipPathNumbers(loRn, hiRn);
    var rows = getRows(rns, log);
    return new Path(rows);
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















