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
import java.util.Collection;
import java.util.Iterator;
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
import io.crums.util.CollectionUtils;
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
 * This base class is purely "functional": its methods produce
 * no side effects (that is, no side effects beyond consuming and advancing
 * the position of the stream argument). However, it does contain a few hooks
 * that a subclass can exploit to record work and so on.
 * </p><p>
 * Also, even tho "stateless", <em>concurrent access is synchronized</em>.
 * This is because, for efficiency, each instance sports a couple of stateful
 * SHA-256 digesters. Use the {@linkplain #StateHasher(StateHasher) copy constructor}
 * for concurrent scenarios.
 * </p>
 * 
 * @see ContextedHasher
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
  
  
  /**
   * The salter. Each word counts as a column (cell) value.
   * Each word's hash is salted using this salter. 
   * @return
   */
  public TableSalt salter() {
    return salter;
  }
  
  
  public Optional<Predicate<ByteBuffer>> commentMatcher() {
    return Optional.ofNullable(commentMatcher);
  }
  
  
  public Optional<String> tokenDelimiters() {
    return Optional.ofNullable(tokenDelimiters);
  }
  
  

  /**
   * Returns the max number of bytes per line. Defaults to 8k. Override to increase.
   * Invoked once at the beginning of {@linkplain #play(ReadableByteChannel, State) play}.
   */
  protected int lineBufferSize() {
    return 8192;
  }
  

  /** Returns the tokenizer using the given {@code line}. */
  protected Iterator<?> newTokens(String line) {
    var tokenizer = tokenDelimiters == null ?
        new StringTokenizer(line) :
          new StringTokenizer(line, tokenDelimiters);
    return tokenizer.asIterator();
  }
  

  
  /**
   * Invocation hook when a line of text forms a ledgered row.
   * This contains sufficient information to construct a
   * {@code FrontieredRow} instance. Noop, by default.
   * 
   * @param preFrontier hash frontier <em>before</em> new row is created
   *                    the row's number is one greater than the pre-frontier's
   * @param cols        the row's column values (not empty)
   * @param offset      offset the row (line) begins in the log (inc.)
   * @param endOffset   end offset row ends (exc.)
   * @param lineNo      number of {@code '\n'} chars preceding this row
   * 
   * @see #advanceByLine(ByteBuffer, HashFrontier, long, long)
   * @see FullRow
   */
  protected void observeRow(
      HashFrontier preFrontier, List<ColumnValue> cols,
      long offset, long endOffset, long lineNo) throws IOException {
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
   * @param fro       state of the ledger up to (and including) the row
   * @param offset    the starting offset of the row in the stream (inclusive)
   */
  protected void observeLedgeredLine(Fro fro, long offset) throws IOException {
//      HashFrontier frontier, long offset, long eolOff, long lineNo) throws IOException {
  
  }
  
  

  /**
   * <p>
   * Observes the given line of {@code text} and if it forms a ledger row
   * computes the source row's hash (the input hash in the skip ledger)
   * and returns it.
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
   * {@linkplain #observeRow(HashFrontier, List, long, long, long)} is invoked, and if that method
   * is overridden (a noop, by default), then there may be side effects.
   * </p>
   * 
   * @param text      line of text (usually not empty since trailing {@code '\n'} is included)
   * @param frontier  hash state of the ledger <em>before</em> this line of text
   * @param offset    the starting offset of {@code text}
   * @param lineNo    informational only (not used in logic)
   * 
   * @return          the input hash if this line of text forms a row; {@code null} o.w.
   */
  protected final ByteBuffer advanceByLine(
      ByteBuffer text, HashFrontier frontier, long offset, long lineNo) throws IOException {
    if (commentMatcher != null && commentMatcher.test(text))
      return null;
    
    long endOffset = offset + text.remaining();
    var line = Strings.utf8String(text);
    
    if (line.isBlank() && !allowEmptyLines())
      return null;
    
    var tokenizer = newTokens(line);

    final long rn = frontier.rowNumber() + 1;
    
    List<ColumnValue> columns;
    
    if (!tokenizer.hasNext()) {
      
      if (allowEmptyLines())
        columns = List.of(new NullValue(salter.salt(rn, 1)));
      else
        return null;
    
    } else {
    
      columns = new ArrayList<ColumnValue>(32);
      int colNo = 0;
      
      do {
        columns.add(
            new StringValue(
                tokenizer.next().toString(),
                salter.salt(rn, ++colNo)));
      } while (tokenizer.hasNext());
    }
    
    var inputHash = SourceRow.rowHash(columns, work, work2);
    
    observeRow(frontier, columns, offset, endOffset, lineNo);
    return inputHash;
  }
  
  
  
  /** Plays the given file and returns its state. */
  public State play(File file) throws IOException {
    try (ReadableByteChannel ch = Opening.READ_ONLY.openChannel(file)) {
      return play(ch);
    }
  }
  
  
  /**
   * Plays the given {@code file}, starting from the {@linkplain State#eolOffset() EOL offset}
   * in the specified {@code state}, and returns the resultant state.
   */
  public State play(File file, State state) throws IOException {
    try (var fc = Opening.READ_ONLY.openChannel(file)) {
      return play(fc, state);
    }
  }
  
  
  /**
   * Plays the given log stream and returns its state.
   */
  public State play(InputStream log) throws IOException {
    return play(Channels.newChannel(log));
  }
  
  /** Plays the given log stream and returns its state. */
  public final State play(ReadableByteChannel logChannel) throws IOException {
    return play(logChannel, State.EMPTY);
  }
  

  

  /** Plays the given seekable stream from the beginning are returns its state. */
  public final State play(SeekableByteChannel logChannel) throws IOException {
    return play((ReadableByteChannel) logChannel.position(0));
  }

  
  /** Plays the given seekable stream, starting from the given state. */
  public final State play(SeekableByteChannel logChannel, State state)
          throws IOException {
    logChannel.position(state.eolOffset());
    return play((ReadableByteChannel) logChannel, state);
  }
  
  
  /**
   * Plays the log assuming it's starting from the given state.
   * Invokes the callback methods listed below.
   * 
   * @param logChannel  log stream (assumed to be positioned at
   *                    {@code state.eolOffset()}
   * @param state       the starting state
   * @return            the resultant state once the end of the
   *                    {@code logChannel} is reached
   *                    
   * @see #lineBufferSize()
   * @see #allowEmptyLines()
   * @see #observeLedgeredLine(Fro, long)
   * @see #observeEndState(Fro)
   * @see #nextStateAhead(State, long)
   */
  public final synchronized State play(
      ReadableByteChannel logChannel, State state)
          throws IOException {
    
    long offset = state.eolOffset();
    long lineNo = state.lineNo();
    
    
    
    ByteBuffer lineBuffer = ByteBuffer.allocate(lineBufferSize());

    int eol = loadLine(lineBuffer, logChannel);
    
    Fro fro = null;
    
    while (eol != 0 && !stopPlay()) {
      
      ++lineNo;
      
      final int loadedLimit = lineBuffer.limit();
      
      lineBuffer.limit(eol);
      
      final long endOff = offset + lineBuffer.remaining();
      
      ByteBuffer inputHash = advanceByLine(lineBuffer, state.frontier(), offset, lineNo);
      
      if (inputHash != null) {
        
        fro = new Fro(state, inputHash, endOff, lineNo);
        state = fro.state();
        observeLedgeredLine(fro, offset);
      }
      
      offset = endOff;
      
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
    if (fro != null)
      observeEndState(fro);
    return state;
  }
  
  
  /**
   * End state callback invoked by {@linkplain #play(ReadableByteChannel, State)}.
   * Defaults to noop.
   */
  protected void observeEndState(Fro fro) throws IOException {  }
  
  /** Stop play short circuit method. Defaults to {@code false}. */
  protected boolean stopPlay() {
    return false;
  }
  
  
  

  /**
   * Returns a list of ascending full rows.
   * 
   * @param rns         ascending list of row numbers
   * @param log         text-based log file
   * 
   * @return not null
   * 
   * @throws IllegalArgumentException if {@code rns} is not ascending
   * @throws NoSuchElementException   if there are too few ledgerable rows in the {@code logChannel}
   */
  public List<FullRow> getFullRows(List<Long> rns, File log)
      throws IOException, IllegalArgumentException, NoSuchElementException {
    
    if (rns.isEmpty())
      return List.of();
    
    try (var logChannel = Opening.READ_ONLY.openChannel(log)) {
      return getFullRows(rns, logChannel);
    }
  }
  
  
  /**
   * Returns a list of ascending full rows.
   * 
   * @param rns         ascending list of row numbers
   * @param logChannel  seekable stream
   * 
   * @return not null
   * 
   * @throws IllegalArgumentException if {@code rns} is not ascending
   * @throws NoSuchElementException   if there are too few ledgerable rows in the {@code logChannel}
   */
  public List<FullRow> getFullRows(List<Long> rns, SeekableByteChannel logChannel)
      throws IOException, IllegalArgumentException, NoSuchElementException {
    
    if (rns.isEmpty())
      return List.of();
    
    
    List<FullRow> rows = new ArrayList<>(rns.size());
    
    State state = State.EMPTY;
    for (long rn : rns) {
      if (rn <= state.rowNumber())
        throw new IllegalArgumentException("out-of-bounds/sequence rn: " + rn);
      FullRow row = getFullRow(rn, state, logChannel);
      rows.add(row);
      state = row.toState();
    }
    
    return rows;
  }
  
  
  /**
   * Plays the given log and returns the specified row. Invokes the
   * {@linkplain #nextStateAhead(State, long)} lookup method, positions
   * the stream and returns
   * {@linkplain #getFullRow(long, State, ReadableByteChannel)}.
   * 
   * <h4>No Side Effects</h4>
   * <p>
   * By default, this method has no side effects. This is because a private subclass is
   * used to do the actual parsing.
   * </p>
   * 
   * @param rn          target row number
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
  public FullRow getFullRow(long rn, SeekableByteChannel logChannel)
      throws IOException, IllegalArgumentException, NoSuchElementException {
    
    return getFullRow(rn, State.EMPTY, logChannel);
  }
  
  /**
   * Plays the given log and returns the specified row. Invokes the
   * {@linkplain #nextStateAhead(State, long)} lookup method, positions
   * the stream and returns
   * {@linkplain #getFullRow(long, State, ReadableByteChannel)}.
   * 
   * <h4>No Side Effects</h4>
   * <p>
   * By default, this method has no side effects. This is because a private subclass is
   * used to do the actual parsing.
   * </p>
   * 
   * @param rn          target row number
   * @param state       state <em>before</em> {@code rn}; i.e. {@code state.rowNumber() < rn}
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
  public FullRow getFullRow(
      long rn, State state, SeekableByteChannel logChannel)
          throws IOException, IllegalArgumentException, NoSuchElementException {
          
    synchronized (this) {
      state = nextStateAhead(state, rn);
    }
    logChannel.position(state.eolOffset());
    
    return getFullRow(rn, state, (ReadableByteChannel) logChannel);
  }
  
  
  
  /**
   * Plays the given log forward and returns a {@linkplain FullRow}
   * for the given row number. The given log channel is assumed to be positioned at the
   * beginning of content following the {@linkplain State#rowNumber()
   * state row number}.
   * 
   * <h4>No Side Effects</h4>
   * <p>
   * By default, this method has no side effects. This is because a private subclass is
   * used to do the actual parsing.
   * </p>
   * 
   * @param rn         target row number
   * @param state      state <em>before</em> target row, i.e. {@code state.rowNumber() < rn}
   * @param logChannel positioned at end of state row [{@code state.rowNumber()}]
   * 
   * @return not null
   * 
   * @throws IllegalArgumentException if {@code rn <= state.rowNumber()}
   * @throws NoSuchElementException if the target row number is never reached
   */
  public FullRow getFullRow(
      long rn, State state, ReadableByteChannel logChannel)
          throws IOException, IllegalArgumentException, NoSuchElementException {
    
    if (rn <= state.rowNumber())
      throw new IllegalArgumentException(
          "row [%d] <= state [%d]".formatted(rn, state.rowNumber()));
    
    class RowCollector extends Collector<FullRow> {

      @Override
      protected void observeRow(
          HashFrontier preFrontier, List<ColumnValue> cols,
          long offset, long endOff, long lineNo) {
        
        if (preFrontier.rowNumber() + 1 == rn) {
          this.item = new FullRow(cols, preFrontier, endOff, lineNo);
        }
      }
    }
    
    var c = new RowCollector();
    var rState = c.play(logChannel, state);
    
    if (c.failed())
      throw new NoSuchElementException(
          "row [%d] never reached; final row [%d]".formatted(rn, rState.rowNumber()));
    
    return c.item;
  }
  
  
  public ByteBuffer getRowHash(long rn, SeekableByteChannel logChannel)
      throws IOException, IllegalArgumentException, NoSuchElementException {
    
    return getRowHash(rn, State.EMPTY, logChannel);
  }
  
  
  
  public ByteBuffer getRowHash(long rn, State state, SeekableByteChannel logChannel)
      throws IOException, IllegalArgumentException, NoSuchElementException {
    
    state = nextStateAhead(state, rn + 1);
    if (state.rowNumber() == rn)
      return state.rowHash();
    
    logChannel.position(state.eolOffset());
    return getRowHash(rn, state, (ReadableByteChannel) logChannel);
  }
  
  
  public ByteBuffer getRowHash(long rn, State state, ReadableByteChannel logChannel)
      throws IOException, IllegalArgumentException, NoSuchElementException {
    
    long srn = state.rowNumber();
    
    if (rn < srn)
      throw new IllegalArgumentException(
          "row [%d] < state [%d]".formatted(rn, srn));
    
    if (rn == srn)
      return state.rowHash();
    
    class FroCollector extends Collector<Fro> {

      @Override
      protected void observeLedgeredLine(Fro fro, long offset) throws IOException {
        if (fro.rowNumber() == rn)
          this.item = fro;
      }
    }
    
    var c = new FroCollector();
    var rState = c.play(logChannel, state);
    
    if (rState.rowNumber() != rn) {
      assert c.failed();
      throw new NoSuchElementException(
          "row [%d] never reached; final row [%d]".formatted(rn, rState.rowNumber()));
    }
    
    return rState.rowHash();
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
      
      var fr = getFullRow(rn, state, log);
      rows[index] = fr.row();
      state = fr.toState();
    }
    
    return Lists.asReadOnlyList(rows);
  }
  
  
  /**
   * Returns the next saved state ahead of row number {@code rn}, if any;
   * {@code state}, otherwise. This a look-ahead hook for a subclass that
   * implements it: if an instance has a saved state with a higher row number
   * that is still ahead of {@code rn}, then it should return that instance.
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
   * Returns the skip path connecting the specified rows.
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
  
  
  
  /**
   * Returns the path connecting the high row {@code hiRn}, to the given
   * target rows {@code targetRns}, to row [1].
   * 
   * @param targetRns target row numbers &gt; 0 and &le; {@code hiRn}
   * @param hiRn      highest row number
   * @param log       source log
   * 
   * @throws NoSuchElementException
   *         if {@code log} does not contain {@code hiRn} ledgerable lines
   */
  public Path getPath(Collection<Long> targetRns, long hiRn, SeekableByteChannel log)
      throws IOException, NoSuchElementException {
    
    if (hiRn < 1)
      throw new IllegalArgumentException("hiRn " + hiRn);
    
    if (targetRns.isEmpty())
      return getPath(1L, hiRn, log);
    
    var rns =  SkipLedger.stitchCollection(
        CollectionUtils.concat(targetRns, List.of(1L, hiRn)));
    
    if (rns.get(rns.size() - 1) < hiRn)
      throw new IllegalArgumentException(
          "hiRn " + hiRn + " > max targetRns " + rns.get(rns.size() - 1));
    
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
   * @return EOL position in {@code buffer} (1 beyond {@code '\n'} or at end-of-stream)
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


  class Collector<T> extends StateHasher {
    
    T item;
    
    Collector() {
      super(StateHasher.this);
    }
    
    boolean failed() {
      return item == null;
    }
    

    @Override
    protected boolean stopPlay() {
      return item != null;
    }
  }

}















