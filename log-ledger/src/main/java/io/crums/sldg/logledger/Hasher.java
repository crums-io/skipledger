/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.logledger;


import static io.crums.sldg.SldgConstants.DIGEST;

import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.crums.sldg.HashConflictException;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.logledger.LogParser.Listener;
import io.crums.sldg.salt.TableSalt;
import io.crums.sldg.src.SaltScheme;
import io.crums.util.Lists;
import io.crums.util.Strings;

/**
 * A {@linkplain Listener} for computing the skipledger hash
 * of a log. This class defines its own listener interface for the per-row
 * artifacts (hashes) it produces.
 * <p>
 * The common artifact generated after a run of parser
 * ({@linkplain LogParser#parse()}) is the final {@linkplain #parseState()
 * parse state}.
 * </p>
 * 
 * @see Hasher.RowHashListener
 * @see LogParser#pushListener(Listener)
 */
public class Hasher implements Listener {
  
  
  
  
  
  /**
   * Creates and returns an instance, sans salt.
   */
  public static Hasher initNoSalt() {
    return new Hasher(SaltScheme.NO_SALT, null, HashFrontier.SENTINEL);
  }
  
  /**
   * Creates and returns a salted instance.
   * 
   * @param salter the log's salt.
   */
  public static Hasher initInstance(TableSalt salter) {
    return new Hasher(SaltScheme.SALT_ALL, salter, HashFrontier.SENTINEL);
  }
  
  /**
   * Creates and returns a new instance with the given rules and
   * optional checkpoint.
   */
  public static Hasher initInstance(
      HashingRules rules, Optional<Checkpoint> checkpoint) {
    return checkpoint.isEmpty() ?
        new Hasher(rules) :
          new Hasher(rules, checkpoint.get());
  }
  
  
  /**
   * Callback interface invoked by {@linkplain
   * Hasher#ledgeredLine(long, Grammar, long, long, ByteBuffer)} on
   * computing the input-hash of the ledgered line, and its corresponding
   * row hash.
   */
  public interface RowHashListener {


    /**
     * Composes and returns an instance using the given listeners.
     * The returned listener invokes the individual listeners in order.
     * 
     * @param listeners not {@code null}
     */
    public static RowHashListener compose(
        Collection<RowHashListener> listeners) {
      RowHashListener[] array =
          listeners.toArray(new RowHashListener[listeners.size()]);
      return compose(array);
    }
    
    /**
     * Composes and returns an instance using the given listeners.
     * The returned listener invokes the individual listeners in order.
     * 
     * @param listeners not {@code null}
     */
    public static RowHashListener compose(RowHashListener... listeners) {
      // validate
      for (int index = listeners.length; index-- > 0; )
        if (listeners[index] == null)
          throw new NullPointerException(
              "listener at index " + index + " is null");
      
      return
          listeners.length == 1 ?
              listeners[0] :
                new RowHashListener() {
                @Override
                public void rowHashParsed(
                    ByteBuffer inputHash,
                    HashFrontier frontier, HashFrontier prevFrontier) {
                  
                  for (int index = 0; index < listeners.length; ++index)
                    listeners[index].rowHashParsed(
                        inputHash.clear(), frontier, prevFrontier);
                  
                }
                @Override
                public void parseEnded() {
                  for (int index = 0; index < listeners.length; ++index)
                    listeners[index].parseEnded();
                }
              };
    }
    
    /**
     * No-op instance.
     */
    public final static RowHashListener NOOP = new RowHashListener() {
      @Override
      public void rowHashParsed(
          ByteBuffer inputHash,
          HashFrontier frontier,
          HashFrontier prevFrontier) {  }
    };
    
    
    /**
     * Observes the just-hashed row (ledgered line).
     * 
     * <h4>Invariants</h4>
     * <p>
     * None of the arguments are {@code null}, the {@code inputHash}
     * is not modified, and
     * {@code prevFrontier.nextFrontier(inputHash).equals(frontier)} evaluates
     * to {@code true}.
     * </p>
     * <h4>{@code inputHash} Protocol</h4>
     * <p>
     * Here, the {@code inputHash} parameter is designed so that it need not
     * be {@linkplain ByteBuffer#duplicate() duplicated}. Its capacity is
     * 32-bytes ({@linkplain SldgConstants#HASH_WIDTH}). Its remaining bytes
     * is kept at capacity; however, code evaluating its value, should not
     * depend on this. Instead, use the value returned by
     * {@code inputHash.duplicate().clear()}.
     * </p>
     * 
     * 
     * @param inputHash         the computed input hash (read-only)
     * @param frontier          the current frontier
     * @param prevFrontier      the previous frontier
     */
    void rowHashParsed(
        ByteBuffer inputHash, HashFrontier frontier, HashFrontier prevFrontier);
    
    

    /**
     * Invoked on the conclusion of a parse run. There will be no more
     * calls to this instance during this run.
     */
    default void parseEnded() {  }
  }
  
  
  private final MessageDigest rowDigester = DIGEST.newDigest();
  private final MessageDigest cellDigester = DIGEST.newDigest();
  private final SaltScheme saltScheme;
  private final TableSalt salter;
  
  
  private HashFrontier frontier;
  private long eol;
  private HashFrontier prevFrontier;
  private long prevEol;
  
  private ByteBuffer inputHash;
  
  private long lastRn;
  
  /**
   * @see #ledgeredLine(long, Grammar, long, long, ByteBuffer)
   */
  private RowHashListener rowHashListener = RowHashListener.NOOP;
  
  
  /**
   * Constructs an instance initialized with the given {@linkplain Checkpoint}.
   * This is the preferred constructor as it "replays" and validates the last row
   * parsed.
   * 
   * @param saltScheme  not {@code null}
   * @param salter      not {@code null}, if {@code saltScheme.hasSalt()}   
   * @param initState   initial parse state
   */
  public Hasher(SaltScheme saltScheme, TableSalt salter, Checkpoint initState) {
    this(saltScheme, salter, initState.frontier());
    this.inputHash = initState.inputHash();
    this.eol = initState.eol();
    this.prevFrontier = initState.preState().frontier();
    this.prevEol = initState.preState().eol();

    if (saltScheme.hasSalt() && salter == null)
      throw new NullPointerException("null salter: saltScheme uses salt");
    
  }
  
  
  /**
   * Constructs an instance initialized with the given {@linkplain Checkpoint}.
   * This is the preferred constructor as it "replays" and validates the last row
   * parsed.
   * 
   * @param rules
   * @param initState
   */
  public Hasher(HashingRules rules, Checkpoint initState) {
    this(
        rules.saltScheme(),
        rules.salter().orElse(null),
        initState.frontier());
    
    this.inputHash = initState.inputHash();
    this.eol = initState.eol();
    this.prevFrontier = initState.preState().frontier();
    this.prevEol = initState.preState().eol();
  }
  
  
  /**
   * Constructs an instance for parsing a log from the beginning.
   */
  public Hasher(HashingRules rules) {
    this(
        rules.saltScheme(),
        rules.salter().orElse(null),
        HashFrontier.SENTINEL);
  }
  

  /**
   * 
   * @param saltScheme  not {@code null}
   * @param salter      not {@code null}, if {@code saltScheme.hasSalt()}
   * @param frontier    not {@code null}
   */
  private Hasher(SaltScheme saltScheme, TableSalt salter, HashFrontier frontier) {
    this.saltScheme = saltScheme;
    this.salter = salter;
    this.frontier = frontier;
    
  }
  
  
  /**
   * Sets one or more row-hash listeners.
   * 
   * @param listener    not {@code null}
   * 
   * @see RowHashListener#compose(RowHashListener...)
   */
  public Hasher setRowHashListeners(RowHashListener... listener) {
    this.rowHashListener = RowHashListener.compose(listener);
    return this;
  }
  

  @Override
  public void lineOffsets(long offset, long lineNo) {   }

  @Override
  public void ledgeredLine(
      long rowNo, Grammar grammar, long offset, long lineNo, ByteBuffer line) {
    
    
    final long fRn = frontier.rowNo();
    
    // assert rowNo is in sequence..
    if (rowNo != lastRn + 1) {
      if (rowNo <= lastRn || lastRn != 0 && rowNo >= lastRn + 2)
        throw new IllegalStateException(
            "out-of-sequence row no. %d; last row no. was %d"
            .formatted(rowNo, lastRn));
      if (rowNo > fRn + 1)
        throw new IllegalStateException(
            "out-of-sequence row no. %d; last known row no. is %d"
            .formatted(rowNo, frontier.rowNo()));
    }
    
    if (rowNo == fRn + 1) {
      // the usual path
      advanceFrontier(rowNo, grammar, offset, line);
      rowHashListener.rowHashParsed(inputHash, frontier, prevFrontier);
    
    } else if (rowNo == fRn) {
      
      // corner case.. assert previous frontier hash matches
      // (if we have the data)
      if (prevFrontier != null) {

        final long eolOffset = offset + line.remaining();
        
        var inputHash = computeInputHash(rowNo, grammar, line);
        boolean hashesMatch =
            prevFrontier.nextRow(inputHash).hash().equals(
                frontier.frontierHash());
        
        if (!hashesMatch)
          throw new HashConflictException(
              "row [%d] hash conflicts with frontier hash. Line (quoted): '%s'"
              .formatted(rowNo, Strings.utf8String(line)));
        
        if (eol != eolOffset) {
          // it shouldn't matter, but it's surprising (!)
          // so we want to know..
          System.getLogger(LglConstants.LOG_NAME)
          .log(Level.WARNING,
              "expected EOL %d for row[%d], line[%d], offset[%d]; actual EOL was %d"
              .formatted(eol, rowNo, lineNo, offset, eolOffset));
          
          
//          prevEol = eolOffset;
        }
      }
    } // else we silently ignore rows (lines) before the frontier row no
    
    lastRn = rowNo;
  }
  
  
  private long cIhCount;
  private long tokenCount;
  
  /**
   * Aggregrate statistics collected by the instance.
   * 
   * @param rows        no. of rows (lines) whose input hash was computed
   * @param tokens      total no. of tokens generated from all rows
   * 
   * @see Hasher#getStats()
   */
  public record Stats(long rows, long tokens) {  }
  
  /**
   * Returns aggregate statistics: rows processed, tokens generated, etc.
   */
  public Stats getStats() {
    return new Stats(cIhCount, tokenCount);
  }
  
  /** Computes and returns the input hash with no side effects. */
  private ByteBuffer computeInputHash(
      long rowNo, Grammar grammar, ByteBuffer line) {

    var tokens = grammar.parseTokens(Strings.utf8String(line));
    
    var tokenBytes = Lists.map(tokens, Strings::utf8Bytes);
    
    final int cc = tokens.size();
    
    ++cIhCount;
    tokenCount += cc;
    
    rowDigester.reset();
    
    if (cc == 0) {
      
      return DIGEST.sentinelHash();
    
    } else if (cc == 1) {
      
      if (saltScheme.isSalted(0))
        rowDigester.update(salter.cellSalt(rowNo, 0, cellDigester));
      rowDigester.update(tokenBytes.get(0));
      
    } else if (saltScheme.saltAll()) {
      
      byte[] rowSalt = salter.rowSalt(rowNo, cellDigester);
      for (int index = 0; index < cc; ++index) {
        var cellSalt = TableSalt.cellSalt(rowSalt, index, cellDigester);
        cellDigester.reset();
        cellDigester.update(cellSalt);
        cellDigester.update(tokenBytes.get(index));
        rowDigester.update(cellDigester.digest());
      }
      
    } else if (saltScheme.hasSalt()) {
      // NOTE: this deals w/ custom salt schemes
      // (where *some cells/tokens are *not salted);
      // however, this is not presently modeled for logs
      // (because it is a likely useless feature). So this is
      // DEAD CODE..
      byte[] rowSalt = salter.rowSalt(rowNo, cellDigester);
      for (int index = 0; index < cc; ++index) {
        if (saltScheme.isSalted(index)) {
          var cellSalt = TableSalt.cellSalt(rowSalt, index, cellDigester);
          cellDigester.reset();
          cellDigester.update(cellSalt);
        } else
          cellDigester.reset();
        cellDigester.update(tokenBytes.get(index));
        rowDigester.update(cellDigester.digest());
      }
      
    } else {
      
      for (int index = 0; index < cc; ++index) {
        cellDigester.reset();
        rowDigester.update(cellDigester.digest(tokenBytes.get(index)));
      }
    }
    
    return ByteBuffer.wrap(rowDigester.digest()).asReadOnlyBuffer();
  }
  
  
  @Override
  public void parseEnded() {
    rowHashListener.parseEnded();
  }
  
  
  
  /** This boils down to calculating the row's input hash.. */
  private void advanceFrontier(long rowNo, Grammar grammar, long offset, ByteBuffer line) {

    prevEol = eol;
    eol = offset + line.remaining();
    inputHash = computeInputHash(rowNo, grammar, line);
    prevFrontier = frontier;
    frontier = frontier.nextFrontier(inputHash.duplicate());
    
  }

  
  @Override
  public void skippedLine(long offset, long lineNo, ByteBuffer line) {  }
  
  
  
  /**
   * Determines whether the current parse state has any ledgered lines.
   * If so, then invoking {@linkplain #parseState()} will succeed.
   * 
   * @see #parseState()
   */
  public boolean hasParseState() {
    return inputHash != null;
  }
  
  
  /**
   * Returns the parse state. <em>Not thread-safe while
   * {@linkplain LogParser#parse() parsing}!</em>
   * 
   * @throws NoSuchElementException
   *         if {@code hasParseState()} returns {@code false}
   *         
   * @see #hasParseState()
   */
  public Checkpoint parseState() throws NoSuchElementException {
    if (inputHash == null)
      throw new NoSuchElementException("no new ledgerable lines parsed");
    var preState = new LogState(prevFrontier, prevEol);
    
    return new Checkpoint(preState, inputHash.clear(), eol);
  }
  

}






















