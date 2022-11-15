/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import io.crums.sldg.SldgConstants;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.StringValue;
import io.crums.sldg.src.TableSalt;
import io.crums.util.Strings;

/**
 * Calculates the structured hash of the given log.
 * <p>
 * TODO: support for tracking the log-line offsets.
 * </p>
 * 
 * @see #appendLog(InputStream)
 */
public class LogHasher {
  
  /**
   * Chunky constructor arguments.
   * 
   * @see LogHasher#LogHasher(BaseArgs)
   */
  public record BaseArgs(
      SourceRowParser rowParser, TableSalt salter, HashFrontier frontier) {
    
    /**
     * 
     * @param rowParser   the log entry tokenizer
     * @param salter      not {@code null}
     * @param frontier    if {@code null} then this is a new log ledger; otherwise, the
     *                    last state of the ledger
     */
    public BaseArgs {
      Objects.requireNonNull(rowParser, "null rowParser");
      Objects.requireNonNull(salter, "null salter");
    }
    
    public HashFrontier frontier() {
      return frontier == null ? HashFrontier.SENTINEL : frontier;
    }
  }
  

  private final MessageDigest work = SldgConstants.DIGEST.newDigest();
  private final MessageDigest work2 = SldgConstants.DIGEST.newDigest();
  
  private final SourceRowParser rowParser;
  private final TableSalt salter;
  private HashFrontier frontier;

  
  /**
   * Constructs an instance starting with an empty log.
   * 
   * @param rowParser   the log entry tokenizer
   * @param salter      not {@code null}
   */
  public LogHasher(SourceRowParser rowParser, TableSalt salter) {
    this(new BaseArgs(rowParser, salter, null));
  }
  
  /**
   * Full constructor.
   */
  public LogHasher(SourceRowParser rowParser, TableSalt salter, HashFrontier frontier) {
    this(new BaseArgs(rowParser, salter, frontier));
  }
  
  /**
   * Full constructor.
   * 
   * @param args chuncky arguments
   * 
   * @see BaseArgs
   */
  public LogHasher(BaseArgs args) {
    this.rowParser = args.rowParser();
    this.salter = args.salter();
    this.frontier = args.frontier();
  }


  /**
   * The {@code acceptRow} implementation used for the {@code LogParser}.
   * (The class itself does not implement the interface; no reason to.)
   * 
   * @param text    a line of text that may potentially count as a row
   * @param offset  the offset it occurs in the stream (to be used later)
   * 
   * @return {@code true} iff a row was added
   */
  protected boolean acceptRow(ByteBuffer text, long offset) {
    var str = new String(
        text.array(),
        text.arrayOffset(),
        text.remaining(),
        Strings.UTF_8);
    var tokens = rowParser.apply(str);
    if (tokens.isEmpty())
      return false;
    long rn = frontier.rowNumber() + 1;
    var columns = new ColumnValue[tokens.size()];
    for (int index = 0; index < columns.length; ++index) {
      columns[index] = new StringValue(
          tokens.get(index),
          salter.salt(rn, index + 1));
    }
    var inputHash = new SourceRow(rn, columns).rowHash(work, work2);
    frontier = frontier.nextFrontier(inputHash, work);
    observeRowOffset(frontier.rowNumber(), offset);
    return true;
  }
  
  
  /** Hook for observing row offset. Noop base implementation. */
  protected void observeRowOffset(long rowNumber, long offset) {  }
  
  
  /**
   * Appends the contents of the given log stream to the ledger.
   * If no state was specified at instantiation, then the hash of 
   * a new ledger is calculated.
   * 
   * @param in  not null
   * @return    the number of rows added to the ledger (possibly zero)
   */
  public long appendLog(InputStream in) throws UncheckedIOException {
    var parser = new LogParser(this::acceptRow, in);
    parser.run();
    return parser.rowNumber();
  }
  
  
  
  
  
  /** Returns the state of the ledger. */
  public HashFrontier frontier() {
    return frontier;
  }
  
  
  /** Returns the row parser (set at construction). */
  public SourceRowParser rowParser() {
    return rowParser;
  }

}







