/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

import io.crums.client.Client;
import io.crums.client.ClientException;
import io.crums.io.Serial;
import io.crums.model.CrumTrail;
import io.crums.sldg.RowHash;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.cache.HashedRow;
import io.crums.sldg.logs.text.ContextedHasher.Context;
import io.crums.sldg.time.TrailedRow;

/**
 * The minimum information capturing the skip hash of a log file, how it was
 * calculated, and optionally, when it was witnessed.
 * 
 * <h2>Seals are Secret!</h2>
 * <p>
 * The seal file contains a high-entropy salt value used by the hashing scheme.
 * Its value is never supposed to be exposed: otherwise, an adversary may be able to
 * work out certain values from their hashes (dictionary-attacks).
 * </p>
 */
public class Seal implements Serial {
  
  private final RowHash rowHash;
  
  private final HashingGrammar rules;
  
  
  /**
   * Creates a trailed instance.
   * 
   * @param trail   a row number and a crumtrail
   * @param rules   hashing rules, not null
   */
  public Seal(TrailedRow trail, HashingGrammar rules) {
    this((RowHash) trail, rules);
  }
  
  /**
   * Creates an untrailed instance.
   * 
   * @param rowNumber &ge; 1
   * @param hash      skip ledger row hash. 32-bytes, do not modify (!)
   * @param rules     hashing rules, not null
   */
  public Seal(long rowNumber, ByteBuffer hash, HashingGrammar rules) {
    this(new HashedRow(rowNumber, hash), rules);
  }
  
  private Seal(RowHash rowHash, HashingGrammar rules) {
    this.rowHash = Objects.requireNonNull(rowHash, "null rowHash");
    this.rules = Objects.requireNonNull(rules, "null hash grammar");
    if (rules.isSaltCleared())
      throw new IllegalArgumentException("salt cleared in rules: " + rules);
  }
  
  
  /** Returns the last row number (equivalently, the number of ledgerable lines in the log).  */
  public long rowNumber() {
    return rowHash.no();
  }
  
  
  /** Returns the hash of the last row (equivalently, the hash of the entire log). */
  public ByteBuffer hash() {
    return rowHash.hash();
  }
  
  
  /** Returns the hashing rules. Note the salt must be kept secret. */
  public HashingGrammar rules() {
    return rules;
  }
  
  
  /**
   *  Determines if this instance has a {@link TrailedRow}.
   *  
   *  @see #trail()
   */
  public boolean isTrailed() {
    return rowHash instanceof TrailedRow;
  }
  
  
  /**
   * Returns the optional {@code TrailedRow}.
   * 
   * @see TrailedRow
   */
  public Optional<TrailedRow> trail() {
    return rowHash instanceof TrailedRow trail ? Optional.of(trail) : Optional.empty();
  }
  
  
  /** Returns a trailed version of this instance. */
  public Seal trail(CrumTrail trail) {
    if (!trail.crum().hash().equals(hash()))
      throw new IllegalArgumentException("trail hash mismatch: " + trail);
    return new Seal(new TrailedRow(rowNumber(), trail), rules);
  }
  

  /** Returns a trailed version of this instance. */
  public Seal trail(TrailedRow trail) {
    if (!trail.equals(rowHash))
      throw new IllegalArgumentException("trail mismatch: " + trail);
    return new Seal(trail, rules);
  }
  
  
  /**
   * Verifies this seal against the source {@code log} file. In order for verification
   * to succeed, the {@code log} must contain at least {@linkplain #rowNumber()}-many
   * ledgerable lines and the calculated hash for this row number must equal
   * {@linkplain #hash()}.
   * <p>
   * The log file is played up to one-beyond this seal's row number. The return value
   * can be used to determine if there are more ledgerable rows in log--among other
   * things.
   * </p>
   * 
   * @param log   the text-based log file whose state this seal captures
   * 
   * @return the state of the log up to the seal row number, or one beyond
   *         
   * @throws RowHashConflictException if there are fewer rows in the {@code log} than recorded in the seal,
   *                                  or if the hash of the row recorded in the seal conflicts with that
   *                                  actually in the log file
   */
  public Fro verify(File log) throws IOException, RowHashConflictException {
    
    final long rn = rowNumber();
    
    class LastFroCollector implements Context {
      Fro last;
      @Override
      public void observeEndState(Fro fro) throws IOException {
        last = fro;
      }
    }
    
    var collector = new LastFroCollector();
    
    var state = new ContextedHasher(
        rules.stateHasher(),
        ContextedHasher.newLimitContext(rn + 1),
        collector).play(log);
    
    Fro fro = collector.last;
    if (fro == null)
      throw new IllegalStateException(
          log + " has no ledgerable lines. Did not collect last Fro on playing log");
    
    assert state.rowNumber() == fro.rowNumber();
    
    final boolean extended = fro.rowNumber() == rn + 1;
    
    if (fro.rowNumber() < rn)
      throw new RowHashConflictException(rn,
          "%s has been trimmed to fewer rows: expected [%d], actual [%d]"
          .formatted(log, rn, fro.rowNumber()));

    
    assert extended || fro.rowNumber() == rn;
    
    ByteBuffer rowHash = extended ? fro.preState().rowHash() : fro.rowHash();
    
    if (!rowHash.equals(hash())) {
      State actualState = extended ? fro.preState() : fro.state();
      throw new RowHashConflictException(rn,
          "%s has been logically modified; hash for row [%d] conflicts with that in file (%d:%d)"
          .formatted(log, rn, actualState.lineNo(), actualState.eolOffset()));
    }
    
    return fro;
  }
  
  
  /**
   * Plays the contents of the given log file, and returns the resultant seal. 
   * Along the way, the hash of the row at this row number is verified to match that
   * recorded in this instance. 
   *  
   * @param log   the text-based log file whose state this seal captures
   * @return      {@code this} or new seal at a higher row number
   * 
   * @throws IOException
   * @throws RowHashConflictException
   *         if the contents of {@code log} have been logically modified before this
   *         seal's row number. 
   */
  public Seal extend(File log) throws IOException, RowHashConflictException {
    
    Fro fro = verify(log);
    boolean extended = fro.rowNumber() == rowNumber() + 1;
    
    if (!extended)
      return this;
    
    State state = rules().stateHasher().play(log, fro.state());
    
    return new Seal(state.rowNumber(), state.rowHash(), rules());
  }
  
  
  
  /**
   * Witnesses the hash of this seal, if not already trailed (witnessed).
   * 
   * @return {@code this}, if already trailed, or if a crumtrail (final witness record)
   *         was not retrieved; otherwise a new trailed seal
   * 
   * @throws ClientException on a network or service error, or some such
   */
  public Seal witness() throws ClientException {
    if (isTrailed())
      return this;
    
    Client remote = new Client();
    remote.addHash(hash());
    var crecord = remote.getCrumRecords().get(0);
    if (!crecord.isTrailed())
      return this;
    return trail(crecord.trail());
  }

  
  
  
  //   S E R I A L
  
  
  @Override
  public int serialSize() {
    var trail = trail();
    int bytes = 8;
    if (trail.isPresent())
      bytes += trail.get().trail().serialSize();
    else
      bytes += SldgConstants.HASH_WIDTH;
    bytes += rules.serialSize();
    return bytes;
  }

  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    var trail = trail();
    long encodedRn = (trail.isEmpty() ? -1L : 1L) * rowNumber();
    out.putLong(encodedRn);
    if (trail.isPresent())
      trail.get().trail().writeTo(out);
    else
      out.put(hash());
    rules.writeTo(out);
    return out;
  }
  

  /**
   * Loads and returns the serial representation of an instance.
   */
  public static Seal load(ByteBuffer in) throws BufferUnderflowException {
    long encodedRn = in.getLong();
    if (encodedRn == 0)
      throw new IllegalArgumentException("zero row number");
    
    boolean trailed = encodedRn > 0;
    RowHash rowHash;
    if (trailed) {
      long rn = encodedRn;
      var crumtrail = CrumTrail.load(in);
      rowHash = new TrailedRow(rn, crumtrail);
    } else {
      long rn = -encodedRn;
      byte[] hash = new byte[SldgConstants.HASH_WIDTH];
      in.get(hash);
      rowHash = new HashedRow(rn, ByteBuffer.wrap(hash));
    }
    var rules = HashingGrammar.load(in);
    
    return new Seal(rowHash, rules);
  }

}
















