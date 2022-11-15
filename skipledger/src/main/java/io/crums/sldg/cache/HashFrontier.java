/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cache;


import static io.crums.sldg.SldgConstants.DIGEST;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import io.crums.sldg.Row;
import io.crums.sldg.RowHash;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.util.Lists;

/**
 * Models the <em>minimum</em> information necessary starting from some row numer
 * for constructing subsequent <em>row</em>-hashes in a {@linkplain SkipLedger}, given
 * the subsequent rows' {@linkplain Row#inputHash() input-hash}es.
 * 
 * <h2>Motivation</h2>
 * <p>
 * Excepting pedagogical or niche use cases such as testing, useful {@linkplain SkipLedger}
 * implementations are I/O bound. This construct is designed to minimize that I/O. The use cases
 * envisioned at this time are:
 * </p>
 * <ol>
 * <li><em>Verifying a source ledger matches its skipledger.</em><p>In order to verify that a
 * sequence of input-hashes from a source ledger (whether from a {@linkplain io.crums.sldg.SourceLedger SourceLedger} or
 * another application-defined format) matches the state of a skipledger across a range of row
 * numbers, one loads an instance starting from the previous row number (via {@linkplain
 * #loadFrontier(SkipLedger, long)}) and then runs it forward (consuming the sequence of hashes)
 * to the last row number in the range (which doesn't involve accessing the skipledger) and then
 * verifies the frontier's {@linkplain #frontierRow() hi-row} {@linkplain RowHash#hash() hash} matches
 * the skipledger's {@linkplain SkipLedger#rowHash(long) row-hash} at that last row number in
 * the range.</p><br/></li>
 * 
 * <li><em>Internal use in skipledger write-path.</em><p>Appending rows to a skipledger
 * involves internal look-ups of previous row-hashes, which are themselves I/O bound.
 * Opportunities here:
 * </p>
 *   <ul>
 *   <li>{@linkplain SkipLedger#appendRows(ByteBuffer)}. When appending <em>en-bloc</em>.</li>
 *   <li>Propose input-hash streaming append function in {@code SkipLedger}. (Not implemented
 *   cuz haven't thought thru the locking.)</li>
 *   </ul>
 * </li>
 * </ol>
 * 
 * <h2>Model &amp; state transitons</h2>
 * <p>
 * This class adds row-hash information to the parent model. (See {@linkplain Frontier}).
 * </p><p>
 * Instances are <em>immutable</em>. An instance itself doesn't track input-hashes, but it
 * takes one in order to transiton from one {@linkplain #rowNumber() row number} to the next:
 * {@linkplain #nextFrontier(ByteBuffer)}. In this way, the state of a skipledger can be
 * "played forward" by only accessing the source-ledger.
 * </p>
 * 
 * @see #firstRow(ByteBuffer, MessageDigest)
 */
public class HashFrontier extends Frontier {
  
  
  /**
   * Stateless instance representing an empty ledger.
   * This is really a hack, so as to avoid checking for the null instance
   * (which I don't want). The <em>only</em> method that works on this instance
   * is {@linkplain HashFrontier#nextFrontier(ByteBuffer, MessageDigest)}.
   */
  public final static HashFrontier SENTINEL = new HashFrontier(new RowHash[0]) {
    @Override
    public HashFrontier nextFrontier(ByteBuffer inputHash, MessageDigest digest) {
      return firstRow(inputHash, digest);
    }
    @Override
    public long rowNumber() {
      return 0L;
    }
  };
  
  
  /**
   * @return {@code firstRow(inputHash, null)}
   * @see #firstRow(ByteBuffer, MessageDigest)
   */
  public static HashFrontier firstRow(ByteBuffer inputHash) {
    return firstRow(inputHash, null);
  }
  
  
  /**
   * Creates a first-row instance.
   * 
   * @param inputHash the input-hash for the first row (the hash of a row in a source ledger)
   * @param digest    Optional. Note if non-null, the algo is <em>not checked for validity</em>.
   * 
   * @return an instance positioned on row [1]
   * 
   * @throws NullPointerException if {@code inputHash} is {@code null}
   */
  public static HashFrontier firstRow(ByteBuffer inputHash, MessageDigest digest) {
    inputHash = sliceInput(inputHash);
    if (digest == null)
      digest = DIGEST.newDigest();
    else
      digest.reset();
    
    digest.update(inputHash.slice());
    digest.update(DIGEST.sentinelHash());
    RowHash[] levels = { new HashedRow(1L, digest.digest()) };
    return new HashFrontier(levels);
  }
  
  
  
  /**
   * Loads an instance from the given skipledger at the given row number.
   * 
   * @param skipLedger  not-empty skipledger
   * @param rowNumber   &ge; 1 and &le; {@code skipLedger.size()}
   * 
   * @return an instance positioned at row [rowNumber] of the skipledger
   * 
   * @throws NullPointerException
   */
  public static HashFrontier loadFrontier(SkipLedger skipLedger, long rowNumber) {
    ByteBuffer hash = skipLedger.rowHash(rowNumber);
    RowHash row = new HashedRow(rowNumber, hash);
    final int skipCount = SkipLedger.skipCount(rowNumber);
    final int frontierLevels = levelCount(rowNumber);
    RowHash[] levelFrontier = new RowHash[frontierLevels];
    for (int index = 0; index < skipCount; ++index)
      levelFrontier[index] = row;
    for (int index = skipCount; index < frontierLevels; ++index) {
      long levelRn = levelRowNumber(rowNumber, index);
      ByteBuffer levelHash = skipLedger.rowHash(levelRn);
      levelFrontier[index] = new HashedRow(levelRn, levelHash);
    }
    return new HashFrontier(levelFrontier);
  }
  
  
  
  //        M E M B E R S   /   C O N S T R U C T O R S
  
  
  
  
  private final RowHash[] levelFrontier;
  
  
  
  
  /**
   * Copy constructor (if you need it in a subclass).
   */
  protected HashFrontier(HashFrontier copy) {
    Objects.requireNonNull(copy, "null copy");
    // instances are immutable, so this is safe
    // (that is, as good as the copy)
    this.levelFrontier = copy.levelFrontier;
  }
  
  /**
   * No check.
   */
  private HashFrontier(RowHash[] levelFrontier) {
    this.levelFrontier = levelFrontier;
  }
  
  
  
  
  //      M E T H O D S
  
  
  
  @Override
  public final int levelCount() {
    return levelFrontier.length;
  }
  
  
  
  
  /**
   * Returns the frontier row.
   * 
   * @return row numbered {@linkplain #rowNumber()}
   */
  public final RowHash frontierRow() {
    return levelFrontier[0];
  }
  
  
  /**
   * Returns the hash of the frontier row.
   * 
   * @return {@code frontierRow().hash()}
   */
  public final ByteBuffer frontierHash() {
    return levelFrontier[0].hash();
  }
  
  
  /**
   * Returns the row at the given level.
   * 
   * @param level &ge; 1 and &lt; {@linkplain #levelCount()}
   */
  public RowHash levelRow(int level) {
    return levelFrontier[level];
  }
  
  
  
  public List<? extends RowHash> levelRows() {
    return Lists.asReadOnlyList(levelFrontier);
  }
  
  
  private static ByteBuffer sliceInput(ByteBuffer inputHash) {
    ByteBuffer input = inputHash.slice();
    if (input.remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException("input hash size: " + inputHash);
    return input;
  }
  
  
  public HashFrontier nextFrontier(ByteBuffer inputHash) {
    return nextFrontier(inputHash, null);
  }
  
  
  public HashFrontier nextFrontier(ByteBuffer inputHash, MessageDigest digest) {
    
    inputHash = sliceInput(inputHash);
    final long rn = rowNumber() + 1;
    
    final int skipCount = SkipLedger.skipCount(rn);
    
    assert skipCount != levelFrontier.length; // cuz there can't be 2 top levels
    
    // check to see if we need to expand (relatively rare)
    final boolean expand = skipCount > levelFrontier.length;
    
    assert !expand || skipCount == levelFrontier.length + 1 && rn == 2 * tail();
    

    // compute the hash for the new row
    
    if (digest == null)
      digest = DIGEST.newDigest();
    else
      digest.reset();
    
    // update the digest with a sequence consisting of the new row's input hash
    // followed by each row hash referenced by that row, from its 0th level to
    // to (skipCount - 1)'th, in that order.
    digest.update(inputHash);
    
    // (exclusive):
    final int lastNonSentinelLevel = expand ? levelFrontier.length : skipCount;
    
    for (int level = 0; level < lastNonSentinelLevel; ++level)
      digest.update(levelFrontier[level].hash());
    
    
    // the digest is prepared, excepting 1 corner case: when we expand
    // check for that, all while you're at it, allocated the next levels array..
    
    RowHash[] nextLevels;
    if (expand) {
      
      // when a new highest level row is created, it's highest level pointer
      // points to row zero (whose hash is just a string of zeros).
      digest.update(DIGEST.sentinelHash());
      nextLevels = new RowHash[skipCount];
    
    } else
      
      nextLevels = new RowHash[levelFrontier.length];
    
    // the digest is ready.
    HashedRow next = new HashedRow(rn, digest.digest());

    // copy the unaffected levels, back-to-front
    // (falls thru if *expand*-ed)
    for (int level = levelFrontier.length; level-- > skipCount; )
      nextLevels[level] = levelFrontier[level];
    
    // set the remaining levels to *next*, from skipCount - 1 to 0
    // (that's all 
    for (int level = skipCount; level-- > 0; )
      nextLevels[level] = next;
    
    return new HashFrontier(nextLevels);
  }
  
  

  @Override
  public long rowNumber() {
    return levelFrontier[0].rowNumber();
  }

  @Override
  public final long tail() {
    return levelFrontier[levelFrontier.length - 1].rowNumber();
  }

  
  @Override
  public List<Long> levelRowNumbers() {
    return new Lists.RandomAccessList<Long>() {

      @Override
      public Long get(int index) {
        return levelFrontier[index].rowNumber();
      }

      @Override
      public int size() {
        return levelFrontier.length;
      }
    };
  }
  
  

}
