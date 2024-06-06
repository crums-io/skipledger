/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cache;


import static io.crums.sldg.SldgConstants.DIGEST;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;

import io.crums.io.Serial;
import io.crums.sldg.ByteFormatException;
import io.crums.sldg.Row;
import io.crums.sldg.RowHash;
import io.crums.sldg.SerialRow;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.util.IntegralStrings;
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
 * sequence of input-hashes from a source
 * matches the state of a skipledger across a range of row
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
public class HashFrontier extends Frontier implements Serial {
  
  
  /**
   * Stateless instance representing an empty ledger.
   * This is really a hack, so as to avoid checking for the null instance
   * (which I don't want). The <em>only</em> methods that work on this instance
   * are {@linkplain HashFrontier#nextFrontier(ByteBuffer, MessageDigest)}, and
   * the zero-returning {@linkplain HashFrontier#frontierHash()} and
   * {@linkplain HashFrontier#rowNumber()} methods.
   * <p>
   * The {@code Serial} interface methods also work on this instance.
   * </p>
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
    @Override
    public ByteBuffer frontierHash() {
      return DIGEST.sentinelHash();
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

  private static ByteBuffer sliceInput(ByteBuffer inputHash) {
    ByteBuffer input = inputHash.slice();
    if (input.remaining() != SldgConstants.HASH_WIDTH)
      throw new IllegalArgumentException("input hash size: " + inputHash);
    return input;
  }
  
  
  private final static ByteBuffer SENTINEL_HASH = SldgConstants.DIGEST.sentinelHash();
  
  
  
  //        M E M B E R S   /   C O N S T R U C T O R S
  
  
  
  
  private final RowHash[] levelFrontier;
  
  
  /**
   * @param rowNumber   &ge; 1
   * @param levelHashes level hashes. Recall the hash at level zero is the
   *                    hash of row [{@code rowNumber}]
   * 
   * @see Frontier#levelCount(long)
   */
  public HashFrontier(long rowNumber, ByteBuffer[] levelHashes) {
    this(rowNumber, Lists.asReadOnlyList(levelHashes));
  }
  
  /**
   * @param rowNumber   &ge; 1
   * @param levelHashes level hashes. Recall the hash at level zero is the
   *                    hash of row [{@code rowNumber}]
   *
   * @see Frontier#levelCount(long)
   */
  public HashFrontier(long rowNumber, List<ByteBuffer> levelHashes) {
    SkipLedger.checkRealRowNumber(rowNumber);
    final int expectedLevels = levelCount(rowNumber);
    if (levelHashes.size() != expectedLevels)
      throw new IllegalArgumentException(
          "expected %d levels for row [%d]; actual %d".formatted(
              expectedLevels, rowNumber, levelHashes.size()));
    
    var deepHash = levelHashes.get(expectedLevels - 1);
    long deepRn = levelRowNumber(rowNumber, expectedLevels - 1);
    
    noSentinel(deepHash, expectedLevels - 1);
    
    for (int index = expectedLevels - 1; index-- > 0; ) {
      long levelRn = levelRowNumber(rowNumber, index);
      var levelHash = levelHashes.get(index);
      boolean sameHash = levelHash.equals(deepHash);
      if (levelRn == deepRn) {
        if (!sameHash)
          throw new IllegalArgumentException(
              "hash conflict for row [%d] at levelHashes[%d]"
              .formatted(levelRn, index));
      } else {
        if (sameHash)
          throw new IllegalArgumentException(
              "row [%d] and row [%d] set to the same hash (at levelHashes[%d] " +
              " for row number %d)"
              .formatted(deepRn, levelRn, index, rowNumber));
        noSentinel(levelHash, index);
        deepRn = levelRn;
        deepHash = levelHash;
      }
    }
    
    this.levelFrontier = new RowHash[expectedLevels];
    levelFrontier[0] = new HashedRow(rowNumber, levelHashes.get(0));
    long lastRn = rowNumber;
    for (int index = 1; index < expectedLevels; ++index) {
      long rn = levelRowNumber(rowNumber, index);
      if (lastRn == rn)
        levelFrontier[index] = levelFrontier[index - 1];
      else {
        levelFrontier[index] = new HashedRow(rn, levelHashes.get(index));
        lastRn = rn;
      }
    }
  }
  
  private void noSentinel(ByteBuffer hash, int index) {
    if (hash.equals(SENTINEL_HASH))
      throw new IllegalArgumentException(
          "sentinel (zeroes) hash at index [" + index + "]");
  }
  
  
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
  public final boolean equals(Object o) {
    return o instanceof HashFrontier other &&
        other.rowNumber() == rowNumber() &&
        other.frontierHash().equals(frontierHash()) &&
        other.levelRows().equals(levelRows());
  }
  
  
  
  @Override
  public int hashCode() {
    return frontierHash().hashCode();
  }
  
  
  
  
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
  public ByteBuffer frontierHash() {
    return levelFrontier[0].hash();
  }
  
  
  /**
   * Returns the hash of the row at the given level.
   * 
   * @param level &ge; 0 and &lt; {@linkplain #levelCount()}
   */
  public ByteBuffer levelHash(int level) {
    return levelFrontier[level].hash();
  }
  
  
  /**
   * Returns the row at the given level.
   * 
   * @param level &ge; 0 and &lt; {@linkplain #levelCount()}
   */
  public RowHash levelRow(int level) {
    return levelFrontier[level];
  }
  
  
  /** Returns the level rows ordered in <em>decreasing</em> row numbers. */
  public List<? extends RowHash> levelRows() {
    return Lists.asReadOnlyList(levelFrontier);
  }
  
  /**
   * Returns the next (full) row using the next row's input hash.
   * <p>
   * Note, an instance cannot produce it's own full row: it doesn't
   * keep track of its row's <em>input hashes</em>--only the rows'
   * <em>final hashes</em> (the hash of the entire row).
   * </p>
   * 
   * @param inputHash 32-byte value (SHA-256)
   * @return the next row (it's row number will be one greater than the
   *      this instance's {@linkplain #rowNumber()}
   * @see #nextFrontier(ByteBuffer)
   */
  public Row nextRow(ByteBuffer inputHash) {
    inputHash = sliceInput(inputHash);
    final long rn = rowNumber() + 1;
    
    final int skipCount = SkipLedger.skipCount(rn);
    
    assert skipCount != levelFrontier.length; // cuz there can't be 2 top levels
    
    // check to see if we need to expand (relatively rare)
    final boolean expand = skipCount > levelFrontier.length;
    
    assert !expand || this == SENTINEL || (skipCount == levelFrontier.length + 1 && rn == 2 * tail());
    
    var data = ByteBuffer.allocate(
        SldgConstants.HASH_WIDTH * (1 + skipCount));
    
    data.put(inputHash);
    
    int maxLevel = expand ? levelFrontier.length : skipCount;
    for (int index = 0; index < maxLevel; ++index)
      data.put(levelFrontier[index].hash());
    if (expand)
      data.put(DIGEST.sentinelHash());
    
    assert !data.hasRemaining();
    
    data.flip();
    return new SerialRow(rn, data);
  }
  
  
  

  /**
   * Calculates and returns the next frontier using the given input hash
   * for the next row.
   * 
   * @param inputHash 32-byte value (SHA-256)
   * 
   * @return resultant frontier after adding the row with the given {@code inputHash}
   * @see #nextRow(ByteBuffer)
   */
  public HashFrontier nextFrontier(ByteBuffer inputHash) {
    return nextFrontier(inputHash, null);
  }
  
  /**
   * Calculates and returns the next frontier using the given input hash
   * for the next row.
   * 
   * @param inputHash 32-byte value (SHA-256)
   * @param digest    optional SHA-256 work instance or {@code null}
   * 
   * @return resultant frontier after adding the row with the given {@code inputHash}
   * @see #nextRow(ByteBuffer)
   */
  public HashFrontier nextFrontier(ByteBuffer inputHash, MessageDigest digest) {
    
    final long rn = rowNumber() + 1;

    var prevHashes = Lists.functorList(
        SkipLedger.skipCount(rn),
        level -> level == levelFrontier.length ?
            DIGEST.sentinelHash() :
            levelFrontier[level].hash());

    var rowHash = SkipLedger.rowHash(rn, sliceInput(inputHash), prevHashes);







    // inputHash = sliceInput(inputHash);
    
    final int skipCount = SkipLedger.skipCount(rn);
    
    assert skipCount != levelFrontier.length; // cuz there can't be 2 top levels
    
    // check to see if we need to expand (relatively rare)
    final boolean expand = skipCount > levelFrontier.length;
    
    assert !expand || skipCount == levelFrontier.length + 1 && rn == 2 * tail();
    

    // compute the hash for the new row
    
    // if (digest == null)
    //   digest = DIGEST.newDigest();
    // else
    //   digest.reset();
    
    // update the digest with a sequence consisting of the new row's input hash
    // followed by each row hash referenced by that row, from its 0th level to
    // to (skipCount - 1)'th, in that order.
    // digest.update(inputHash);?
    
    // (exclusive):
    final int lastNonSentinelLevel = expand ? levelFrontier.length : skipCount;
    
    // for (int level = 0; level < lastNonSentinelLevel; ++level)
    //   digest.update(levelFrontier[level].hash());
    
    
    // the digest is prepared, excepting 1 corner case: when we expand
    // check for that, all while you're at it, allocated the next levels array..
    
    RowHash[] nextLevels;
    if (expand) {
      
      // when a new highest level row is created, it's highest level pointer
      // points to row zero (whose hash is just a string of zeros).
      // digest.update(DIGEST.sentinelHash());

      nextLevels = new RowHash[skipCount];
    
    } else
      
      nextLevels = new RowHash[levelFrontier.length];
    
    // the digest is ready.
    // HashedRow next = new HashedRow(rn, digest.digest());
    HashedRow next = new HashedRow(rn, rowHash);

    // copy the unaffected levels, back-to-front
    // (falls thru if *expand*-ed)
    for (int level = levelFrontier.length; level-- > skipCount; )
      nextLevels[level] = levelFrontier[level];
    
    // set the remaining levels to *next*, from skipCount - 1 to 0
    // (that's all folks!)
    for (int level = skipCount; level-- > 0; )
      nextLevels[level] = next;
    
    return new HashFrontier(nextLevels);
  }
  

  @Override
  public long rowNumber() {
    return levelFrontier[0].no
    ();
  }

  @Override
  public final long tail() {
    return levelFrontier[levelFrontier.length - 1].no();
  }

  
  @Override
  public List<Long> levelRowNumbers() {
    return new Lists.RandomAccessList<Long>() {

      @Override
      public Long get(int index) {
        return levelFrontier[index].no();
      }

      @Override
      public int size() {
        return levelFrontier.length;
      }
    };
  }
  
  
  @Override
  public String toString() {
    var hash = frontierHash().limit(4);
    return "%d:%s..".formatted(rowNumber(), IntegralStrings.toHex(hash));
  }


  @Override
  public int serialSize() {
    return levelFrontier.length * SldgConstants.HASH_WIDTH + 8;
  }


  /**
   * {@inheritDoc}
   * <p>
   * Also works on the {@linkplain #SENTINEL} instance.
   * </p>
   */
  @Override
  public ByteBuffer writeTo(ByteBuffer out) {
    out.putLong(rowNumber());
    for (var rowHash : levelFrontier)
      out.put(rowHash.hash());
    return out;
  }
  
  /**
   * Reads and returns an instance from its given serial representation.
   * 
   * @param in  positioned at the beginning and advanced on return. May contain 
   *            additional data (the format is self-delimiting).
   * @return the read in frontier, possibly the {@linkplain #SENTINEL} instance.
   * 
   * @see Serial
   */
  public static HashFrontier loadSerial(ByteBuffer in) {
    long rn = in.getLong();
    if (rn > 0) {
      int levels = levelCount(rn);
      ByteBuffer[] levelHashes = new ByteBuffer[levels];
      for (int index = 0; index < levels; ++index) {
        byte[] hash = new byte[SldgConstants.HASH_WIDTH];
        in.get(hash);
        levelHashes[index] = ByteBuffer.wrap(hash);
      }
      return new HashFrontier(rn, levelHashes);
    
    } else if (rn == 0) {
      return SENTINEL;
    } else {
      throw new ByteFormatException("read nonsense row number " + rn);
    }
  }

}










