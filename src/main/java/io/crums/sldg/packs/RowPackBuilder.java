/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.Ledger;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SldgConstants;
import io.crums.util.Lists;

/**
 * Builds a {@linkplain RowPack}.
 * 
 * <h2>Rules of the Game</h2>
 * <p>
 * Every row bag must <em>provably maintain rows from the same ledger.</em> The following
 * rules are designed to ensure this guarantee.
 * </p>
 * <p><ol>
 * <li>A row may only ever be <em>added</em> when it is already referenced by an existing row
 * in the bag.</li>
 * <li>A path may <em>extend</em> a bag (by having its rows saved in the bag) only if
 *    <ul>
 *    <li>the path's highest row number is greater than any in the bag, and</li>
 *    <li>the path intersects an existing row (full or referenced-only) in the bag.</li>
 *    </ul>
 * </li>
 * <li>Every row known to the bag (whether referenced-only or in full) is checked against
 * other known rows for consistency.</li>
 * </ol></p><p>
 * Note rule (2) involves a degree of trust. Since the hash of the last row in the ledger
 * represents the state of the entire ledger, extending a bag using a higher numbered path
 * necessarily involves trusting the source of the information. We punt on how this done,
 * for now.
 * </p><p>
 * Rules (1) and (2) are represented by the {@linkplain #add(Row)} and {@linkplain
 * #extend(Path)} methods, resp.
 * 
 * </p>
 */
public class RowPackBuilder extends RecurseHashRowPack implements Serial {
  
  /**
   * Full rows.
   */
  private final TreeMap<Long, ByteBuffer> inputHashes = new TreeMap<>();
  
  /**
   * Referenced-only rows.
   */
  private final TreeMap<Long, ByteBuffer> refHashes = new TreeMap<>();
  
  





  @Override
  public synchronized ByteBuffer writeTo(ByteBuffer out) {
    // Notes here are from RowPack documentation
    // (Why not implemnent writeTo in RowPack? Because
    // most of the time, I expect, we'll be manipulating
    // morsels (which this class is an implementation part of)
    // and it will be 
    
    int size = size();
    
    // I_COUNT
    out.putInt(size);
    if (size == 0)
      return out;
    
    // FULL_RNS
    for (Long rn : getFullRowNumbers())
      out.putLong(rn);
    
    // R_TBL
    for (ByteBuffer hash : refHashes.values())
      out.put(hash.duplicate());
    
    // I_TBL
    for (ByteBuffer hash : inputHashes.values())
      out.put(hash.duplicate());
    
    return out;
  }


  @Override
  public synchronized int serialSize() {
    int size = inputHashes.size();
    int hashes = size + refHashes.size();
    return
        4 +
        size * 8 +
        hashes * SldgConstants.HASH_WIDTH;
  }



  
  
  
  
  /**
   * Adds all the rows in the given bag. If this instance {@linkplain #isEmpty() is empty}, then all
   * the rows in the bag are added. Otherwise, only <em>linked information</em> is
   * added. I.e. this method only allows adding detail to already known information.
   * 
   * @return the number of full rows added
   * 
   * @throws HashConflictException
   *         if a hash in {@code bag} conflicts with existing hashes.
   */
  public synchronized int addAll(RowPack bag) throws HashConflictException {
    int count = 0;

    // no checks necessary when starting with an empty instance
    if (isEmpty()) {
      for (Long rn : bag.getFullRowNumbers()) {
        Row row = bag.getRow(rn);
        addSansCheck(row);
        ++count;
      }
    
    } else {
      
      // the bag already contains other rows..
      // work this incrementally, starting from the lowest row number
      // and work higher.
      
      // we might have to do this in multiple passes:
      // for instance, a lower may first be ineligible for adding
      // --until a higher row number that references it is added
      // to the bag.
      
      // so we keep at it, until we detect we're not making any progress
      
      TreeSet<Long> unprocessedRns = new TreeSet<>(bag.getFullRowNumbers());

      int benchedSize;
      
      do {
        benchedSize = unprocessedRns.size();

        Iterator<Long> rowit = unprocessedRns.iterator();
        while (rowit.hasNext()) {
          Long rn = rowit.next();
          Row row = bag.getRow(rn);
          if (add(row)) {
            ++count;
            rowit.remove();
          }
        }
        
        // as long as unprocessedRns is getting smaller we're making progress..
      } while (benchedSize > unprocessedRns.size());
      
    } // else
    
    return count;
  }
  
  
  
  
  
  
  /**
   * Extends this bag to a higher row number by adding the given {@code path} with
   * a higher row number. 
   * <p>
   * Two requirements must be met by the {@code path} argument to extend the bag:
   * </p>
   * <ol>
   * <li>{@code path}.{@linkplain Path#hiRowNumber() hiRowNumber()} must be greater than
   * {@linkplain #hi()}</li>
   * <li>{@code path} must intersect or reference an existing full row (as advertised by
   * {@linkplain #getFullRowNumbers()}).</li>
   * </ol>
   * 
   * @return {@code true} <b>iff</b> one or more rows were added
   * 
   * @throws HashConflictException
   *         if a hash in {@code path} conflicts with existing hashes. If this happens, then
   *         the given path doesn't come from this bag's ledger.
   */
  public synchronized boolean extend(Path path) throws HashConflictException {
    
    if (path.hiRowNumber() < hi())
      return false;;
    
    
    // we do this in 2 passes..
    
    // first pass we check to see if we can even do this
    boolean linked = false;
    
    for (Long coveredRn : path.rowNumbersCovered().tailSet(1L))
      linked |= pathLinkedAt(path, coveredRn);
    
    if (!linked)
      return false;
    
    // 1st pass checks out
    // 2nd pass: do it, replace / add rows
    
    for (Row row : path.rows())
      addSansCheck(row);
    
    return true;
  }
  

  
  
  /**
   * 
   * @param path
   * @param rn a member of {@code path}.{@linkplain Path#rowNumbersCovered() rowNumbersCovered()}
   * @return
   */
  private boolean pathLinkedAt(Path path, Long rn) {
    boolean linked = false;
    
    if (isKnownRow(rn)) {
      if (rowHash(rn).equals(path.getRowHash(rn)))
          linked = true;
      else
        throw new HashConflictException("at row[" + rn + "] in path " + path);
    }
    
    return linked;
  }
  
  
  
  
  private  boolean isKnownRow(Long rn) {
    return refHashes.containsKey(rn) || inputHashes.containsKey(rn);
  }
  
  
  /**
   * <p>
   * Adds the given row if it's not already in the bag, <em>and</em> if it's linked from an
   * exisiting (higher numbered) row in the bag.
   * </p><p>
   * With one exception this method is <em>fail-fast.</em> The single exception
   * is on an assertion-panic, which may leave the instance in an inconsistent state.</p>
   * 
   * @return {@code true} <b>iff</b> the row was added
   * 
   * @throws HashConflictException
   *         if the {@code row}'s hash conflicts with existing hashes. If this happens, then
   *         the given row doesn't come from this bag's ledger.
   */
  public synchronized boolean add(Row row) throws HashConflictException {
    final long rn = row.rowNumber();
    if (rn > hi())
      throw new IllegalArgumentException(
          "attempt to extend bag with row [" + rn + "]; hi is " + hi());
    
    
    // check the input hashes first; if we get a hit here
    // return no.. but before returning, check their hashes
    // and nag if they confict
    
    if (inputHashes.get(rn) != null) {
      if (row.hash().equals(rowHash(rn)))
        return false;
      throw new HashConflictException("attempt to add row [" + rn + "] with conflicting hash");
    }

    // not a input hash; if the rn is not referenced, boot it
    ByteBuffer rHash = refHashes.get(rn);
    if (rHash == null)
      return false;
    
    // if the row hash is not the same as our copy, cough
    if (!row.hash().equals(rHash))
      throw new HashConflictException("attempt to add row [" + rn + "] with conflicting hash");
    
    
    // ok, so we're adding this row..
    // (we don't consider what row links to this row..
    // don't have to if we maintain the invariants for
    // this.inputHashes and this.refHashes )
    
    addSansCheck(row);
    
    return true;
  }
  
  
  
  private void addSansCheck(Row row) {
    
    Long rn = row.rowNumber();
    
    

    // populate hash pointers as necessary
    for (int level = row.prevLevels(); level-- > 0; ) {
      
      Long ptrRn = rn - (1L << level);
      if (ptrRn == 0)
        continue; // the sentinel hash is never written
      
      ByteBuffer ptrHash = row.prevHash(level);
      
      ByteBuffer existingRef = findHash(ptrRn);
      if (existingRef == null) {
        refHashes.put(ptrRn, ptrHash);
      
      // otherwise don't do anything
      // .. but verify general sanity about tamper-proof-ness
      } else if (!existingRef.equals(ptrHash)) {
        
        // Given theory, this shouldn't happen with a strong hash function
        // if we get here, there's a bug
        // TODO
        
        System.err.println("- - PLEASE REPORT THIS BUG! - -");
        System.err.println();
        Thread.dumpStack();
        System.err.println();
        System.err.println("on attempt to add row: " + row);
        System.err.println("referenced row: " + ptrRn);
        System.err.println("level: " + level);
        System.err.println();
        System.err.println("- - - - - - - - - - - - - - - -");
        
        throw new AssertionError("Details dumped on std error: Please report!");
      }
    } // for
    
    
    inputHashes.put(rn, row.inputHash());
    refHashes.remove(rn);
  }
  
  
  
  /**
   * Do not modify non-null return value!
   */
  private ByteBuffer findHash(Long rowNumber) {
    return
        inputHashes.get(rowNumber) == null ?
            refHashes.get(rowNumber) : rowHash(rowNumber);
  }
  
  
  /**
   * Returns the number of full rows in the bag.
   * 
   * @return {@linkplain #getFullRowNumbers()}{@code .size()}
   */
  public synchronized int size() {
    return inputHashes.size();
  }
  
  
  /**
   * Determines whether the bag is empty.
   * 
   * @return {@linkplain #getFullRowNumbers()}{@code .size() == 0}
   */
  public synchronized boolean isEmpty() {
    return inputHashes.isEmpty();
  }
  
  
  /**
   * The highest (full) row number in the bag, or 0 if empty.
   *
   * @return &ge; {@linkplain #lo()}
   */
  public synchronized long hi() {
    return inputHashes.isEmpty() ? 0L : inputHashes.lastKey();
  }
  
  
  /**
   * The lowest (full) row number in the bag, or 0 if empty.
   * 
   * @return &ge; 0
   */
  public synchronized long lo() {
    return inputHashes.isEmpty() ? 0L : inputHashes.firstKey();
  }


  @Override
  protected ByteBuffer refOnlyHash(long rowNumber) {
    ByteBuffer refHash = refHashes.get(rowNumber);
    return refHash == null ? null : refHash.asReadOnlyBuffer();
  }



  /**
   * <p>Overriden only to synchronize.</p>
   * {@inheritDoc}
   */
  @Override
  public synchronized ByteBuffer rowHash(long rowNumber) {
    return super.rowHash(rowNumber);
  }


  @Override
  public synchronized ByteBuffer inputHash(long rowNumber) {
    ByteBuffer input = inputHashes.get(rowNumber);
    if (input == null)
      throw new IllegalArgumentException("no info for row with given number: " + rowNumber);
    return input.asReadOnlyBuffer();
  }

  

  @Override
  public List<Long> getFullRowNumbers() {
    return Lists.readOnlyCopy(inputHashes.keySet());
  }
  
  
  
  
  
  
  
  
  
  



  public static RowPack createBag(Ledger ledger, List<Long> rowNumbers) {
    return createBag(ledger, rowNumbers, true);
  }
  
  
  /**
   * Creates and returns a new bag.
   * 
   * @param ledger the source
   * @param rowNumbers the row numbers for which full row information will be available
   * @param copy determines if {@code rowNumbers} is defensively copied
   * @return
   */
  public static RowPack createBag(Ledger ledger, List<Long> rowNumbers, boolean copy) {
    if (Objects.requireNonNull(rowNumbers, "null rowNumbers").isEmpty())
      throw new IllegalArgumentException("empty rowNumbers");
    
    Objects.requireNonNull(ledger, "null ledger");
    
    if (copy)
      rowNumbers = Lists.readOnlyCopy(rowNumbers);
    
    final long lastRowNum = ledger.size();
    final int count = rowNumbers.size();
    
    final long lastRn = rowNumbers.get(count - 1);
    
    // some quick init bounds checks..
    if (lastRowNum < lastRn)
      throw new IllegalArgumentException(
          "last rowNumber out-of-bounds: " + lastRn + " > ledger size "+ lastRowNum);
    
    final long firstRn = rowNumbers.get(0);
    
    if (firstRn < 1)
      throw new IllegalArgumentException("first rowNumber out-of-bounds: " + firstRn);
    
    
    ByteBuffer inputs = ByteBuffer.allocate(count * SldgConstants.HASH_WIDTH);
    long prevRn = 0;
    for (int index = 0; index < count; ++index) {
      long rn = rowNumbers.get(index);
      if (rn <= prevRn)
        throw new IllegalArgumentException(
            "out-of-sequence rowNumber " + rn + " at index " + index + ": " + rowNumbers);
      prevRn = rn;
      ByteBuffer e = ledger.getRow(rn).inputHash();
      inputs.put(e);
    }
    
    assert !inputs.hasRemaining();
    
    inputs.flip();

    ByteBuffer hashes;
    {
      SortedSet<Long> refs = Ledger.refOnlyCoverage(rowNumbers).tailSet(1L);
      int hashCount = refs.size();
      
      if (hashCount == 0) {
        hashes = BufferUtils.NULL_BUFFER;
      } else {
        hashes = ByteBuffer.allocate(hashCount * SldgConstants.HASH_WIDTH);
        for (long refRn : refs) {
          ByteBuffer rowHash = ledger.rowHash(refRn);
          hashes.put(rowHash);
        }
        assert !hashes.hasRemaining();
        hashes.flip();
      }
    }
    
    return new RowPack(rowNumbers, hashes, inputs);
  }



  

}









