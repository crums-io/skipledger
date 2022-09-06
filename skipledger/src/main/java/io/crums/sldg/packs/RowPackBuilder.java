/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.packs;


import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;

import io.crums.io.Serial;
import io.crums.io.buffer.BufferUtils;
import io.crums.sldg.HashConflictException;
import io.crums.sldg.Path;
import io.crums.sldg.Row;
import io.crums.sldg.SerialRow;
import io.crums.sldg.SkipLedger;
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
 * <ol>
 * <li>An empty instance's rows may be <em>initialized</em> either with an existing
 * {@linkplain RowPack row-pack} or a ledger {@linkplain Path path}. This initialization
 * establishes the highest row number in the pack.</li>
 * <li>A row may only ever be <em>added</em> if it is already referenced by an existing row
 * in the bag.</li>
 * </ol>
 * <p>
 * Note rule (1) involves a degree of trust. Since the hash of the last row in the ledger
 * represents the state of the entire ledger, extending a bag using a higher numbered path
 * necessarily involves trusting the source of the information. We punt on how this done,
 * for now.
 * </p><p>
 * Rule (1) is represenented by the {@linkplain #init(Path)} and {@linkplain #init(RowPack)} methods.<br/>
 * Rule (2) is enforced by the {@linkplain #add(Row)} method.
 * </p>
 * 
 * @see RowPack {@code RowPack} for the serialization format
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
  
  
  private final HashMap<Long, SerialRow> cache = new HashMap<>();
  
  



  @Override
  public synchronized ByteBuffer writeTo(ByteBuffer out) {
    
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
    if (size == 0)
      return 4;
    int hashes = size + refHashes.size();
    return
        4 +
        size * 8 +
        hashes * SldgConstants.HASH_WIDTH;
  }



  
  
  
  
  /**
   * Adds all the rows in the given pack. If this instance {@linkplain #isEmpty() is empty}, then all
   * the rows in the bag are added. Otherwise, only <em>linked information</em> is
   * added. I.e. this method only allows adding detail to already known information.
   * 
   * @return the number of full rows added
   * 
   * @throws HashConflictException
   *         if a hash in {@code bag} conflicts with existing hashes.
   */
  public synchronized int addAll(RowPack pack) throws HashConflictException {
    
    if (isEmpty())
      return init(pack);

    // the bag already contains other rows..
    // work this incrementally, starting from the highest row number
    // and working our way down to the lowest

    int count = 0;
    
    for (var rns = Lists.reverse(pack.getFullRowNumbers()).iterator(); rns.hasNext(); ) {
      long rn = rns.next();
      if (add(pack.getRow(rn)))
        ++count;
    }
    
    return count;
  }
  
  
  /**
   * Initializes the instance with the given {@code pack}.
   * 
   * @return the number of full rows added
   */
  public synchronized int init(RowPack pack) {
    checkInit();
    List<Long> fullRns = pack.getFullRowNumbers();
    for (long rn : fullRns)
      addSansCheck(pack.getRow(rn));
    return fullRns.size();
  }
  
  
  /**
   * Initializes the instance with the given path.
   * 
   * @param path usually a state path
   * 
   * @return the number of full rows added (i.e. {@code path.rows().size()}
   */
  public synchronized int init(Path path) throws IllegalStateException {
    checkInit();
    path.rows().forEach(r -> addSansCheck(r));
    return path.rows().size();
  }
  
  private void checkInit() {
    if (!isEmpty())
      throw new IllegalStateException("attempt to initialize while not empty");
  }
  
  
  
  
  
  
  

  
  
  /**
   * Adds the rows in the given {@code path} that can be linked from the highest
   * row in this instance.
   * 
   * @param path a path whose lowest row number is &le; {@linkplain #hi()}
   * 
   * @return the number of rows from the given {@code path} added or already existence
   *         in this instance (possibly zero)
   */
  public synchronized int addPath(Path path) {
    final long hi = hi();
    if (path.loRowNumber() > hi)
      throw new IllegalArgumentException(
          "lowest row number in path (" + path.loRowNumber() + ") > highest existing row number " + hi);
    
    int count = 0;
    List<Row> rows = path.rows();
    // add the rows in reverse
    for (int index = rows.size(); index-- > 0; ) {
      Row row = rows.get(index);
      if (row.rowNumber() > hi)
        continue;
      if (add(row))
        ++count;
    }
    return count;
  }
  
  
  /**
   * <p>
   * Adds the given row if it's not already in the bag, <em>and</em> if it's linked from an
   * exisiting (higher numbered) row in the bag.
   * </p><p>
   * With one exception this method is <em>fail-fast.</em> The single exception
   * is on an assertion-panic, which may leave the instance in an inconsistent state.</p>
   * 
   * @param row with row-number &le; {@linkplain #hi()}
   * 
   * @return {@code true} <b>iff</b> on return the given (full) row exists in the morsel
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
    // return yes (got it already).. but before returning, check their hashes
    // and nag if they confict
    
    if (inputHashes.get(rn) != null) {
      if (row.hash().equals(rowHash(rn)))
        return true;
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
    
    // do we need to cache.clear() (?)
    // can reason it's unnecessary.. so won't
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
  

  @Override
  public synchronized long hi() {
    return inputHashes.isEmpty() ? 0L : inputHashes.lastKey();
  }
  

  @Override
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
  public synchronized Row getRow(long rowNumber) {
    Long rn = rowNumber;
    SerialRow cachedRow = cache.get(rn);
    if (cachedRow == null) {
      Row row = super.getRow(rowNumber);
      // this cascades, but note that entering a synchronized
      // block when the monitor is already-held is cheap
      cachedRow = new SerialRow(row);
      cache.put(rn, cachedRow);
    }
    
    return cachedRow;
  }

  

  @Override
  public synchronized List<Long> getFullRowNumbers() {
    return Lists.readOnlyCopy(inputHashes.keySet());
  }
  
  
  @Override
  public synchronized boolean hasFullRow(long rowNumber) {
    return inputHashes.containsKey(rowNumber);
  }
  
  
  
  
  
  



  public static RowPack createBag(SkipLedger ledger, List<Long> rowNumbers) {
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
  public static RowPack createBag(SkipLedger ledger, List<Long> rowNumbers, boolean copy) {
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
      SortedSet<Long> refs = SkipLedger.refOnlyCoverage(rowNumbers).tailSet(1L);
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









