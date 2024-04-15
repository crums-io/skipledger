/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

import io.crums.util.Lists;
import io.crums.util.Sets;

/**
 * 
 */
public class PathPackBuilder implements PathBag {

  protected final Object lock = new Object();

  /**
   * Input hashes of full rows.
   */
  private final TreeMap<Long, ByteBuffer> inputHashes = new TreeMap<>();
  /**
   * Referenced-only rows. Invariant: does not contain hashes of
   * rows with input hashes.
   */
  private final TreeMap<Long, ByteBuffer> refHashes = new TreeMap<>();
  /**
   * Computed hashes of input rows. Invariant: {@code inputHashes.size() == refHashes.size()}
   */
  private final HashMap<Long, ByteBuffer> memoHashes = new HashMap<>();
  
  private boolean corrupted;
  
  /**
   * 
   */
  public PathPackBuilder() {
  }
  
  
  public int addRow(Row row) {
    
    final Long rn = row.rowNumber();

    synchronized (lock) {
      
      // check if it already exists, bail if it does
      {
        ByteBuffer existingMH = memoHashes.get(rn);
        if (existingMH != null) {
          // already here.. check the row's value
          // and let the user know if what they're
          // adding conflicts with what's already here
          if (existingMH.equals(row.hash()))
            return 0;
          else
            throw new HashConflictException(
                "attempt to overwrite conflicting row [" + rn + "]");
        }
      }
      
      final var fullRns = getFullRowNumbers();
      final int insertIndex;
      {
        int sIndex = Collections.binarySearch(fullRns, rn);
        assert sIndex < 0;
        insertIndex = -1 - sIndex;
      }
      
      
      final int levels = SkipLedger.skipCount(rn);
      
      // if we're adding to the end of the path
      if (insertIndex == fullRns.size()) {

        // if it's the first row, just add it
        if (insertIndex == 0)
          return addSansLinkCheck(row);
        
        // o.w. check the row is linked to the last
        // row in the pack..
        long hiRn = hi();
        if (!SkipLedger.rowsLinked(hiRn, rn))
          throw new IllegalArgumentException(
              "row [" + rn + "] does not reference hi row [" + hiRn + "]");
        
        if (!memoHashes.get(hiRn).equals(row.hash(hiRn)))
          throw new HashConflictException(
              "hash pointer: [" + rn + "] -> [" + hiRn + "] (current hi)");
        
        // check the row's hash pointers to rows below hiRn (if any) match
        final int hiRnSkipCount = SkipLedger.skipCount(hiRn);
        for (int level = levels; level-- > hiRnSkipCount; ) {
          long refNo = row.prevRowNumber(level);
          var existingRef = findHash(refNo);  // (can't be null)
          if (!existingRef.equals(row.prevHash(level)))
            throw new HashConflictException(
              "hash pointer: [" + rn + "] -> [" + refNo + "]");
        }
        
        // thumbs up
        return addSansLinkCheck(row);
      }
      
      // the pack is not empty, and we're NOT adding to
      // the end of path..
      
      // determine if the row no. is linked from above
      long aboveRn = fullRns.get(insertIndex);
      var existingRef = refHashes.get(rn);
      
      if (existingRef == null)
        throw new IllegalArgumentException(
            "row [" + rn + "] not referenced from above; nearest [" + aboveRn + "]");

      var safeRow = toSafeRow(row);
      if (!existingRef.equals(safeRow.hash()))
        throw new HashConflictException(
            "hash pointer: [" + aboveRn + "] -> [" + rn + "] (argument)");
      
      // checks out..
      // (leaning on hashing, we needn't check any other common hash ptrs)
      
      // finally, verify we're linked to below
      if (insertIndex > 1) {
        long belowRn = fullRns.get(insertIndex - 1);
        if (!SkipLedger.rowsLinked(belowRn, rn))
          throw new IllegalArgumentException(
              "row [" + rn + "] (argument) does not reference [" + belowRn + "]");
        // good.. nothing more to check
        // (no, we don't need to check common lower h-ptr values.. those were
        // implicitly verified when row's hash checked out.)
      }
      
      return addSansLinkCheck(safeRow);
    } // synchronized
  }
  
  
  
  public int addPath(Path path) {
    
    // PUNT: some of this logic might belong in the
    // Path class itself
    
    synchronized (lock) {
      
      int count = 0;
      if (memoHashes.isEmpty()) {
        // then the instance is empty
        var rows = path.rows();
        // add the rows back to front (more efficient)
        // and be done with it.
        for (int index = rows.size(); index-- > 0; )
          count += addSansLinkCheck(rows.get(index));
        
        return count;
      }
      
      final var pathRns = path.rowNumbers();
      if (pathRns.size() == 1)
        return addRow(path.first());
      
      var fullRns = getFullRowNumbers();

      // assemble the *new*, full row no.s to be added
      var unknownRns = new TreeSet<Long>(pathRns);
      unknownRns.removeAll(fullRns);
      
      {
        var stitchRns =
            unknownRns.isEmpty() ?
                Sets.sortedSetView(fullRns) : // (paths rns stitched by def.)
                  new TreeSet<>(
                      SkipLedger.stitchCollection(
                          Lists.concat(fullRns, pathRns)));
        
        // since both pathRns and fullRns are already stitched,
        // the 2 paths (one represented by *this* instance) are
        // stitchable only if stitchRns is the union of fullRns
        // and unknownRns
        
        if (stitchRns.size() - fullRns.size() != unknownRns.size()) {
          stitchRns.removeAll(unknownRns);
          throw new IllegalArgumentException(
              "cannot stitch path; missing rows: " + stitchRns);
        }
      }
      // good, the 2 paths are stitchable.
      
      var hi = fullRns.get(fullRns.size() - 1);
      
      // the highest row no. in the 2 paths
      // whose hashes must agree is either the
      // highest row no. in the new path
      // or the highest row no. in "this" path
      final long highestCommonRn =
          unknownRns.tailSet(hi).isEmpty() ? path.hiRowNumber() : hi;
      
      var hiHashExpect = findHash(highestCommonRn); // not null; NPE, o.w.
      var hiHashActual = path.getRowHash(highestCommonRn);
      
      if (!hiHashExpect.equals(hiHashActual))
        throw new HashConflictException("row [" + highestCommonRn + "]");
      
      // nothing more to check
      // (via the magic of 1-way hashing)
      
      // add back to front, again, cause it's more efficient
      for (var iRn = unknownRns.descendingIterator(); iRn.hasNext(); ) {
        var row = path.getRowByNumber(iRn.next());
        count += addSansLinkCheck(row);
      }
      
      return count;
    } // synchronized
  }
  
  
  
  public int addPack(PathPack pack) {
    if (pack.isEmpty())
      return 0;
    
    return addPath(pack.path());
  }
  
  
  private SerialRow toSafeRow(Row row) {
    return row instanceof SerialRow safe ? safe : new SerialRow(row);
  }

  private int addSansLinkCheck(Row row) {
    return addSansLinkCheck(toSafeRow(row));
  }
  
  /**
   * Adds the row without checking it links into the path.
   * But <em>does</em> check for consistency with existing data.
   * Will not overwrite data without throwing a wrench.
   * I.e. fails, but not fail fast.
   * 
   * @return no. of ref- and input-hashes added
   * 
   * @throws AssertionError hash conflicts here are assertion errors:
   *                        (caller is expected to check for hash conflicts
   *                        in advance)
   */
  private int addSansLinkCheck(SerialRow safeRow) {
    final long rn = safeRow.rowNumber();
    final int levels = safeRow.prevLevels();
    
    final var rowHash = safeRow.hash();
    
    // sanity check it's not already in here..
    {
      var memoHash = this.memoHashes.get(rn);
      if (memoHash != null) {
        if (memoHash.equals(rowHash))
          return 0;
        // then there's a bug
        throw new AssertionError(
            "memo-ed hash at [" + rn + "] conflicts with argument: " + safeRow);
      }
    }
    
    // write the row's hash pointers, keeping
    // a tally of how many we write..
    int tally = 0;
    
    for (int level = levels; level-- > 0; ) {
      long ptrRn = rn - (1L << level);
      if (ptrRn == 0)
        continue; // the sentinel hash is never written
      // find the existing row hash for ptrRn
      // (either memo-ed or ref-only)
      var existingRef = findHash(ptrRn);
      var ptrHash = safeRow.prevHash(level);
      // write it as a reference, if we haven't seen it
      if (existingRef == null) {
        this.refHashes.put(ptrRn, ptrHash);
        ++tally;
      // otherwise don't do anything
      // but do verify..
      } else if (!existingRef.equals(ptrHash)) {
        
        // well we've got a bug..
        if (level < levels - 1)
          this.corrupted = true;
        
        throw new AssertionError("[" + rn + ":" + level + "]");
      }
    }

    ++tally;
    var prev = this.inputHashes.put(rn, safeRow.inputHash());
    assert prev == null;
    
    this.memoHashes.put(rn, rowHash);
    this.refHashes.remove(rn);
    
    return tally;
  }
  
  
  /** Do not modify return value! */
  private ByteBuffer findHash(long rowNo) {
    final Long rn = rowNo;
    var hash = refHashes.get(rn);
    if (hash == null)
      hash = memoHashes.get(rn);
    return hash;
  }
  
  
  @Override
  public Path path() {
    final List<Long> snapshotRns = getFullRowNumbers();
    return new Path(Lists.map(snapshotRns, rn -> getRow(rn)), null);
  }
  
  

  @Override
  public ByteBuffer inputHash(long rowNumber) {
    synchronized (lock) {
      ByteBuffer input = inputHashes.get(rowNumber);
      if (input == null)
        throw new IllegalArgumentException("no info for row [" + rowNumber + "]");
      return input.asReadOnlyBuffer();
    }
  }
  
  
  @Override
  public ByteBuffer rowHash(long rowNumber) {
    if (rowNumber <= 0) {
      if (rowNumber == 0)
        return SldgConstants.DIGEST.sentinelHash();
      throw new IllegalArgumentException("negative row number: " + rowNumber);
    }

    synchronized (lock) {
      ByteBuffer hash = refHashes.get(rowNumber);
      if (hash == null)
        hash = memoHashes.get(rowNumber);
      
      if (hash == null)
        throw new IllegalArgumentException("no info for row [" + rowNumber + "]");
      
      return hash.asReadOnlyBuffer();
    }
  }
  


  @Override
  public Row getRow(long rowNumber) {
    Long rn = rowNumber;
    synchronized (lock) {
      var rowHash = memoHashes.get(rowNumber);
      if (rowHash == null)
        throw new IllegalArgumentException("no info for row [" + rn + "]");
      
      return new MemoRow(rowNumber, rowHash.slice());
    }
  }

  

  /**
   * A read-only snapshot.
   * {@inheritDoc}
   */
  @Override
  public List<Long> getFullRowNumbers() {
    synchronized (lock) {
      return Lists.readOnlyCopy(inputHashes.keySet());
    }
  }
  
  
  @Override
  public boolean hasFullRow(long rowNumber) {
    synchronized (lock) {
      return inputHashes.containsKey(rowNumber);
    }
  }
  
  
  



  @Override
  public ByteBuffer refOnlyHash(long rowNumber) {
    synchronized (lock) {
      ByteBuffer refHash = refHashes.get(rowNumber);
      return refHash == null ? null : refHash.asReadOnlyBuffer();
    }
  }
  
  
  private class MemoRow extends BaggedRow {
    
    private final ByteBuffer hash;

    MemoRow(long rowNumber, ByteBuffer hash) {
      super(rowNumber, PathPackBuilder.this);
      this.hash = hash;
    }
    

    @Override
    public ByteBuffer hash() {
      return hash.slice();
    }
    
  }
  

}
