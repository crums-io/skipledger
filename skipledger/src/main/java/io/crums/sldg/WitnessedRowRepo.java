/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.crums.model.CrumTrail;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.util.Lists;

/**
 * A repo of crumtrails for witnessed rows from the same ledger.
 * This was factored out of the {@linkplain HashLedger} interface.
 */
public interface WitnessedRowRepo extends AutoCloseable {
  
  
  /**
   * <p>Does not throw checked exceptions.</p>
   * 
   * {@inheritDoc}
   */
  void close();
  

  
  /**
   * Returns the number of rows that have crumtrails (represented as {@linkplain TrailedRow} instances).
   * 
   * @return &ge; 0
   * 
   * @see #getTrailByIndex(int)
   */
  int getTrailCount();
  
  
  /**
   * Returns the trailed row by index.
   * 
   * @param index &ge; 0 and &lt; {@linkplain #getTrailCount()}
   */
  TrailedRow getTrailByIndex(int index);
  

  
  /**
   * Adds the crumtrail in given witness record, if it's not already saved.
   * 
   * @param trailedRecord a <em>trailed</em> record (i.e. {@linkplain WitnessRecord#isTrailed()
   *        trailedRecord.isTrailed()} returns {@code true})
   *        
   * @return {@code true} if it wasn't already saved
   */
  boolean addTrail(WitnessRecord trailedRecord);
  

  /**
   * Returns the nearest witnessed row <em>on or after</em> the given row number, if any; {@code null} o.w.
   * A non-null return value is evidence how <em>old</em> the given row is (since it must have been created
   * before it was witnessed).
   * 
   * @param rowNumber &ge; 1
   */
  default TrailedRow nearestTrail(long rowNumber) {
    var witRns = getTrailedRowNumbers();
    int sindex = Collections.binarySearch(witRns, rowNumber);
    if (sindex < 0) {
      sindex = -1 - sindex;
      if (sindex == witRns.size())
        return null;
    }
    return getTrailByIndex(sindex);
  }
  
  default List<TrailedRow> getTrailedRows() {
    return new Lists.RandomAccessList<TrailedRow>() {
      final int size = getTrailCount();
      @Override
      public TrailedRow get(int index) {
        return getTrailByIndex(index);
      }
      @Override
      public int size() {
        return size;
      }
    };
  }
  


  /**
   * Returns the row numbers that have {@linkplain CrumTrail}s. I.e.
   * the rows that have been witnessed.
   * <p>
   * Note, the default implementation is inefficient. Override, if you care.
   * </p>
   * 
   * @return strictly ascending list of row numbers, possibly empty
   * 
   * @see #getTrailByIndex(int)
   */
  default List<Long> getTrailedRowNumbers() {
    return new Lists.RandomAccessList<Long>() {
      final int size = getTrailCount();
      @Override
      public Long get(int index) {
        return getTrailByIndex(index).rowNumber();
      }
      @Override
      public int size() {
        return size;
      }
    };
  }
  
  
  /**
   * Returns the last row number witnessed, if any; 0 (zero) otherwise. Here,
   * a witnessed row is just a ledger row with a {@linkplain CrumTrail} annotation
   * (represented as a {@linkplain TrailedRow}).
   */
  default long lastWitnessedRowNumber() {
    List<Long> witRns = getTrailedRowNumbers();
    return witRns.isEmpty() ? 0 : witRns.get(witRns.size() - 1);
  }
  
  
  default Optional<TrailedRow> lastTrailedRow() {
    int count = getTrailCount();
    return count == 0 ? Optional.empty() : Optional.of(getTrailByIndex(count - 1));
  }

  
  /**
   * Returns the nearest witnessed rows <em>on or after</em> the given row numbers.
   * 
   * @param rowNumbers non-null bag of row numbers (each &ge; 1)
   * @return (possibly empty) list of trailed row numbers in ascending order with no duplicates.
   */
  default List<TrailedRow> nearestTrails(Collection<Long> rowNumbers) {
    
    if (rowNumbers.isEmpty())
      return List.of();
    
    var rns = Lists.sortRemoveDups(rowNumbers);
    final var witRns = getTrailedRowNumbers();

    var trails = new ArrayList<TrailedRow>();
    while (!rns.isEmpty()) {
      long targetRn = rns.get(0);
      
      int tIndex = Collections.binarySearch(witRns, targetRn);
      
      if (tIndex < 0) {
        tIndex = -1 - tIndex;
        if (tIndex == witRns.size())
          break;
        int rIndex = Collections.binarySearch(rns, witRns.get(tIndex));
        if (rIndex < 0)
          rIndex = -1 - rIndex;
        else
          ++rIndex;
        rns = rns.subList(rIndex, rns.size());
      } else
        rns = rns.subList(1, rns.size());
      
      var trail = getTrailByIndex(tIndex);
      trails.add(trail);
      
    } // while (
    
    return Collections.unmodifiableList(trails);
  }
  

}
