/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

import io.crums.sldg.bags.MorselBag;
import io.crums.sldg.entry.EntryInfo;
import io.crums.util.Lists;
import io.crums.util.Sets;
import io.crums.util.Tuple;

/**
 * A bundle of paths, entries (row source data), and other data from a same ledger. A morsel
 * usually contains a small subset of the data from a much larger ledger. Regardless it provides
 * the same tamper-proofness guarantees the ledger it came from itself provides.
 */
public class Morsel {
  
  /**
   * Ranks the latest {@linkplain PathInfo}s first. Here <em>latest</em> means
   * the path with the highest row number. I.e. this ranks paths by their
   * {@linkplain PathInfo#hi() hi} row number, but in reverse. If two paths share
   * the same hi row numbers, then they are ordered by the <em>sizes</em> of their
   * respective {@linkplain PathInfo#declaration() declaration}s (the more more
   * concise declaration comes first). Note that {@linkplain PathInfo#lo()} does
   * not figure in this ordering.
   */
  public final static Comparator<PathInfo> LEADERBOARD =
      new Comparator<PathInfo>() {

        @Override
        public int compare(PathInfo a, PathInfo b) {
          long diff = a.hi() - b.hi();  // reverse order (it's a leaderboard)
          if (diff == 0) {
            // the smaller declaration comes first
            diff = a.declaration().size() - b.declaration().size();
          }
          return diff > 0 ? 1 : (diff == 0 ? 0 : -1);
        }
      };
  
  
  
  private final MorselBag bag;
  
  
  public Morsel(MorselBag bag) {
    this.bag = Objects.requireNonNull(bag, "null bag");
  }
  
  
  
  public List<EntryInfo> availableEntries() {
    return bag.availableEntries();
  }
  
  
  
  
  
  public SortedSet<Long> knownRowSet() {
    return Sets.sortedSetView(bag.getFullRowNumbers());
  }
  
  
  
  
  public Optional<Path> findPath(List<Long> rowNumbers) {
    {
      PathInfo info = new PathInfo(rowNumbers); // validates also
      int index = declaredPaths().indexOf(info);
      if (index != -1) {
        return Optional.of(getDeclaredPath(index));
      }
    }
    
    SortedSet<Long> known = knownRowSet();
    assert !known.isEmpty();
    
    Optional<List<Long>> stitch = Ledger.stitchPath(known, rowNumbers);
    if (stitch.isEmpty())
      return Optional.empty();
    
    return Optional.of(createPath(stitch.get()));
  }
  
  
  
  public List<PathInfo> declaredPaths() {
    return bag.declaredPaths();
  }
  
  
  /**
   * Returns the {@linkplain PathInfo#isState() state path}s as a leader board.
   * 
   * @return
   */
  public List<PathInfo> statePaths() {
    List<PathInfo> all = bag.declaredPaths();
    if (all.isEmpty())
      return all;
    List<PathInfo> states = new ArrayList<>(all.size());
    all.stream().filter(p -> p.isState()).forEach(p -> states.add(p));
    
    switch (states.size()) {
    case 0:   return Collections.emptyList();
    case 1:   return Collections.singletonList(states.get(0));
    }
    
    Collections.sort(states, LEADERBOARD);
    
    return Collections.unmodifiableList(states);
  }
  
  
  public Path getDeclaredPath(int index) {
    return createPath(bag.declaredPaths().get(index).rowNumbers());
  }


  
  
  private Path createPath(final List<Long> rowNumbers) {
    
    Row[] rows = new Row[rowNumbers.size()];
    
    final List<Tuple<Long,Long>> beacons = bag.beaconRows();
    int bcIndex = 0;
    long nextBeaconRn = beacons.isEmpty() ? Long.MAX_VALUE : beacons.get(0).a;
    
    
    ArrayList<Tuple<Long,Long>> outBeacons = new ArrayList<>(8);
    
    for (int index = 0; index < rows.length; ++index) {
      
      final long rn = rowNumbers.get(index);
      
      rows[index] = bag.getRow(rn);
      
      while (nextBeaconRn <= rn) {
        if (rn == nextBeaconRn)
          outBeacons.add(beacons.get(bcIndex));
        
        ++bcIndex;
        nextBeaconRn =
            bcIndex == beacons.size() ?
                Long.MAX_VALUE : beacons.get(bcIndex).a;
      }
    }
    
    
    List<Tuple<Long,Long>> ob;
    
    if (outBeacons.isEmpty())
      ob = Collections.emptyList();
    else {
      outBeacons.trimToSize();
      ob = Collections.unmodifiableList(outBeacons);
    }
    
    List<Row> ro = Lists.asReadOnlyList(rows);
    
    // constructor validates.. if the data is garbage, it blows up here
    return new Path(ro, ob, true);
  }
  

}
