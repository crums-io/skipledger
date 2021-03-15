/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.util.List;

import io.crums.model.CrumTrail;

/**
 * Adds support for {@linkplain TrailedPath}s and row sources
 * (a.k.a {@linkplain Entry}). These are appendages that are somehow
 * maintained outside the {@linkplain Ledger}.
 */
public interface MorselBag extends PathBag {
  
  
  /**
   * Returns information about the available {@linkplain TrailPath}s.
   * There's a {@linkplain CrumTrail} at the end of the path (i.e. at the
   * highest row number).
   */
  List<PathInfo> availableTrailPaths();
  
  
  /**
   * Returns the crumtrail for the given {@code rowNumber}. The given
   * row number is highest rowNumber in one of the {@linkplain PathInfo}
   * instances returned by {@linkplain #availableTrailPaths()}.
   */
  CrumTrail crumTrail(long rowNumber);
  
  
  /**
   * Returns information about the entries in this bag.
   * 
   * @return ordered list
   * 
   * @see #entry(long)
   */
  List<EntryInfo> availableEntries();
  
  
  /**
   * Returns the entry for the given row number.
   * 
   * @see #availableEntries()
   */
  Entry entry(long rowNumber);
  
  
  
  

}
