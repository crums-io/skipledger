/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;

import java.util.List;

import io.crums.sldg.Entry;
import io.crums.sldg.EntryInfo;

/**
 * A bag of {@linkplain Entry entries}.
 */
public interface EntryBag {

  
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
