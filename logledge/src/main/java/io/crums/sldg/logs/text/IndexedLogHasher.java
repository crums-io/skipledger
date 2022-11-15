/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.logs.text;

import java.util.List;

/**
 * 
 */
public class IndexedLogHasher extends LogHasher {

  /**
   * 
   */
  public IndexedLogHasher(BaseArgs args, List<Long> offsets, int rnFactor) {
    super(args);
  }

}
