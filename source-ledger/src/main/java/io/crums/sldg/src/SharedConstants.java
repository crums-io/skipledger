/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.src;

import io.crums.util.hash.Digest;
import io.crums.util.hash.Digests;

/**
 * Some constants in the base module are reused here. Redefined here so that
 * we can delay (or avoid altogether) dependency on base module. (A cleaner
 * solution, would be to centralize constants in a standalone sub-module.
 */
public class SharedConstants {
  
  // never
  private SharedConstants() {  }
  
  final static Digest DIGEST = Digests.SHA_256;
  
  public final static int HASH_WIDTH = DIGEST.hashWidth();

}

