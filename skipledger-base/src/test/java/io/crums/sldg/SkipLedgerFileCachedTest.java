/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;

import java.io.File;

import org.junit.jupiter.api.Assertions;


import io.crums.io.Opening;
import io.crums.sldg.cache.RowCache;

/**
 * Unit test for skip ledger cache under development. The base test cases are
 * ill suited for testing the effectiveness of the cache tho. (It's designed to
 * address the {@linkplain SkipLedger#getPath(java.util.List)},
 * {@linkplain SkipLedger#skipPath(long, long)},
 * {@linkplain SkipLedger#statePath()} type use cases). Still, surprised the
 * overhead my prototype caching introduces. I'll return to caching, later.
 * 
 * 
 * <p>TODO: add these as a performance test in the base test.</p>
 */
public class SkipLedgerFileCachedTest extends AbstractSkipLedgerTest {

  
  
  
  
  
  
  @Override
  protected SkipLedger newLedger(Object methodLabel) throws Exception {
    File file = getMethodOutputFilepath(methodLabel);
    RowCache cache = new RowCache(8, 1);
    return new SkipLedgerFile(file, Opening.CREATE_ON_DEMAND, false, cache);
  }

  @Override
  protected SkipTable newTable(Object methodLabel) throws Exception {
    Assertions.fail();
    return null;
  }

}
