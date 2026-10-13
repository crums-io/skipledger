/*
 * Copyright 2025 Babak Farhang
 */
module io.crums.sldg.logledger {
  
  requires transitive io.crums.sldg.base;
  requires transitive io.crums.sldg.src;

  requires io.crums.util.xp;
  requires io.crums.util.io;
  requires io.crums.util.io.buffer;
  requires io.crums.util.io.sef;
  
  exports io.crums.sldg.logledger;
}