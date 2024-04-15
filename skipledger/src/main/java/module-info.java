module io.crums.sldg.ledgers {
  
  requires transitive io.crums.sldg.base;
  
  requires transitive io.crums.core;
  
  requires io.crums.util.xp;
  
  exports io.crums.sldg.bags;
  exports io.crums.sldg.fs;
  exports io.crums.sldg.json;
  exports io.crums.sldg.ledgers;
  exports io.crums.sldg.mrsl;
  exports io.crums.sldg.packs;
  exports io.crums.sldg.src;
  exports io.crums.sldg.time;
  exports io.crums.sldg.util;
  
}