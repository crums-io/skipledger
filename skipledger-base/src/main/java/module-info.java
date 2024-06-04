module io.crums.sldg.base {
  
  requires io.crums.util.mrkl;

  requires transitive io.crums.util;
  requires transitive io.crums.jsonimple;
  
  exports io.crums.sldg;
  exports io.crums.sldg.cache;
  exports io.crums.sldg.json;
  
}