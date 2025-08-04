module io.crums.sldg.bindle {
  requires transitive io.crums.sldg.src;
  requires transitive io.crums.sldg.base;
  requires transitive io.crums.tc;
  
  exports io.crums.sldg.bindle;
  exports io.crums.sldg.bindle.util;
  exports io.crums.sldg.bindle.tc;
}