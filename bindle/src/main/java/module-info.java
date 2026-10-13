module io.crums.sldg.bindle {
  requires transitive io.crums.sldg.src;
  requires transitive io.crums.sldg.base;
  requires transitive io.crums.tc;
  requires io.crums.util.io;
  requires io.crums.util.io.buffer;
  
  exports io.crums.sldg.bindle;
  exports io.crums.sldg.bindle.util;
  exports io.crums.sldg.bindle.tc;
}