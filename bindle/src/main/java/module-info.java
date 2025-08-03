module io.crums.sldg.morsel {
  requires transitive io.crums.sldg.src;
  requires transitive io.crums.sldg.base;
  requires transitive io.crums.tc;
  
  exports io.crums.sldg.morsel;
  exports io.crums.sldg.morsel.util;
  exports io.crums.sldg.morsel.tc;
}