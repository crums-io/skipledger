module io.crums.sldg.sql {
  
  requires java.sql;
  requires io.crums.util.xp;
  
  requires transitive io.crums.sldg.base;
  
  exports io.crums.sldg.sql;
}