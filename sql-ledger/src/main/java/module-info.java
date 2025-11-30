module io.crums.sldg.src.sql {
  
  requires java.sql;
  requires transitive io.crums.jsonimple;
  requires transitive io.crums.sldg.src;
  
  exports io.crums.sldg.src.sql;
  exports io.crums.sldg.src.sql.config;
  
}
