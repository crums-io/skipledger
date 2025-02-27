module io.crums.sldg.sql {
  
  requires io.crums.util.xp;

  requires transitive java.sql;
  requires transitive io.crums.sldg.ledgers;
  
  exports io.crums.sldg.sql;
}