module io.crums.sldg.mc.mgmt {
  
  requires transitive java.sql;
  requires transitive com.zaxxer.hikari;
  requires io.crums.sldg.src.sql;
  
  requires transitive io.crums.sldg.mc;
  exports io.crums.sldg.mc.mgmt;
}