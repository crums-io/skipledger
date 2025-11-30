module io.crums.sldg.mc.fx {
  
  uses java.sql.Driver;
  requires java.sql.rowset;
  requires transitive javafx.controls;
  requires java.prefs;
  requires io.crums.sldg.mc.mgmt;
  requires org.kordamp.ikonli.core;
  requires org.kordamp.ikonli.javafx;
  requires org.kordamp.ikonli.fontawesome6;
  exports io.crums.sldg.mc.fx to javafx.graphics;
}