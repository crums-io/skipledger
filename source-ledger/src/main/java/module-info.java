module io.crums.sldg.src {

  requires transitive io.crums.util;
  requires io.crums.util.io.buffer;
  requires transitive io.crums.sldg.salt;
  requires transitive io.crums.jsonimple;

  exports io.crums.sldg.src;
  exports io.crums.sldg.src.json;

}