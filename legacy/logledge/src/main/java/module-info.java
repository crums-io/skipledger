/*
 * Copyright 2022 crums.io
 */
module io.crums.sldg.logs {
  
  requires transitive io.crums.sldg.ledgers;
  
  requires io.crums.util.xp;
  
  exports io.crums.sldg.logs.text;
}