/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.testy;

import static org.junit.jupiter.api.Assertions.*;

import picocli.CommandLine.Help.Ansi;
import org.junit.jupiter.api.Test;
/**
 * 
 */
public class JansiTest {

  
  @Test
  public void testAnsiStringLength() {
    var jello = "jello";
    var jansiEncoded = "@|fg(yellow) " + jello + "|@";
    var ansi = Ansi.AUTO.text(jansiEncoded);
    int len = ansi.getCJKAdjustedLength();
    assertEquals(jello.length(), len);
  }

}


