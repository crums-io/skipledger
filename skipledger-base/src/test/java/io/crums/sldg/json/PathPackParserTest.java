/*
 * Copyright 2024 Babak Farhang
 */
package io.crums.sldg.json;


import static io.crums.sldg.PathTest.newRandomLedger;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.crums.sldg.AbstractSkipLedgerTest;
import io.crums.sldg.Path;
import io.crums.sldg.PathPack;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.testing.SelfAwareTestCase;
import io.crums.util.Strings;
import io.crums.util.json.JsonPrinter;
import io.crums.util.json.simple.JSONArray;

import org.junit.jupiter.api.Test;

/**
 * 
 */
public class PathPackParserTest extends SelfAwareTestCase {
  
  final static String TARGETS = "bnos";
  final static String HASHES = "hashes";
  final static String TYPE = "type";
  
  @Test
  public void testOneB64() {
    Object label = new Object() { };
    int size = 1;
    var codec = HashEncoding.BASE64_32;
    testStateRoundtrip(label, size, codec);
  }
  
  @Test
  public void testOneHex() {
    Object label = new Object() { };
    int size = 1;
    var codec = HashEncoding.HEX;
    testStateRoundtrip(label, size, codec);
  }
  
  @Test
  public void testTwo() {
    Object label = new Object() { };
    int size = 2;
    var codec = HashEncoding.BASE64_32;
    testStateRoundtrip(label, size, codec);
  }
  
  @Test
  public void testThree() {
    Object label = new Object() { };
    int size = 3;
    var codec = HashEncoding.BASE64_32;
    testStateRoundtrip(label, size, codec);
  }
  
  @Test
  public void test500k() {
    Object label = new Object() { };
    int size = 500_000;
    var codec = HashEncoding.BASE64_32;
    testStateRoundtrip(label, size, codec);
  }
  
  @Test
  public void test512k() {
    Object label = new Object() { };
    int size = 512 * 1024;
    var codec = HashEncoding.BASE64_32;
    testStateRoundtrip(label, size, codec);
  }
  
  @Test
  public void test512kWithTarget() {
    Object label = new Object() { };
    int size = 512 * 1024;
    var codec = HashEncoding.BASE64_32;
    int target = 256 * 1024 + 8_722;
    testStateRoundtrip(label, size, codec, target);
  }
  
  @Test
  public void testMillion() {
    Object label = new Object() { };
    int size = 1_000_000;
    var codec = HashEncoding.BASE64_32;
    if (checkEnabled(AbstractSkipLedgerTest.TEST_ALL, label))
      testStateRoundtrip(label, size, codec);
  }
  
  @Test
  public void test4M() {
    Object label = new Object() { };
    int size = 4 * 1024 * 1024;
    var codec = HashEncoding.BASE64_32;
    if (checkEnabled(AbstractSkipLedgerTest.TEST_ALL, label))
      testStateRoundtrip(label, size, codec);
  }
  

  private void testStateRoundtrip(Object label, int size, HashEncoding codec) {
    testStateRoundtrip(label, size, codec, -1L);
  }
  
  private void testStateRoundtrip(Object label, int size, HashEncoding codec, long targetRn) {

    SkipLedger ledger = newRandomLedger(size);
    boolean hasTarget = targetRn > 0;
    Path state = hasTarget ?
        ledger.getPath(1L, targetRn, (long) size) :
        ledger.statePath();
    
    var expected = PathPack.forPath(state);
    var parser = new PathPackParser(
        codec, TARGETS, TYPE, HASHES);
    var jObj = parser.toJsonObject(expected);
    System.out.println();
    System.out.println(method(label) + ":");
    if (hasTarget)
      System.out.println("target: " + targetRn);
    
    JsonPrinter.println(jObj);
    var rt = parser.toEntity(jObj);
    
    assertEquals(expected.getFullRowNumbers(), rt.getFullRowNumbers());
    assertEquals(expected.inputsBlock(), rt.inputsBlock());
    assertEquals(expected.refsBlock(), rt.refsBlock());
    
    int hashCount =
        (rt.inputsBlock().remaining() + rt.refsBlock().remaining())
        / SldgConstants.HASH_WIDTH;
    
    System.out.println(
        Strings.nOf(hashCount, "hash") + " for " + Strings.nOf(size, "row"));
  }


  @Test
  public void testCompressed() {
    final Object label = new Object() { };
    final int size = 1_573_718;
    final var codec = HashEncoding.BASE64_32;
    var ledger = newRandomLedger(size);
    Path path = ledger.statePath().compress();
    var pack = PathPack.forPath(path);
    var parser = new PathPackParser(
        codec, TARGETS, TYPE, HASHES);

    var jPack = parser.toJsonObject(pack);
    System.out.println();
    System.out.println(method(label) + ":");

    JsonPrinter.println(jPack);
    var rt = parser.toEntity(jPack);

    assertEquals(path, rt.path());

    int hashCount = ((JSONArray) jPack.get(HASHES)).size();
    System.out.println(hashCount + " hashes for " + size + " rows (compressed)");
  }

  @Test
  public void testCompressedWithTarget() {
    final Object label = new Object() { };
    final int size = 10_573_718;
    final var codec = HashEncoding.BASE64_32;
    final long targetRn = size - 2097;
    var ledger = newRandomLedger(size);
    Path path = ledger.getPath(1L, targetRn, (long) size).compress();
    var pack = PathPack.forPath(path);
    var parser = new PathPackParser(
        codec, TARGETS, TYPE, HASHES);

    var jPack = parser.toJsonObject(pack);
    System.out.println();
    System.out.println(method(label) + ":");

    JsonPrinter.println(jPack);
    var rt = parser.toEntity(jPack);

    assertEquals(path, rt.path());

    int hashCount = ((JSONArray) jPack.get(HASHES)).size();
    System.out.println(hashCount + " hashes for " + size + " rows (compressed)");

  }

}


