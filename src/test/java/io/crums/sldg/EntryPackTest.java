/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import  org.junit.Test;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.sldg.entry.Entry;
import io.crums.sldg.entry.TextEntry;
import io.crums.sldg.packs.EntryPack;
import io.crums.sldg.packs.EntryPackBuilder;

/**
 * 
 */
public class EntryPackTest extends SelfAwareTestCase {
  
  @Test
  public void testEmpty() {
    EntryPackBuilder builder = new EntryPackBuilder();
    ByteBuffer serialForm = builder.serialize();
    EntryPack pack = EntryPack.load(serialForm);
    assertTrue(pack.availableEntries().isEmpty());
    try {
      pack.entry(1);
      fail();
    } catch (IllegalArgumentException expected) {  }
  }
  
  
  @Test
  public void testOne() {
    final Object label = new Object() { };
    
    int[] rns = {
//        1574,
        1580
    };
    String[] entryTexts = {
//        "this is a test",
        "this is _only_ a test",
    };
    
    test(rns, entryTexts, label);
  }
  
  
  @Test
  public void testTwo() {
    final Object label = new Object() { };
    
    int[] rns = {
        1574,
        1580
    };
    String[] entryTexts = {
        "this is a test",
        "this is _only_ a test",
    };
    
    test(rns, entryTexts, label);
  }
  
  
  
  
  private void test(int[] rns, String[] entryTexts, Object label) {
    
    TextEntry[] entries = new TextEntry[rns.length];
    for (int index = 0; index < rns.length; ++index)
      entries[index] = new TextEntry(entryTexts[index], rns[index]);

    EntryPackBuilder builder = new EntryPackBuilder();
    for (var entry : entries)
      builder.setEntry(entry.rowNumber(), entry.content(), null);
    
    ByteBuffer serialForm = builder.serialize();
    EntryPack pack = EntryPack.load(serialForm);
    
    assertEquals(rns.length, pack.availableEntries().size());
    System.out.println(method(label) + ":- entries: " + pack.availableEntries());
    
    for (var expected : entries) {
      Entry actual = pack.entry(expected.rowNumber());
      assertEquals(expected.rowNumber(), actual.rowNumber());
      assertEquals(expected.content(), actual.content());
    }
    
  }

}






