/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static org.junit.jupiter.api.Assertions.*;
import static io.crums.sldg.logs.text.StateHasherTest.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.gnahraf.test.IoTestCase;

import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.io.sef.Alf;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.src.TableSalt;
import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * 
 */
public class LogHasherTest extends IoTestCase {

  
  final static ByteBuffer FHEADER = hBuffer("FT-2");
  final static ByteBuffer OHEADER = hBuffer("LO-23");
  final static ByteBuffer LHEADER = hBuffer("LNOS");
  
  final static String FT = "FT";
  final static String LO = "LO";
  final static String LN = "LN";

  
  final static int HD_SPLIT_OFFSET = 169;
  
  final static String HD_MOD_OFFSET = "hd-mod_offset.log";
  final static String HD_MOD_ROW = "hd-mod_row.log";
  
  
  private static ByteBuffer hBuffer(String txt) {
    return ByteBuffer.wrap(txt.getBytes(Strings.UTF_8)).asReadOnlyBuffer();
  }
  
  @Test
  public void testHdExpo1() throws Exception {
    Object label = new Object() { };
    final int rnExpo = 1;
    testHd(label, rnExpo, false, null);
  }
  
  @Test
  public void testHdExpo0() throws Exception {
    Object label = new Object() { };
    final int rnExpo = 0;
    testHd(label, rnExpo, false);
  }
  
  @Test
  public void testHdExpo2() throws Exception {
    Object label = new Object() { };
    final int rnExpo = 2;
    testHd(label, rnExpo, false);
  }
  
  @Test
  public void testHdExpo2NoComment() throws Exception {
    Object label = new Object() { };
    final int rnExpo = 2;
    testHd(label, rnExpo, true);
  }
  

  
  private void testHd(
      Object label, int rnExpo,
      boolean xComments) throws Exception {
    testHd(label, rnExpo, xComments, null);
  }
  
  private void testHd(
      Object label, int rnExpo,
      boolean xComments,
      String tokenDelimiters) throws Exception {
    
    long expectedRows = xComments ? HD_NO_BLANK - HD_COMMENT : HD_NO_BLANK;
    
    final File dir = testDir(label);
    
    int randSeed = method(label).hashCode();
    var salter = newSalter(randSeed);
    
    var fTable = FrontierTableFile.create(new File(dir,  FT), FHEADER);
    var rowOffsets = new Alf(
        createFile(new File(dir, LO), OHEADER),
        OHEADER.capacity());
    
     var lineNos = new Alf(
         createFile(new File(dir, LN), LHEADER),
         LHEADER.capacity());
    
    Predicate<ByteBuffer> cFilter =
        xComments ? (b) -> b.get(b.position()) == '#' : null;
    
    var hasher = new LogHasher(salter.clone(), cFilter, tokenDelimiters, rowOffsets, lineNos, fTable, rnExpo);
    HashFrontier state;
    
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      state = hasher.play(log).frontier();
    }

    assertEquals(expectedRows, state.rowNumber());
    
    long expectedFLs = expectedRows / hasher.rnDelta();
    
    assertEquals(expectedFLs, fTable.size());
    assertEquals(expectedFLs, rowOffsets.size());
  }
  
  
  
  @Test
  public void testPlayHd2xExpo0() throws Exception {
    Object label = new Object() { };
    int rnExpo = 0;
    boolean xComments = false;
    String tokenDelimiters = null;
    int splitOffset = 169;
    int lineNo = 8;
    
    testPlayHd2x(label, rnExpo, xComments, tokenDelimiters, splitOffset, lineNo);
  }
  
  @Test
  public void testPlayHd2xExpo0SansC() throws Exception {
    Object label = new Object() { };
    int rnExpo = 0;
    boolean xComments = true;
    
    testPlayHd2x(label, rnExpo, xComments);
  }
  
  
  @Test
  public void testPlayHd2xExpo2() throws Exception {
    Object label = new Object() { };
    int rnExpo = 2;
    boolean xComments = false;
    
    testPlayHd2x(label, rnExpo, xComments);
  }
  
  
  @Test
  public void testUpdateHdExpo2() throws Exception {
    Object label = new Object() { };
    int rnExpo = 2;
    boolean xComments = false;
    
    testUpdateHd(label, rnExpo, xComments);
  }
  
  
  @Test
  public void testUpdateHdExpo1NoReplay() throws Exception {
    Object label = new Object() { };
    int rnExpo = 1;
    boolean xComments = false;
    
    testUpdateHd(label, rnExpo, xComments, true);
  }

  
  @Test
  public void testOffsetConflictOnUpdate() throws Exception {
    Object label = new Object() { };
    int rnExpo = 0;
    final File dir = testDir(label);
    
    var salter = newTableSalt(label);
    
    var fTable = FrontierTableFile.create(new File(dir,  FT), FHEADER);
    var rowOffsets = new Alf(
        createFile(new File(dir, LO), OHEADER),
        OHEADER.capacity());

    var lineNos = new Alf(
        createFile(new File(dir, LN), LHEADER),
        LHEADER.capacity());
    
    var hasher = new LogHasher(salter.clone(), null, null, rowOffsets, lineNos, fTable, rnExpo);
    
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      hasher.play(log);
    }
    
    try (var log = getClass().getResourceAsStream(HD_MOD_OFFSET)) {
      hasher.play(log);
      fail();
    } catch (OffsetConflictException expected) {
//      System.out.println(method(label) + ": expected error: " + expected);
    }

    hasher.close();
    
  }
  
  
  
  @Test
  public void testRecoverFromOffsetConflict() throws Exception {
    Object label = new Object() { };
    int rnExpo = 0;
    final File dir = testDir(label);
    
    var salter = newTableSalt(label);
    
    var fTable = FrontierTableFile.create(new File(dir,  FT), FHEADER);
    var rowOffsets = new Alf(
        createFile(new File(dir, LO), OHEADER),
        OHEADER.capacity());

    var lineNos = new Alf(
        createFile(new File(dir, LN), LHEADER),
        LHEADER.capacity());
    
    var hasher = new LogHasher(salter.clone(), null, null, rowOffsets, lineNos, fTable, rnExpo);
    
    HashFrontier expectedState;
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      expectedState = hasher.play(log).frontier();
    }
    
    OffsetConflictException conflict;
    try (var log = getClass().getResourceAsStream(HD_MOD_OFFSET)) {
      hasher.play(log);
      conflict = null;
      fail();
    } catch (OffsetConflictException expected) {
      // System.out.println(method(label) + ":\n  expected error: " + expected);
      conflict = expected;
    }
    

    File logFile = copyResource(dir, HD_MOD_OFFSET);

    HashFrontier state;
    try (var fc = openReadonly(logFile)) {
      state = hasher.recoverFromConflict(conflict, fc).frontier();
    }

    hasher.close();
    
    assertEquals(expectedState, state);
    
  }
  
  
  @Test
  public void testRecoverFromHashConflict() throws Exception {
    Object label = new Object() { };
    int rnExpo = 0;
    final File dir = testDir(label);
    
    var salter = newTableSalt(label);
    
    var fTable = FrontierTableFile.create(new File(dir,  FT), FHEADER);
    var rowOffsets = new Alf(
        createFile(new File(dir, LO), OHEADER),
        OHEADER.capacity());

    var lineNos = new Alf(
        createFile(new File(dir, LN), LHEADER),
        LHEADER.capacity());
    
    var hasher = new LogHasher(salter.clone(), null, null, rowOffsets, lineNos, fTable, rnExpo);
    
    try (var log = getClass().getResourceAsStream(HD_LOG)) {
      hasher.play(log);
    }
    
    
    
    RowHashConflictException conflict;
    try (var log = getClass().getResourceAsStream(HD_MOD_ROW)) {
      hasher.play(log);
      conflict = null;
      fail();
    } catch (RowHashConflictException expected) {
      // System.out.println(method(label) + ":\n  expected error: " + expected);
      conflict = expected;
    }
    
    

    File logFile = copyResource(dir, HD_MOD_ROW);

    HashFrontier state, expectedState;
    try (var fc = openReadonly(logFile)) {
      state = hasher.recoverFromConflict(conflict, fc).frontier();
      expectedState = new StateHasher(hasher).play(fc.position(0)).frontier();
    }

    hasher.close();
    
    assertEquals(expectedState, state);
  }
  
  
  
  @Test
  public void testGetFullRow() throws Exception {
    Object label = new Object() { };
    int rnExpo = 1;
    final File dir = testDir(label);
    File logFile = copyResource(dir, HD_LOG);
    
    var salter = newTableSalt(label);
    
    var fTable = FrontierTableFile.create(new File(dir,  FT), FHEADER);
    var rowOffsets = new Alf(
        createFile(new File(dir, LO), OHEADER),
        OHEADER.capacity());

    var lineNos = new Alf(
        createFile(new File(dir, LN), LHEADER),
        LHEADER.capacity());
    var hasher = new LogHasher(salter, COMMENT_TEST, null, rowOffsets, lineNos, fTable, rnExpo);

    long rn = 4;
    String expectedLine = "Sat on a wall and took a great fall";
    
    FullRow row;
    try (var log = openReadonly(logFile)) {
      hasher.play(log);
      row = hasher.getFullRow(rn, log);
      hasher.close();
    }
    
    assertEquals(rn, row.rowNumber());
    assertEquals(expectedLine, asText(row.columns()));
  }
  
  
  
  private TableSalt newTableSalt(Object label) {
    int randSeed = method(label).hashCode();
    return newSalter(randSeed);
  }
  
  
  
  private void testPlayHd2x(
      Object label, int rnExpo,
      boolean xComments) throws Exception {

    String tokenDelimiters = null;
    int splitOffset = 169;
    int lineNo = 8;
    
    testPlayHd2x(label, rnExpo, xComments, tokenDelimiters, splitOffset, lineNo);
  }
  
  private void testPlayHd2x(
      Object label, int rnExpo,
      boolean xComments,
      String tokenDelimiters,
      int splitOffset,  // expected at 1 beyond EOL
                        // AND at end of block
      int splitLineNo) throws Exception {
    

    final File dir = testDir(label);

    int randSeed = method(label).hashCode();
    var salter = newSalter(randSeed);

    Predicate<ByteBuffer> cFilter =
        xComments ? (b) -> b.get(b.position()) == '#' : null;
    
    File frontierFile = new File(dir,  FT);
    File offsetFile = new File(dir, LO);
    var fTable = FrontierTableFile.create(frontierFile, FHEADER);
    var rowOffsets = new Alf(
        createFile(offsetFile, OHEADER),
        OHEADER.capacity());

    var lineNos = new Alf(
        createFile(new File(dir, LN), LHEADER),
        LHEADER.capacity());
    
    var hasher = new LogHasher(salter.clone(), cFilter, tokenDelimiters, rowOffsets, lineNos, fTable, rnExpo);
    
    File logFile = new File(dir, HD_LOG);
    copyResourceToFile(logFile, HD_LOG);
    HashFrontier state;
    
    try (var closer = new TaskStack()) {
      var fc = openReadonly(logFile);
      closer.pushClose(fc);
      var trunc = ChannelUtils.truncate(fc, splitOffset);
      state = hasher.play(trunc).frontier();
    }
    
//    System.out.println(state);
    
    // close hasher and reopen
    hasher.close();
    
    fTable = FrontierTableFile.loadReadWrite(frontierFile, FHEADER);
    rowOffsets = new Alf(
        openReadWrite(offsetFile),
        OHEADER.capacity());

    lineNos = new Alf(
        openReadWrite(new File(dir, LN)),
        LHEADER.capacity());
    
    
    hasher = new LogHasher(salter.clone(), cFilter, tokenDelimiters, rowOffsets, lineNos, fTable, rnExpo);
    
    long rc = hasher.endOffsetsRecorded();
    assert rc > 0;
    assertEquals(splitOffset, hasher.endOffset(rc - 1));
    
    // play the state post the split offset
    HashFrontier recomputedState;
    try (var closer = new TaskStack()) {
      closer.pushClose(hasher);
      var fc = openReadonly(logFile);
      closer.pushClose(fc);
      fc.position(splitOffset);
      state = hasher.play(fc, new State(state, splitOffset, splitLineNo)).frontier();
      
      var stateHasher = new StateHasher(salter.clone(), cFilter, tokenDelimiters);
      recomputedState = stateHasher.play(fc.position(0)).frontier();
    }
    
    assertEquals(recomputedState, state);
  }
  
  


  private void testUpdateHd(
      Object label, int rnExpo,
      boolean xComments) throws Exception {
    
    testUpdateHd(label, rnExpo, xComments, false);
  }
  

  private void testUpdateHd(
      Object label, int rnExpo,
      boolean xComments, boolean noReplay) throws Exception {
    

    String tokenDelimiters = null;
    int splitOffset = 169;

    final File dir = testDir(label);

    int randSeed = method(label).hashCode();
    var salter = newSalter(randSeed);

    Predicate<ByteBuffer> cFilter =
        xComments ? (b) -> b.get(b.position()) == '#' : null;
    
    File frontierFile = new File(dir,  FT);
    File offsetFile = new File(dir, LO);
    var fTable = FrontierTableFile.create(frontierFile, FHEADER);
    var rowOffsets = new Alf(
        createFile(offsetFile, OHEADER),
        OHEADER.capacity());

    var lineNos = new Alf(
        createFile(new File(dir, LN), LHEADER),
        LHEADER.capacity());
    
    var hasher = new LogHasher(salter.clone(), cFilter, tokenDelimiters, rowOffsets, lineNos, fTable, rnExpo);
    
    File logFile = copyResource(dir, HD_LOG);
    HashFrontier state;
    
    try (var closer = new TaskStack()) {
      var fc = openReadonly(logFile);
      closer.pushClose(fc);
      var trunc = ChannelUtils.truncate(fc, splitOffset);
      state = hasher.play(trunc).frontier();
    }
    
    // close hasher and reopen
    hasher.close();
    
    fTable = FrontierTableFile.loadReadWrite(frontierFile, FHEADER);
    rowOffsets = new Alf(
        openReadWrite(offsetFile),
        OHEADER.capacity());
    
    lineNos = new Alf(
        openReadWrite(new File(dir, LN)),
        LHEADER.capacity());
    
    if (noReplay)
      hasher = new LogHasher(salter.clone(), cFilter, tokenDelimiters, rowOffsets, lineNos, fTable, rnExpo) {
          @Override
          protected int replayLimit() {
            return 2;
          }
        };
    else
      hasher = new LogHasher(salter.clone(), cFilter, tokenDelimiters, rowOffsets, lineNos, fTable, rnExpo);
    
    // play the state post the split offset
    HashFrontier recomputedState;
    try (var closer = new TaskStack()) {
      closer.pushClose(hasher);
      var fc = openReadonly(logFile);
      closer.pushClose(fc);
      
      state = hasher.update(fc).frontier();
      
      recomputedState = new StateHasher(salter, cFilter, tokenDelimiters).play(fc.position(0)).frontier();
    }
    
    assertEquals(recomputedState, state);
  }
  
  
  
  
  
  
  
  private File testDir(Object label) {
    var dir = getMethodOutputFilepath(label);
    dir.mkdirs();
    assert dir.isDirectory();
    return dir;
  }
  
  
  private FileChannel createFile(File file, ByteBuffer header) throws IOException {
    var fc = Opening.CREATE.openChannel(file);
    ChannelUtils.writeRemaining(fc, 0, header.duplicate());
    return fc;
  }
  
  private FileChannel openReadonly(File file) throws IOException {
    return Opening.READ_ONLY.openChannel(file);
  }
  
  private FileChannel openReadWrite(File file) throws IOException {
    return Opening.READ_WRITE_IF_EXISTS.openChannel(file);
  }
  
  
//  private File copyResource(File dir, String resource) throws IOException {
//    File copy = new File(dir, resource);
//    copyResourceToFile(copy, resource);
//    return copy;
//  }
//  
//  private void copyResourceToFile(File file, String resource) throws IOException {
//    var res = getClass().getResourceAsStream(resource);
//    copyToFile(file, res);
//  }
//  
//  private void copyToFile(File file, InputStream res) throws IOException {
//    try (TaskStack closer = new TaskStack()) {
//      closer.pushClose(res);
//      var fstream = new FileOutputStream(file);
//      closer.pushClose(fstream);
//      byte[] buffer = new byte[4096];
//      while (true) {
//        int len = res.read(buffer);
//        if (len == -1)
//          break;
//        fstream.write(buffer, 0, len);
//      }
//    }
//  }

}
