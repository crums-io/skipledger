/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static org.junit.jupiter.api.Assertions.*;
import static io.crums.sldg.logs.text.StateHasherTest.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

import org.junit.jupiter.api.Test;

import com.gnahraf.test.IoTestCase;

import io.crums.io.BackedupFile;
import io.crums.io.channels.ChannelUtils;

/**
 * 
 */
public class LogHashRecorderTest extends IoTestCase {


  private File testDir(Object label) {
    var dir = getMethodOutputFilepath(label);
    dir.mkdirs();
    assert dir.isDirectory();
    return dir;
  }

  
  static void copyResourceToFile(File file, String resource, long len) throws IOException {
    var res = LogHashRecorderTest.class.getResourceAsStream(resource);
    copyToFile(file, res, len);
  }
  
  
  static void copyToFile(File file, InputStream res, long len) throws IOException {
    var ch = Channels.newChannel(res);
    var tr = ChannelUtils.truncate(ch, len);
    var truncRes = Channels.newInputStream(tr);
    StateHasherTest.copyToFile(file, truncRes);
  }
  
  
  @Test
  public void testHd() throws Exception {
    Object label = new Object() { };
    
    int dex = 1;
    
    File dir = testDir(label);
    
    File log = new File(dir, HD_LOG);
    
    copyResourceToFile(log, HD_LOG, LogHasherTest.HD_SPLIT_OFFSET);
    
    State state;
    StateHasher hasher;
    try (var hashRecorder = new LogHashRecorder(log, null, null, dex)) {
      state = hashRecorder.update();
      hasher = hashRecorder.stateHasher();
    }
    
    
    BackedupFile backup = new BackedupFile(log);
    backup.moveToBackup();
    StateHasherTest.copyResourceToFile(log, HD_LOG);
    
    
    try (var hashRecorder = new LogHashRecorder(log, false)) {
      var prevState = hashRecorder.getState().get();
      assertEquals(state, prevState);
      
      state = hashRecorder.update();
    }
    
    State expected = hasher.play(log);
    assertEquals(expected, state);
  }
  
  
  @Test
  public void testHdSansComments() throws Exception {
    Object label = new Object() { };
    
    int dex = 1;
    
    File dir = testDir(label);
    
    
//    File log = copyResource(dir, HD_LOG);
    File log = new File(dir, HD_LOG);
    copyResourceToFile(log, HD_LOG, LogHasherTest.HD_SPLIT_OFFSET);
    
    
    State state;
    StateHasher hasher;
    try (var hashRecorder = new LogHashRecorder(log, "#", null, dex)) {
      state = hashRecorder.update();
      hasher = hashRecorder.stateHasher();
    }

    
    BackedupFile backup = new BackedupFile(log);
    backup.moveToBackup();
    StateHasherTest.copyResourceToFile(log, HD_LOG);
    
    
    try (var hashRecorder = new LogHashRecorder(log, false)) {
      var prevState = hashRecorder.getState().get();
      assertEquals(state, prevState);
      
      state = hashRecorder.update();
    }
    
    State expected = hasher.play(log);
    
    assertEquals(expected, state);
    assertEquals(HD_SANS_COMMENT, state.rowNumber());
  }

}













