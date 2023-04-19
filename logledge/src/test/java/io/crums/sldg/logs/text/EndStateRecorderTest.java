/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.gnahraf.test.IoTestCase;

import io.crums.sldg.SldgConstants;


/**
 * 
 */
public class EndStateRecorderTest extends IoTestCase {


  private File testDir(Object label) {
    var dir = getMethodOutputFilepath(label);
    dir.mkdirs();
    assert dir.isDirectory();
    return dir;
  }
  
  
  @Test
  public void testSerialization() throws IOException {
    final Object label = new Object() {  };
    final File dir = testDir(label);
    
    long seed = 11;
    var rand = new Random(seed);
    final int rows = 9;
    
    State preState, state = State.EMPTY;
    ByteBuffer inputHash;
    do {
      inputHash = nextRandomHash(rand);
      long eol = nextRandOffset(state, rand);
      long line = nextRandLineNo(state, rand);
      preState = state;
      state = new State(state.frontier().nextFrontier(inputHash), eol, line);
    } while (state.rowNumber() < rows);
    
    Fro fro = new Fro(preState, inputHash, state.eolOffset(), state.lineNo());
    
    var endRecorder = new EndStateRecorder(dir);
    endRecorder.save(fro);
    
    Fro out = endRecorder.loadState(fro.rowNumber()).get();
    assertEquals(fro, out);
  }
  
  
  private ByteBuffer nextRandomHash(Random rand) {
    byte[] bytes = new byte[SldgConstants.HASH_WIDTH];
    rand.nextBytes(bytes);
    return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
  }
  
  private final static int MAX_EOL_INCR = 18;
  
  private long nextRandOffset(State state, Random rand) {
    return 2 + state.eolOffset() + rand.nextInt(MAX_EOL_INCR);
  }

  
  private final static int MAX_LINE_INCR = 3;

  private long nextRandLineNo(State state, Random rand) {
    return 1 + state.lineNo() + rand.nextInt(MAX_LINE_INCR);
  }

}
