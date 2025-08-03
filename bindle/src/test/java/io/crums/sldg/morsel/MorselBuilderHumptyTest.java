/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.morsel;


import static org.junit.jupiter.api.Assertions.*;
import static io.crums.sldg.morsel.NuggetBuilderDumptyTest.*;

import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import io.crums.sldg.HashConflictException;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SkipLedgers;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.logledger.Grammar;
import io.crums.sldg.logledger.LogLedger;
import io.crums.sldg.morsel.tc.NotarizedRow;
import io.crums.sldg.src.SaltScheme;
import io.crums.tc.BlockProof;
import io.crums.tc.CargoProof;
import io.crums.tc.ChainParams;
import io.crums.tc.TimeBinner;
import io.crums.testing.IoTestCase;
import io.crums.util.TaskStack;

/**
 * 
 */
public class MorselBuilderHumptyTest extends IoTestCase {
  
  
  final static long TEST_EPOCH =
      new Calendar.Builder()
      .setFields(Calendar.YEAR, 2025, Calendar.DAY_OF_YEAR, 90)
      .build().getTimeInMillis();
  
  
  final static long ORIGINS_REF_START = 28L;
  final static long ORIGINS_REF_END = ORIGINS_REF_START + HD_LINECOUNT - 1;
  
  
  @Test
  public void testTwoLogsWithSource() throws Exception {
    final var label = new Object() {  };
    
    MorselBuilder morsel = prepare(label, true);
    assertEquals(
        List.of(1L, HD_LINECOUNT),
        morsel.getNugget(HD).sourcePack().get().sourceNos());
    assertEquals(
        List.of(ORIGINS_REF_START, ORIGINS_REF_END),
        morsel.getNugget(HD_ORIGINS).sourcePack().get().sourceNos());
  }
  
  @Test
  public void testTwoLogsNotarized() throws Exception {
    final var label = new Object() {  };
    
    printMethod(label);
    MorselBuilder morsel = prepare(label, false);
    
    mockWitnessLogs(morsel, true);
  }
  
  
  
  
  @Test
  public void testTwoLogsReferenced() throws Exception {
    final var label = new Object() {  };

    printMethod(label);
    MorselBuilder morsel = prepare(label, false);
    
    addReferences(morsel, true);
  }
  
  @Test
  public void testTwoLogsNotarizedReferenced() throws Exception {
    final var label = new Object() {  };

    MorselBuilder morsel = prepare(label, false);
    mockWitnessLogs(morsel, false);
    addReferences(morsel, false);
  }

  
  private void printMethod(Object label) {
    System.out.println();
    System.out.println("  = = =  " + method(label) + "  = = =");
  }
  
  
  private static void printMethod(IoTestCase test, Object label) {
    System.out.println();
    System.out.println("  = = =  " + test.method(label) + "  = = =");
    
  }
  

  private MorselBuilder prepare(Object label, boolean print) throws Exception {
    return prepare(this, label, print);
  }
  
  static MorselBuilder prepare(IoTestCase test, Object label, boolean print) throws Exception {
    if (print)
      printMethod(test, label);
    
    File dir = test.getMethodOutputFilepath(label);
    File hd = copyResource(dir, HD);
    File hdOrigins = copyResource(dir, HD_ORIGINS);
    
    LogLedger hdLog, hdOriginsLog;
    {
      Grammar grammar = Grammar.DEFAULT.skipBlankLines(false);
      
      hdLog = LogLedger.initSalt(hd, grammar);
      hdOriginsLog = LogLedger.initSalt(hdOrigins, grammar);
    }
    
    assertEquals(HD_LINECOUNT, hdLog.buildSkipLedger());
    assertEquals(HD_ORIGINS_LINECOUNT, hdOriginsLog.buildSkipLedger());
    
    MorselBuilder morsel = new MorselBuilder();
    
    // note: nugget paths are set in one pass;
    // multipaths are tested elsewhere (eg NuggetBuilderTest)
    
    try (var closer = new TaskStack()) {
      
      var hdSldg = hdLog.loadSkipLedger().get();
      closer.pushClose(hdSldg);
      
      LedgerId hdId =
          morsel.declareLog(
              HD, hdSldg.statePath(), null, "Modern Humpty Dumpty");
      
      var hdSource = hdLog.loadSourceIndex().get();
      closer.pushClose(hdSource);
      
      morsel.initSourcePack(hdId, SaltScheme.SALT_ALL);
      assertTrue(morsel.addSourceRow(hdId, hdSource.sourceRow(HD_LINECOUNT)));
      assertFalse(morsel.addSourceRow(hdId, hdSource.sourceRow(HD_LINECOUNT)));
      assertTrue(morsel.addSourceRow(hdId, hdSource.sourceRow(1L)));
      
      var originsSldg = hdOriginsLog.loadSkipLedger().get();
      closer.pushClose(originsSldg);
      
      LedgerId originsId = 
          morsel.declareLog(
              HD_ORIGINS,
              originsSldg.getPath(
                  1L, ORIGINS_REF_START, ORIGINS_REF_END, HD_ORIGINS_LINECOUNT),
              new URI("https://en.wikipedia.org/wiki/Humpty_Dumpty"),
              "Historical account of Humpty Dumpty from wikipedia.org");
      
      var originsSource = hdOriginsLog.loadSourceIndex().get();
      closer.pushClose(originsSource);
      
      

      morsel.initSourcePack(originsId, SaltScheme.SALT_ALL);
      
      try {
        morsel.addSourceRow(originsId, hdSource.sourceRow(4L));
        fail();
      } catch (HashConflictException expected) {
        if (print)
          printExpected(expected);
      }
      assertTrue(morsel.addSourceRow(originsId, originsSource.sourceRow(28L)));
      assertTrue(morsel.addSourceRow(originsId, originsSource.sourceRow(31L)));
    }
    
    return morsel;
  }
  
  
  
  static void addReferences(MorselBuilder morsel, boolean print) {
    LedgerId hd = morsel.idByAlias(HD);
    LedgerId origins = morsel.idByAlias(HD_ORIGINS);
    assertTrue(morsel.addReference(
        hd, origins, new Reference(1L, ORIGINS_REF_START)));
    assertFalse(morsel.addReference(
        hd, origins, new Reference(1L, ORIGINS_REF_START)));
    
    addBadReference(morsel, new Reference(1L, ORIGINS_REF_END), print);
    addBadReference(morsel, new Reference(2L, ORIGINS_REF_START), print);
    addBadReference(morsel, new Reference(HD_LINECOUNT, ORIGINS_REF_START), print);
    addBadReference(morsel, new Reference(HD_LINECOUNT, ORIGINS_REF_START), print);

    assertTrue(morsel.addReference(
        hd, origins, new Reference(HD_LINECOUNT, 3, ORIGINS_REF_END, 3)));
  }
  
  
  private static void addBadReference(
      MorselBuilder morsel, Reference ref, boolean print) {
    
    LedgerId hd = morsel.idByAlias(HD);
    LedgerId origins = morsel.idByAlias(HD_ORIGINS);
    try {
      morsel.addReference(hd, origins, ref);
      fail();
    } catch (Exception expected) {
      if (print)
        printExpected(expected);
    }
  }
  
  
  static void mockWitnessLogs(MorselBuilder morsel, boolean print) {

    TimeBinner binner = TimeBinner.HALF_MINUTE;
    
    
    
    CargoProof originsWitProof =
        mockCargoProofForRowHash(
            3, 11,
            TEST_EPOCH,
            morsel.getNugget(HD_ORIGINS).paths().rowHash(HD_ORIGINS_LINECOUNT));
    
    CargoProof hdWitProof =
        mockCargoProofForRowHash(
            18, 27,
            TEST_EPOCH + 15_066L * binner.duration(),
            morsel.getNugget(HD).paths().rowHash(HD_LINECOUNT));
        
    
    BlockProof blockProof = mockBlockProof(
        binner, 12_000, 13_000, originsWitProof, hdWitProof);
    
    LedgerId timchain =
        morsel.declareTimechain("MTC", blockProof, null, "Mock timechain");
    
    assertTrue(
        morsel.addNotarizedRow(
            morsel.idByAlias(HD),
            timchain,
            new NotarizedRow(HD_LINECOUNT, hdWitProof)));
    
    try {
      morsel.addNotarizedRow(
          morsel.idByAlias(HD_ORIGINS),
          timchain,
          new NotarizedRow(HD_ORIGINS_LINECOUNT, hdWitProof));
      fail();
    } catch (HashConflictException expected) {
      if (print)
        printExpected(expected);
    }
    
    assertTrue(
        morsel.addNotarizedRow(
            morsel.idByAlias(HD_ORIGINS),
            timchain,
            new NotarizedRow(HD_ORIGINS_LINECOUNT, originsWitProof)));
    
    
  }
  
  
  
  static BlockProof mockBlockProof(
      TimeBinner binner, int preBlocks, int postBlocks, CargoProof... cargoProofs) {
    
    if (preBlocks < 0)
      throw new IllegalArgumentException("preBlocks " + preBlocks);
    if (postBlocks < 0)
      throw new IllegalArgumentException("postBlocks " + postBlocks);
    
    Arrays.sort(cargoProofs, (a, b) -> Long.compare(a.crum().utc(), b.crum().utc()));
    
    ChainParams params = ChainParams.forStartUtc(
        binner,
        cargoProofs[0].crum().utc() - preBlocks * binner.duration());
    
    Random rand = new Random(params.inceptionUtc());
    
    
    SkipLedger chain = SkipLedgers.inMemoryInstance();
    
    byte[] mockHash = new byte[SldgConstants.HASH_WIDTH];
    ByteBuffer inputHash = ByteBuffer.wrap(mockHash);
    

    ArrayList<Long> pathNos = new ArrayList<>();
    
    for (var cargo : cargoProofs) {
      final long blockNo = params.blockNoForUtc(cargo.crum().utc());
      pathNos.add(blockNo);
      assertTrue(blockNo > chain.size(), "multiple cargoProofs for block " + blockNo);
      while (chain.size() < blockNo - 1L) {
        rand.nextBytes(mockHash);
        chain.appendRows(inputHash.clear());
      }
      chain.appendRows(ByteBuffer.wrap(cargo.rootHash()));
    }
    for (int countdown = postBlocks; countdown-- > 0; ) {
      rand.nextBytes(mockHash);
      chain.appendRows(inputHash.clear());
    }
    
    if (pathNos.get(0) != 1L)
      pathNos.add(0, 1L);
    if (pathNos.getLast() != chain.size())
      pathNos.add(chain.size());
    
    return new BlockProof(params, chain.getPath(pathNos));
  }

}




















