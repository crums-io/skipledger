/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.bindle;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import io.crums.sldg.Path;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.bindle.tc.NotarizedRow;
import io.crums.sldg.bindle.tc.NotaryPack;
import io.crums.sldg.logledger.Grammar;
import io.crums.sldg.logledger.LogLedger;
import io.crums.sldg.src.SourceRow;
import io.crums.tc.CargoProof;
import io.crums.tc.Constants;
import io.crums.tc.Crum;
import io.crums.testing.IoTestCase;
import io.crums.util.TaskStack;
import io.crums.util.mrkl.Builder;
import io.crums.util.mrkl.FixedLeafBuilder;
import io.crums.util.mrkl.Proof;
import io.crums.util.mrkl.Tree;

/**
 * 
 */
public class NuggetBuilderDumptyTest extends IoTestCase {
  
  final static String HD = "hd.text";
  final static long HD_LINECOUNT = 4L;
  final static String HD_ORIGINS = "hd-origins.text";
  final static long HD_ORIGINS_LINECOUNT = 33L;

  
  
  @Test
  public void testHdSalted() throws Exception {
    var label = new Object() {  };
    
    testHdSalted(label, true);
  }
  
  
  @Test
  public void testLazyNug() throws Exception {
    var label = new Object() {  };
    
    var builder = testHdSalted(label, false);
    
    ByteBuffer serial = builder.build().serialize();
    LazyNugget nug = LazyNugget.load(mockIdLookup(), serial);
    
    assertNugget(builder, nug);
  }
  
  
  private LedgerId[] mockIds() {
    return new LedgerId[] {
        newLogId(99, HD),
        newId(42, LedgerType.TIMECHAIN, "timechain-42"),
        newLogId(3, HD_ORIGINS)
    };
  }
  
  
  
  private Function<Integer, LedgerId> mockIdLookup() {
    LedgerId[] ids = mockIds();
    return (i) -> {
      int index = ids.length;
      while (index-- > 0 && i != ids[index].id());
      if (index == -1)
        throw new NoSuchElementException("id " + i);
      return ids[index];
    };
  }
  
  
  
  private NuggetBuilder testHdSalted(Object label, boolean print) throws Exception {
    File dir = createTestDir(label);
    File hd = copyResource(dir, HD);
    
    var mockIds = mockIds();
    
    final LedgerId id = mockIds[0];
    
    var grammar = Grammar.DEFAULT.skipBlankLines(false);
    var llg = LogLedger.initSalt(hd, grammar);
    
    assertEquals(HD_LINECOUNT, llg.buildSkipLedger());
    
    Path state;
    try (SkipLedger sldg = llg.loadSkipLedger().get()) {
      state = sldg.statePath();
    }
    
    
    var builder = new NuggetBuilder(id, state);
    var sources = llg.loadSourceIndex().get();
    SourceRow srcRow2 = sources.sourceRow(2L);
    builder.setSaltScheme(llg.rules().saltScheme());
    assertTrue(builder.addSourceRow(srcRow2));
    try {
      builder.addSourceRow(sources.sourceRow(3L));
      fail();
    } catch (IllegalArgumentException expected) {
      if (print)
        printExpected(expected);
    }
    try (SkipLedger sldg = llg.loadSkipLedger().get()) {
      long hiInterectNo = builder.addPath(sldg.getPath(3L));
      assertEquals(3L, hiInterectNo);
    }
    assertTrue(builder.addSourceRow(sources.sourceRow(3L)));
    
    sources.close();
    
    final long mockUtc = System.currentTimeMillis() - 3_600_000;
    
    CargoProof witProof =
        mockCargoProofForRowHash(
            5, 7, mockUtc, builder.paths().rowHash(3L));
    
    var notarizedRow = new NotarizedRow(3L, witProof);
    
    final LedgerId mockTimeChainId = mockIds[1];
    
    assertTrue(builder.addNotarizedRow(mockTimeChainId, notarizedRow));
    
    final LedgerId referencedId = mockIds[2];
    
    try {
      builder.addForeignRef(referencedId, new Reference(1L, 3, 28L, 3));
      fail();
    } catch (IllegalArgumentException expected) {
      if (print)
        printExpected(expected);
    }
    
    final var ref = new Reference(3L, 4, 30L, 4);
    // the truth of the matter (the foreign ref) is validated
    // at the bindle layer, not here
    assertTrue(
        builder.addForeignRef(referencedId, ref));
    
    // build it (if you inspect the internals, this is a bit silly, ofc.)
    var nugget = builder.build();
    
    var sourcePack = nugget.sourcePack().get();
    assertTrue(sourcePack.containsSource(2L));
    assertTrue(sourcePack.containsSource(3L));
    
    assertEquals(1, nugget.notaryPacks().size());
    
    var notaryPack = nugget.notaryPacks().get(0);
    assertEquals(mockTimeChainId, notaryPack.chainId());
    assertEquals(1, notaryPack.notarizedRows().size());
    
    assertEquals(1, nugget.refPacks().size());
    ForeignRefs fRefs = nugget.refPacks().get(0);
    assertEquals(referencedId, fRefs.foreignId());
    assertEquals(List.of(ref), fRefs.refs());
    
    // finally, test the serial interface..
    ByteBuffer serialBytes = nugget.serialize();
    Function<Integer, LedgerId> idLookup = mockIdLookup();
    
    ObjectNug copy = ObjectNug.load(idLookup, serialBytes);
    
    assertNugget(builder, copy);
    
    return builder;
  }
  
  
  
  static void assertNugget(Nugget expected, Nugget actual) {
    assertEquals(expected.id(), actual.id());
    if (expected.sourcePack().isEmpty())
      assertTrue(actual.sourcePack().isEmpty());
    else {
      assertTrue(actual.sourcePack().isPresent());
      var expectedSource = expected.sourcePack().get();
      var actualSource = actual.sourcePack().get();
      assertEquals(expectedSource.sources(), actualSource.sources());
    }

    assertEquals(expected.notaryPacks().size(), actual.notaryPacks().size());
    for (var expNotary : expected.notaryPacks()) {
      var expId = expNotary.chainId();
      NotaryPack actualNotary =
          actual.notaryPacks().stream()
          .filter(n -> expId.equals(n.chainId()))
          .findAny().orElseThrow(
              () -> new NoSuchElementException(
                  "expected notary id %s not found".formatted(expId)));
      assertEquals(expNotary.notarizedRows(), actualNotary.notarizedRows());
    }
    
    assertEquals(expected.refPacks().size(), actual.refPacks().size());
    
  }
  
  
  
  
  
  static void printExpected(Exception expected) {
    System.out.println("[EXPECTED]: " + expected);
  }
  
  
  
  
  static LedgerId newLogId(int id, String name) {
    return newId(id, LedgerType.LOG, name);
  }
  
  
  
  static LedgerId newId(int id, LedgerType type, String name) {
    var props = new LedgerInfo.StdProps(type, name, null, null);
    
    // note-to-self: the interface is a hassle..
    var info = new LedgerInfo(props) {
      @Override
      public int serialSize() { return 0; }
      @Override
      public ByteBuffer writeTo(ByteBuffer out) {
        return out;
      }
      @Override
      LedgerInfo edit(StdProps props) {
        return this;
      }
    };
    
    return new LedgerId(id, info);
  }
  
  
  
  
  
  static CargoProof mockCargoProofForRowHash(
      int index, int leafCount, long utc, ByteBuffer rowHash) {

    assertTrue(leafCount > 1);
    Objects.checkIndex(index, leafCount);

    Crum crum = new Crum(rowHash, utc);
    
    MessageDigest digest = Constants.DIGEST.newDigest();
    digest.update(crum.serialForm());
    
    byte[] hashOfCrum = digest.digest();
    

    byte[] hash = new byte[Constants.HASH_WIDTH];
    
    Random random = new Random(utc);
    
    Builder builder = new FixedLeafBuilder(Constants.HASH_ALGO);
    
    for (int i = 0; i < index; ++i) {
      random.nextBytes(hash);
      builder.add(hash);
    }
    builder.add(hashOfCrum);
    
    for (int i = index + 1; i < leafCount; ++i) {
      random.nextBytes(hash);
      builder.add(hash);
    }
    
    Tree tree = builder.build();
    Proof proof = new Proof(tree, index);
    return new CargoProof(proof, crum);
  }
  
  
  
  
  private File createTestDir(Object label) {
    var dir = getMethodOutputFilepath(label);
    assertTrue(dir.mkdirs());
    return dir;
  }
  
  
  static File copyResource(File dir, String resource) throws IOException {
    dir.mkdirs();
    assertTrue(dir.isDirectory());
    File dest = new File(dir, resource);
    try (var closer = new TaskStack()) {
      var in = NuggetBuilderDumptyTest.class.getResourceAsStream(resource);
      closer.pushClose(in);
      var out = new FileOutputStream(dest);
      closer.pushClose(out);
      byte[] buffer = new byte[4096];
      while (true) {
        int len = in.read(buffer);
        if (len == -1)
          break;
        out.write(buffer, 0, len);
      }
    }
    return dest;
  }

}
