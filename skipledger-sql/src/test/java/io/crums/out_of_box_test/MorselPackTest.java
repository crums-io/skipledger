/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.out_of_box_test;


import static io.crums.out_of_box_test.PathTest.newLedger;
import static io.crums.out_of_box_test.PathTest.newRandomLedger;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Random;

import  org.junit.jupiter.api.Test;

import com.gnahraf.test.IoTestCase;

import io.crums.model.Crum;
import io.crums.model.CrumTrail;
import io.crums.model.HashUtc;
import io.crums.sldg.MorselFile;
import io.crums.sldg.Path;
import io.crums.sldg.PathInfo;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.SldgConstants;
import io.crums.sldg.bags.MorselBag;
import io.crums.sldg.packs.MorselPack;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.src.ColumnInfo;
import io.crums.sldg.src.SourceInfo;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.src.TableSalt;
import io.crums.util.hash.Digests;
import io.crums.util.mrkl.Builder;
import io.crums.util.mrkl.FixedLeafBuilder;
import io.crums.util.mrkl.Proof;
import io.crums.util.mrkl.Tree;

/**
 * <p>Effectively an integration test of the various XxxPackBuilders.</p>
 * 
 * TODO: needs way more coverage. Taking lazy approach, during active
 * development
 */
public class MorselPackTest extends IoTestCase {
  
  
  private final static long MIN_UTC = HashUtc.INCEPTION_UTC + 100_000;
  
  
  
  @Test
  public void testInitState() {
    int initSize = 1000;
    SkipLedger ledger = newRandomLedger(initSize);
    assertEquals(initSize, ledger.size());
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.initPath(ledger.statePath(), "comments go here");
    
    Path expected = ledger.statePath();
    assertInBag(expected, builder);
    
    assertEquals(1, builder.declaredPaths().size());
    
    PathInfo expectedDecl = builder.declaredPaths().get(0);
    assertEquals(expected.loRowNumber(), expectedDecl.lo());
    assertEquals(expected.hiRowNumber(), expectedDecl.hi());
    
    MorselPack pack = toPack(builder);
    
    assertInBag(expected, pack);
    assertDeclaredPaths(builder.declaredPaths(), pack);
  }
  
  
  
  
  @Test
  public void testWithCrumTrails() {
    final int initSize = 2000;
    
    SkipLedger ledger = newRandomLedger(initSize);
    MorselPackBuilder builder = new MorselPackBuilder();
    Path state = ledger.statePath();
    final String comment = "woohoo";
    
    builder.initPath(state, comment);
    
    
    
    // add a valid crumtrail
    final long witRn = initSize;  // (last rn in state path)
    
    CrumTrail trail = mockCrumTrail(builder, witRn);
    assertTrue( builder.addTrail(witRn, trail) );
    assertFalse( builder.addTrail(witRn, trail) );
    
    assertInBag(trail, witRn, builder);
    MorselPack pack = toPack(builder);
    assertInBag(trail, witRn, pack);
    
    assertStateDeclaration(ledger, pack, comment);
    
    List<Long> rns = state.rowNumbers();
    
    // now add another valid trail
    
    final long witRn2 = rns.get(rns.size() - 2);
    
    CrumTrail trail2 = mockCrumTrail(builder, witRn2);
    assertTrue( builder.addTrail(witRn2, trail2) );
    
    MorselPack pack2 = toPack(builder);
    assertInBag(trail2, witRn2, pack2);
    assertInBag(trail, witRn, pack2);
    assertStateDeclaration(ledger, pack2, comment);
    
  }
  
  
  
  @SuppressWarnings("deprecation")
  @Test
  public void testWithSources_1() throws IOException {

    final Object label = new Object() { };

    final int finalSize = 2021;
    
    final String comment = "yes we can";
    
    int[] srcRns = {
        1574,
        1580
    };
    String[] col1 = {
        "this is a test",
        "this is _only_ a test",
    };
    
    Number[] col2 = { 23, null };
    
    // prepare the ledger..
    
    SkipLedger ledger = newLedger();
    Random random = new Random(finalSize);
    
    SourceRow[] srcs = new SourceRow[srcRns.length];
    byte[] randHash = new byte[SldgConstants.HASH_WIDTH];
    
    for (int index = 0; index < srcRns.length; ++index) {
      
      final long rn = srcRns[index];
      
      while (ledger.size() + 1 < rn) {
        random.nextBytes(randHash);
        ledger.appendRows(ByteBuffer.wrap(randHash));
      }
      assertTrue( ledger.size() == rn - 1) ;
      srcs[index] = new SourceRow(rn, col1[index], col2[index]);
      ledger.appendRows(srcs[index].rowHash());
    }
    while (ledger.size() < finalSize) {
      random.nextBytes(randHash);
      ledger.appendRows(ByteBuffer.wrap(randHash));
    }
    
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.initPath(ledger.statePath(), comment);
    builder.addPathToTarget(srcs[0].rowNumber(), ledger);
    
    MorselPack pack = toPack(builder);

    assertInBag(ledger.skipPath(srcs[0].rowNumber(), builder.hi()), pack);
    assertStateDeclaration(ledger, pack, comment);
    
    builder.addSourceRow(srcs[0]);
    
    assertInBag(srcs[0], builder);
    
    pack = toPack(builder);
    assertInBag(srcs[0], pack);
    
    long witRow = ((srcs[0].rowNumber() + 1) / 16) * 16;
    builder.addPathToTarget(witRow, ledger);
    
    CrumTrail trail = mockCrumTrail(builder, witRow);
    builder.addTrail(witRow, trail);
    
    pack = toPack(builder);
    
    assertInBag(trail, witRow, pack);
    assertInBag(srcs[0], builder);
    assertStateDeclaration(ledger, pack, comment);
    
    File mFile = getMethodOutputFilepath(label);
    
    MorselFile.createMorselFile(mFile, builder);
    
    MorselFile morselFile = new MorselFile(mFile);
    
    pack = morselFile.getMorselPack();
    assertInBag(trail, witRow, pack);
    assertInBag(srcs[0], builder);
    assertStateDeclaration(ledger, pack, comment);
    
  }
  
  
  @SuppressWarnings("deprecation")
  @Test
  public void testWithSources_2() throws IOException {

    final Object label = new Object() { };

    final int finalSize = 2021;
    
    final String comment = "this is not a real comment";
    
    int[] srcRns = {
        1574,
        1580
    };
    String[] col1 = {
        "this is a test",
        "this is _only_ a test",
    };
    
    Number[] col2 = { 23, null };
    
    // prepare the ledger..
    
    SkipLedger ledger = newLedger();
    Random random = new Random(finalSize);
    
    SourceRow[] srcs = new SourceRow[srcRns.length];
    byte[] randHash = new byte[SldgConstants.HASH_WIDTH];
    
    for (int index = 0; index < srcRns.length; ++index) {
      
      final long rn = srcRns[index];
      
      while (ledger.size() + 1 < rn) {
        random.nextBytes(randHash);
        ledger.appendRows(ByteBuffer.wrap(randHash));
      }
      assertTrue( ledger.size() == rn - 1) ;
      srcs[index] = new SourceRow(rn, col1[index], col2[index]);
      ledger.appendRows(srcs[index].rowHash());
    }
    while (ledger.size() < finalSize) {
      random.nextBytes(randHash);
      ledger.appendRows(ByteBuffer.wrap(randHash));
    }
    
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.initPath(ledger.statePath(), comment);
    for (var src : srcs) {
      builder.addPathToTarget(src.rowNumber(), ledger);
      builder.addSourceRow(src);
    }
    
    long witRow = ((srcs[0].rowNumber() + 1) / 16) * 16;
    builder.addPathToTarget(witRow, ledger);
    
    CrumTrail trail = mockCrumTrail(builder, witRow);
    builder.addTrail(witRow, trail);
    
    for (var src : srcs)
      assertInBag(src, builder);
    
    File mFile = getMethodOutputFilepath(label);
    
    MorselFile.createMorselFile(mFile, builder);
    
    MorselFile morselFile = new MorselFile(mFile);
    
    MorselPack pack = morselFile.getMorselPack();

    
    assertInBag(ledger.skipPath(srcs[0].rowNumber(), builder.hi()), pack);
    assertStateDeclaration(ledger, pack, comment);
    

    for (var src : srcs)
      assertInBag(src, pack);
    
    assertInBag(trail, witRow, pack);
    
  }
  
  

  
  
  private TableSalt randomShaker(long init) {

    Random random = new Random(init);
    byte[] seed = new byte[SldgConstants.HASH_WIDTH];
    random.nextBytes(seed);
    return new TableSalt(seed);
  }
  
  
  @Test
  public void testWithSources_3_Salty() throws IOException {

    final Object label = new Object() { };

    final int finalSize = 2021;
    
    final String comment = "this is not a real comment";
    
    final TableSalt shaker = randomShaker(101);
    
    int[] srcRns = {
        1574,
        1580
    };
    String[] col1 = {
        "this is a test",
        "this is _only_ a test",
    };
    
    Number[] col2 = { 23, null };
    
    // prepare the ledger..
    
    SkipLedger ledger = newLedger();
    Random random = new Random(finalSize);
    
    SourceRow[] srcs = new SourceRow[srcRns.length];
    byte[] randHash = new byte[SldgConstants.HASH_WIDTH];
    
    for (int index = 0; index < srcRns.length; ++index) {
      
      final long rn = srcRns[index];
      
      while (ledger.size() + 1 < rn) {
        random.nextBytes(randHash);
        ledger.appendRows(ByteBuffer.wrap(randHash));
      }
      assertTrue( ledger.size() == rn - 1) ;
      srcs[index] = SourceRow.newSaltedInstance(rn, shaker, col1[index], col2[index]);
      ledger.appendRows(srcs[index].rowHash());
    }
    while (ledger.size() < finalSize) {
      random.nextBytes(randHash);
      ledger.appendRows(ByteBuffer.wrap(randHash));
    }
    
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.initPath(ledger.statePath(), comment);
    for (var src : srcs) {
      builder.addPathToTarget(src.rowNumber(), ledger);
      builder.addSourceRow(src);
    }
    
    long witRow = ((srcs[0].rowNumber() + 1) / 16) * 16;
    builder.addPathToTarget(witRow, ledger);
    
    CrumTrail trail = mockCrumTrail(builder, witRow);
    builder.addTrail(witRow, trail);
    
    for (var src : srcs)
      assertInBag(src, builder);
    
    File mFile = getMethodOutputFilepath(label);
    
    MorselFile.createMorselFile(mFile, builder);
    
    MorselFile morselFile = new MorselFile(mFile);
    
    MorselPack pack = morselFile.getMorselPack();

    
    assertInBag(ledger.skipPath(srcs[0].rowNumber(), builder.hi()), pack);
    assertStateDeclaration(ledger, pack, comment);
    

    for (var src : srcs)
      assertInBag(src, pack);
    
    assertInBag(trail, witRow, pack);
    
  }
  
  
  @Test
  public void testWithSourceInfo() throws IOException {

    final Object label = new Object() { };

    final int finalSize = 2021;
    
    final String comment = "this is not a real comment";
    
    final TableSalt shaker = randomShaker(101);
    
    int[] srcRns = {
        1574,
        1580
    };
    String[] col1 = {
        "this is a test",
        "this is _only_ a test",
    };
    
    SourceInfo srcInfo;
    {
      ColumnInfo[] cols = {
          new ColumnInfo("Title", 1, "A smidgeon of description", "words"),
          new ColumnInfo("num_val", 2),
      };
      srcInfo = new SourceInfo("Test Ledger", "not much to say.. ", List.of(cols));
    }
    
    Number[] col2 = { 23, null };
    
    // prepare the ledger..
    
    SkipLedger ledger = newLedger();
    Random random = new Random(finalSize);
    
    SourceRow[] srcs = new SourceRow[srcRns.length];
    byte[] randHash = new byte[SldgConstants.HASH_WIDTH];
    
    for (int index = 0; index < srcRns.length; ++index) {
      
      final long rn = srcRns[index];
      
      while (ledger.size() + 1 < rn) {
        random.nextBytes(randHash);
        ledger.appendRows(ByteBuffer.wrap(randHash));
      }
      assertTrue( ledger.size() == rn - 1) ;
      srcs[index] = SourceRow.newSaltedInstance(rn, shaker, col1[index], col2[index]);
      ledger.appendRows(srcs[index].rowHash());
    }
    while (ledger.size() < finalSize) {
      random.nextBytes(randHash);
      ledger.appendRows(ByteBuffer.wrap(randHash));
    }
    
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.initPath(ledger.statePath(), comment);
    builder.setMetaPack(srcInfo);
    for (var src : srcs) {
      builder.addPathToTarget(src.rowNumber(), ledger);
      builder.addSourceRow(src);
    }
    
    long witRow = ((srcs[0].rowNumber() + 1) / 16) * 16;
    builder.addPathToTarget(witRow, ledger);
    
    CrumTrail trail = mockCrumTrail(builder, witRow);
    builder.addTrail(witRow, trail);
    
    for (var src : srcs)
      assertInBag(src, builder);
    
    File mFile = getMethodOutputFilepath(label);
    
    MorselFile.createMorselFile(mFile, builder);
    
    MorselFile morselFile = new MorselFile(mFile);
    
    MorselPack pack = morselFile.getMorselPack();

    
    assertInBag(ledger.skipPath(srcs[0].rowNumber(), builder.hi()), pack);
    assertStateDeclaration(ledger, pack, comment);
    

    for (var src : srcs)
      assertInBag(src, pack);
    
    assertInBag(trail, witRow, pack);
    
    var metaPack = pack.getMetaPack();
    assertTrue(metaPack.getSourceInfo().isPresent());
    
    var outSrcInfo = metaPack.getSourceInfo().get();
    
    assertEquals(srcInfo.getName(), outSrcInfo.getName());
    assertEquals(srcInfo.getDescription(), outSrcInfo.getDescription());
    var expectedCols = srcInfo.getColumnInfos();
    var outCols = outSrcInfo.getColumnInfos();
    assertEquals(srcInfo.getColumnInfoCount(), outSrcInfo.getColumnInfoCount());
    assertEquals(expectedCols.size(), outSrcInfo.getColumnInfoCount());
    for (int index = expectedCols.size(); index-- > 0; )
      assertEqual(expectedCols.get(index), outCols.get(index));
  }
  
  

  public static void assertEqual(ColumnInfo expected, ColumnInfo readBack) {
    assertEquals(expected.getName(), readBack.getName());
    assertEquals(expected.getColumnNumber(), readBack.getColumnNumber());
    assertEquals(expected.getDescription(), readBack.getDescription());
    assertEquals(expected.getUnits(), readBack.getUnits());
  }
  

  
  private MorselPack toPack(MorselPackBuilder builder) {
    ByteBuffer serialForm = builder.serialize();
    return MorselPack.load(serialForm);
  }
  
  
  
  
  
  
  
  
  
  
  
  
  /**
   * 10 minutes between rows.
   */
  private static long mockBeaconUtc(long rowNumber) {
    return MIN_UTC + rowNumber * 600_000;   // 10 minutes between rows :\
  }
  
  /**
   * 10 minutes per row plus 4 minutes and 2 minutes fuzz
   */
  public static long mockTrailUtc(long rowNumber) {
    Random random = new Random(rowNumber);
    int delta = 4 * 60_000 + random.nextInt(120_000); // 4 minutes + 2 minutes random fuzz
    return mockBeaconUtc(rowNumber) + delta;
  }
  
  
  public static CrumTrail mockCrumTrail(MorselBag bag, long rowNumber) {
    long utc = mockTrailUtc(rowNumber);
    return mockCrumTrail(bag, rowNumber, utc);
  }
  
  
  public static CrumTrail mockCrumTrail(MorselBag bag, long rowNumber, long utc) {
    byte[] hash;
    {
      ByteBuffer rowHash = bag.rowHash(rowNumber);
      hash = new byte[rowHash.remaining()];
      rowHash.duplicate().get(hash);
    }
    
    Random random = new Random(rowNumber);
    int leafCount = 1000 - random.nextInt(300);
    int leafIndex = leafCount - random.nextInt(leafCount/3) - 2;
    return mockTrail(hash, utc, leafIndex, leafCount);
  }
  
  
  public static void assertStateDeclaration(SkipLedger expected, MorselBag bag, String comment) {
    PathInfo decl = new PathInfo(1, expected.size(), comment);
    assertTrue(bag.declaredPaths().contains(decl));
    assertInBag(expected.statePath(), bag);
  }
  
  
  public static void assertDeclaredPaths(List<PathInfo> expected, MorselBag bag) {
    assertEquals(expected, bag.declaredPaths());
  }
  
  
  public static void assertInBag(SourceRow src, MorselBag bag) {
    assertTrue(bag.sources().contains(src));
  }
  
  
  public static void assertInBag(Path expected, MorselBag bag) {
    expected.rows().forEach(r -> assertEquals(r, bag.getRow(r.rowNumber())));
  }
  
  
  public static void assertInBag(CrumTrail expected, long rowNumber, MorselBag bag) {
    CrumTrail actual = bag.crumTrail(rowNumber);
    assertEquals(expected, actual);
  }
  
  
  

  
//  public static Ledger newRandomLedger(int initSize) {
//    Ledger ledger = Ledgers.newVolatileLedger();
//    Random random = new Random(initSize);
//    byte[] randHash = new byte[Constants.HASH_WIDTH];
//    for (int countdown = initSize; countdown-- > 0; ) {
//      random.nextBytes(randHash);
//      ledger.appendRows(ByteBuffer.wrap(randHash));
//    }
//    return ledger;
//  }
  
  public static CrumTrail mockTrail(ByteBuffer hash, long utc, int index, int leafCount) {
    byte[] array = new byte[hash.remaining()];
    hash.get(array);
    return mockTrail(array, utc, index, leafCount);
  }
  
  
  public static CrumTrail mockTrail(byte[] hash, long utc, int index, int leafCount) {
    Random random = new Random(leafCount + 37 * index);
    
    
    Crum crum = new Crum(hash, utc);

    MessageDigest digest = Digests.SHA_256.newDigest();
    digest.update(crum.serialForm());
    
    byte[] hashOfCrum = digest.digest();

    Builder builder = new FixedLeafBuilder(Digests.SHA_256.hashAlgo());
    
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
    return new CrumTrail(proof, crum);
  }

}
