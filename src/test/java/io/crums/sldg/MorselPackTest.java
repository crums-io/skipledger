/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg;


import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import  org.junit.Test;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.model.Constants;
import io.crums.model.Crum;
import io.crums.model.CrumTrail;
import io.crums.model.HashUtc;
import io.crums.sldg.bags.MorselBag;
import io.crums.sldg.entry.Entry;
import io.crums.sldg.entry.TextEntry;
import io.crums.sldg.packs.MorselPack;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.util.Lists;
import io.crums.util.Tuple;
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
public class MorselPackTest extends SelfAwareTestCase {
  
  
  private final static long MIN_UTC = HashUtc.INCEPTION_UTC + 100_000;
  
  
  
  @Test
  public void testInitState() {
    int initSize = 1000;
    Ledger ledger = newRandomLedger(initSize);
    assertEquals(initSize, ledger.size());
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.initState(ledger);
    
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
  public void testWithBeaconRows() {
    final Object label = new Object() { };
    int initSize = 2000;
    Ledger ledger = newRandomLedger(initSize);
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.initState(ledger);
    
    Path state = ledger.statePath();
    long utc = MIN_UTC;
    long bcRn = state.rows().get(0).rowNumber();
    assertTrue( builder.addBeaconRow(bcRn, utc) );
    
    final Tuple<Long,Long> bcn = new Tuple<>(bcRn, utc);
    
    List<Tuple<Long,Long>> expBeacons = Collections.singletonList(bcn);
    assertBeaconRows(expBeacons, builder);
    
    assertStateDeclaration(ledger, builder);
    assertEquals(state.rowNumbers(), builder.getFullRowNumbers());

    
    MorselPack pack = toPack(builder);
    
    assertStateDeclaration(ledger, pack);
    assertBeaconRows(expBeacons, pack);
    assertEquals(1, pack.declaredPaths().size());
    assertEquals(state.rowNumbers(), pack.getFullRowNumbers());
    
    // add another
    
    long utc2 = utc + 90000;
    long bcRn2 = state.rows().get(4).rowNumber();
    
    final Tuple<Long,Long> bcn2 = new Tuple<>(bcRn2, utc2);
    
    builder.addBeaconRow(bcRn2, utc2);
    expBeacons = Arrays.asList(bcn, bcn2);
    
    assertBeaconRows(expBeacons, builder);
    
    pack = toPack(builder);
    
    assertStateDeclaration(ledger, pack);
    assertBeaconRows(expBeacons, pack);
    assertEquals(1, pack.declaredPaths().size());
    assertEquals(state.rowNumbers(), pack.getFullRowNumbers());
    assertBeaconRows(expBeacons, pack);

    System.out.println(method(label) + " Full row #s: " + builder.getFullRowNumbers());
    
    // test adding non-existent row as beacon
    
    {
      long utc3 = utc2 + 90000;
      long bcRn3 = bcRn2 + 1;
      assertFalse( builder.addBeaconRow(bcRn3, utc3) );
    }
    
    // test adding out-of-sequence beacon
    try {
      long utc3 = utc2 - 1;
      long bcRn3 = state.rowNumbers().get(6);
      
      builder.addBeaconRow(bcRn3, utc3);
      fail();
      
    } catch (IllegalArgumentException expected) {
      System.out.println("EXPECTED ERROR in " + method(label) + ": " + expected);
    }
    
    // but interleaving is ok, as long as it's consistent..
    
    long utcBetween = (utc + utc2) / 2;
    long bcRnBetween = state.rowNumbers().get(2);
    
    assertTrue( builder.addBeaconRow(bcRnBetween, utcBetween) );
    expBeacons = Arrays.asList(bcn, new Tuple<Long,Long>(bcRnBetween, utcBetween), bcn2);
    assertBeaconRows(expBeacons, builder);
    
    pack = toPack(builder);
    
    assertStateDeclaration(ledger, pack);
    assertBeaconRows(expBeacons, pack);
    assertEquals(1, pack.declaredPaths().size());
    assertEquals(state.rowNumbers(), pack.getFullRowNumbers());
    assertBeaconRows(expBeacons, pack);
  }
  
  
  
  
  @Test
  public void testWithCrumTrails() {
    final Object label = new Object() { };
    final int initSize = 2000;
    final int firstBcnIndex = 3;
    
    Ledger ledger = newRandomLedger(initSize);
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.initState(ledger);
    
    Path state = ledger.statePath();
    
    // mark some mock beacons
    List<Long> rns = state.rowNumbers();
    List<Tuple<Long,Long>> expBcns = new ArrayList<>();
    for (int index = firstBcnIndex; index < rns.size() - 2 ; ++index) {
      long rn = rns.get(index);
      long utc = mockBeaconUtc(rn); 
      expBcns.add(new Tuple<>(rn, utc));
      builder.addBeaconRow(rns.get(index), utc);
    }
    
    // add a valid crumtrail
    final long witRn = initSize;  // (last rn in state path)
    
    CrumTrail trail = mockCrumTrail(builder, witRn);
    assertTrue( builder.addTrail(witRn, trail) );
    assertFalse( builder.addTrail(witRn, trail) );
    
    assertInBag(trail, witRn, builder);
    MorselPack pack = toPack(builder);
    assertInBag(trail, witRn, pack);
    
    assertStateDeclaration(ledger, pack);
    
    
    assertBeaconRows(expBcns, pack);
    
    // try out-of-sequence
    try {
      long rn = builder.beaconRows().get(1).a;
      long utc = mockBeaconUtc(rn - 1);
      CrumTrail bad = mockCrumTrail(builder, rn, utc);
      
      builder.addTrail(rn, bad);
      fail();
      
    } catch (IllegalArgumentException expected) {
      System.out.println("EXPECTED ERROR in " + method(label) + ": " + expected);
    }
    
    // try out-of-sequence (2)
    try {
      long rn = rns.get(rns.size() - 2);
      long utc = mockTrailUtc(initSize + 1);
      CrumTrail bad = mockCrumTrail(builder, rn, utc);
      
      builder.addTrail(rn, bad);
      fail();
      
    } catch (IllegalArgumentException expected) {
      System.out.println("EXPECTED ERROR in " + method(label) + ": " + expected);
    }
    
    // now add another valid trail
    
    final long witRn2 = rns.get(rns.size() - 2);
    
    CrumTrail trail2 = mockCrumTrail(builder, witRn2);
    assertTrue( builder.addTrail(witRn2, trail2) );
    
    MorselPack pack2 = toPack(builder);
    assertInBag(trail2, witRn2, pack2);
    assertInBag(trail, witRn, pack2);
    assertStateDeclaration(ledger, pack2);
    
    // adding illegal beacon
    
    try {
      assertFalse(
          "test premise failure",
          Lists.map(builder.beaconRows(), t -> t.a).contains(witRn2));
      
      long badBeaconUtc = trail.crum().utc();
      builder.addBeaconRow(witRn2, badBeaconUtc);
      fail();
      
    } catch (IllegalArgumentException expected) {
      System.out.println("EXPECTED ERROR in " + method(label) + ": " + expected);
    }
  }
  
  
  
  @Test
  public void testWithEntries() {

    final int finalSize = 2021;
    
    int[] entryRns = {
        1574,
        1580
    };
    String[] entryTexts = {
        "this is a test",
        "this is _only_ a test",
    };
    
    // prepare the ledger..
    
    Ledger ledger = Ledgers.newVolatileLedger();
    Random random = new Random(finalSize);
    
    TextEntry[] entries = new TextEntry[entryRns.length];
    byte[] randHash = new byte[SldgConstants.HASH_WIDTH];
    
    for (int index = 0; index < entryRns.length; ++index) {
      final long rn = entryRns[index];
      
      while (ledger.size() + 1 < rn) {
        random.nextBytes(randHash);
        ledger.appendRows(ByteBuffer.wrap(randHash));
      }
      assertTrue( ledger.size() == rn - 1) ;
      
      entries[index] = new TextEntry(entryTexts[index], rn);
      ledger.appendRows(entries[index].hash());
    }
    while (ledger.size() < finalSize) {
      random.nextBytes(randHash);
      ledger.appendRows(ByteBuffer.wrap(randHash));
    }

    TextEntry entry = entries[0];
    
    MorselPackBuilder builder = new MorselPackBuilder();
    builder.initState(ledger);
    builder.addPathToTarget(entry.rowNumber(), ledger);
    
    MorselPack pack = toPack(builder);

    assertInBag(ledger.skipPath(entry.rowNumber(), builder.hi()), pack);
    assertStateDeclaration(ledger, pack);
    
    
    builder.addEntry(entry.rowNumber(), entry.content(), null);
    
    assertInBag(entry, builder);
    
    pack = toPack(builder);
    assertInBag(entry, pack);
    
    long bcnRow = ((entry.rowNumber() - 1) / 16) * 16;
    builder.addPathToTarget(bcnRow, ledger);
    builder.addBeaconRow(bcnRow, mockBeaconUtc(bcnRow));
    
    long witRow = ((entry.rowNumber() + 1) / 16) * 16;
    builder.addPathToTarget(witRow, ledger);
    
    CrumTrail trail = mockCrumTrail(builder, witRow);
    builder.addTrail(witRow, trail);
    
    pack = toPack(builder);
    
    assertInBag(trail, witRow, pack);
    assertInBag(entry, builder);
    assertStateDeclaration(ledger, pack);
    
  }
  
  
  
  

  
  private MorselPack toPack(MorselPackBuilder builder) {
    ByteBuffer serialForm = builder.serialize();
    return MorselPack.load(serialForm);
  }
  
  
  
  
  
  
  
  
  
  
  
  
  /**
   * 10 minutes between rows.
   */
  public static long mockBeaconUtc(long rowNumber) {
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
  
  
  public static void assertInBag(Entry expected, MorselBag bag) {
    Entry actual = bag.entry(expected.rowNumber());
    assertEquals(expected.rowNumber(), actual.rowNumber());
    assertEquals(expected.content(), actual.content());
  }
  
  
  public static void assertStateDeclaration(Ledger expected, MorselBag bag) {
    PathInfo decl = new PathInfo(1, expected.size());
    assertTrue(bag.declaredPaths().contains(decl));
    assertInBag(expected.statePath(), bag);
  }
  
  
  public static void assertDeclaredPaths(List<PathInfo> expected, MorselBag bag) {
    assertEquals(expected, bag.declaredPaths());
  }
  
  
  public static void assertBeaconRows(List<Tuple<Long,Long>> expected, MorselBag bag) {
    assertEquals(expected, bag.beaconRows());
  }
  
  
  
  public static void assertInBag(Path expected, MorselBag bag) {
    expected.rows().forEach(r -> assertEquals(r, bag.getRow(r.rowNumber())));
  }
  
  
  public static void assertInBag(CrumTrail expected, long rowNumber, MorselBag bag) {
    CrumTrail actual = bag.crumTrail(rowNumber);
    assertEquals(expected, actual);
  }

  
  public static Ledger newRandomLedger(int initSize) {
    Ledger ledger = Ledgers.newVolatileLedger();
    Random random = new Random(initSize);
    byte[] randHash = new byte[Constants.HASH_WIDTH];
    for (int countdown = initSize; countdown-- > 0; ) {
      random.nextBytes(randHash);
      ledger.appendRows(ByteBuffer.wrap(randHash));
    }
    return ledger;
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
