/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg;


import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.gnahraf.test.SelfAwareTestCase;

import io.crums.sldg.db.VolatileTable;
import io.crums.util.Tuple;

/**
 * 
 */
public class PathTest extends SelfAwareTestCase {
  
  
  
  @Test
  public void testSerializeAndMinimal() throws Exception {
    final int rows = 79;
    long[] rowNumbers = { 11 };
    testSerializeAndLoad(rows, rowNumbers);
  }
  
  
  @Test
  public void testSerializeAndLoad() throws Exception {
    final int rows = 13;
    long[] rowNumbers = { 7, 8, 10, 11 };
    testSerializeAndLoad(rows, rowNumbers);
  }
  
  
  @Test
  public void testSerializeAndLoadTargeted() throws Exception {
    final int rows = 13;
    long[] rowNumbers = { 7, 8, 10, 11 };
    long target = 8;
    testSerializeAndLoad(rows, rowNumbers, target);
  }
  
  
  @Test
  public void testSerializeAndLoadTargeted2() throws Exception {
    final int rows = 13;
    long[] rowNumbers = { 7, 8, 10, 11 };
    long target = 11;
    testSerializeAndLoad(rows, rowNumbers, target);
  }

  @Test
  public void testSerializeAndLoadSkipPathWithBeacons() throws Exception {
    final int rows = 10_003;
    long lo = 107;
    long hi = 10_001;
    long[] beacons = { 128, 512, 1024, 2048 };
    String method = method(new Object() {  });
    System.out.print("[" + method + "]:");
    testSerializeAndLoadSkipPath(rows, lo, hi, beacons);
    System.out.println(" [DONE]");
  }

  @Test
  public void testSkipPathWithBeaconsTargeted() throws Exception {
    final int rows = 10_003;
    long lo = 107;
    long hi = 10_001;
    long target = 108;
    long[] beacons = { 128, 512, 1024, 2048 };
    String method = method(new Object() {  });
    System.out.print("[" + method + "]:");
    testSerializeAndLoadSkipPath(rows, lo, target, hi, beacons);
    System.out.println(" [DONE]");
  }
  
  
  @Test
  public void testSerializeAndLoadSkipPath() throws Exception {
    final int rows = 10_003;
    long lo = 107;
    long hi = 10_001;
    testSerializeAndLoadSkipPath(rows, lo, hi);
  }
  
  
  @Test
  public void testConcatAdjacent() throws Exception {
    
    final int rows = 100_003;
    
    final long lo = 58;
    final long mid = 45_032;
    final long hi = 87_196;
    
    Ledger ledger = newRandomLedger(rows);
    
    Path loPath = ledger.skipPath(lo, mid);
    Path hiPath = ledger.skipPath(mid, hi);
    
    Path concat = loPath.concat(hiPath);
    assertEquals(lo, concat.loRowNumber());
    assertEquals(hi, concat.hiRowNumber());
    
    assertEquals(
        loPath.rows().size() + hiPath.rows().size() - 1,
        concat.rows().size());
    
    loPath = ledger.skipPath(lo, mid - 1);
    concat = loPath.concat(hiPath);
    assertEquals(lo, concat.loRowNumber());
    assertEquals(hi, concat.hiRowNumber());
    
    assertEquals(
        loPath.rows().size() + hiPath.rows().size(),
        concat.rows().size());
  }
  
  
  @Test
  public void testBogusConcatAdjacent() throws Exception {
    
    final int rows = 100_003;
    
    final long lo = 58;
    final long mid = 45_032;
    final long hi = 87_196;
    
    Ledger ledger = newRandomLedger(rows);
    Ledger ledger2 = newRandomLedger(rows + 1);
    
    Path loPath = ledger.skipPath(lo, mid);
    Path hiPath = ledger2.skipPath(mid, hi);
    
    try {
      loPath.concat(hiPath);
      fail();
    } catch (IllegalArgumentException expected) {
      System.out.println("[EXPECTED ERROR]: " + expected);
    }
  }
  
  
  @Test
  public void testCamelPath() throws Exception {
    final int rows = 10_003;
    long lo = 107;
    
    long target = 111;
    long hi = 10_001;
    long[] beacons = {  };
    testSerializeAndLoadCamelPath(rows, lo, target, hi, beacons);
    
  }
  
  
  @Test
  public void testCamelPathWithBeacons() throws Exception {
    final int rows = 10_003;
    long lo = 107;
    
    long target = 111;
    long hi = 10_001;
    long[] beacons = { lo, target + 1, hi - 1 };

    String method = method(new Object() {  });
    System.out.print("[" + method + "]:");
    testSerializeAndLoadCamelPath(rows, lo, target, hi, beacons);
    System.out.println(" [DONE]");
    
  }
  
  
  private void testSerializeAndLoad(int size, long[] pathNumbers) throws Exception {
    testSerializeAndLoad(size, pathNumbers, pathNumbers[0]);
  }
  
  
  private void testSerializeAndLoad(int size, long[] pathNumbers, long target) throws Exception {

    Ledger ledger = newRandomLedger(size);
    
    ArrayList<Row> rows = new ArrayList<>();
    
    for (long n : pathNumbers)
      rows.add(ledger.getRow(n));
    
    boolean targeted = target != pathNumbers[0];
    Path path = targeted ? new TargetPath(rows, target) : new Path(rows);
    
    ByteBuffer buffer = path.serialize();
    
    int flack = 7;  // make sure it's self-delimiting
    byte[] serialized = new byte[buffer.remaining() + flack];
    buffer.get(serialized, 0, buffer.remaining());
    
    InputStream in = new ByteArrayInputStream(serialized);
    
    Path out = Path.load(in);
    
    assertEquals(path.rows(), out.rows());
    
    assertEquals(target, path.target().rowNumber());
    
    in.close();
    ledger.close();
  }
  
  
  private void testSerializeAndLoadSkipPath(int size, long lo, long hi) throws Exception {
    long[] b = { };
    testSerializeAndLoadSkipPath(size, lo, hi, b);
  }
  
  private void testSerializeAndLoadSkipPath(int size, long lo, long hi, long[] beacons) throws Exception {
    testSerializeAndLoadSkipPath(size, lo, lo, hi, beacons);
  }
  
  private void testSerializeAndLoadSkipPath(int size, long lo, long target, long hi, long[] beacons) throws Exception {

    Ledger ledger = newRandomLedger(size);
    
    
    ArrayList<Row> rows = new ArrayList<>();
    
    for (long n : Ledger.skipPathNumbers(lo, hi))
      rows.add(ledger.getRow(n));
    

    final long baseUtc = System.currentTimeMillis();
    
    
    final long delta = 5000;
    
    List<Tuple<Long,Long>> beaconTuples;
    if (beacons == null || beacons.length == 0)
      beaconTuples = Collections.emptyList();
    else {
      beaconTuples = new ArrayList<>(beacons.length);
      long utc = baseUtc;
      for (long beacon : beacons)
        beaconTuples.add(new Tuple<>(beacon, utc += delta));
    }
    
    if (!beaconTuples.isEmpty())
      System.out.print(" baseUtc: " + baseUtc);
    
    Path path = new Path(rows, beaconTuples);
    
    if (target != lo)
      path = new TargetPath(path, target);
    
    ByteBuffer buffer = path.serialize();
    
    int flack = 7;  // make sure it's self-delimiting
    byte[] serialized = new byte[buffer.remaining() + flack];
    buffer.get(serialized, 0, buffer.remaining());
    
    InputStream in = new ByteArrayInputStream(serialized);
    
    Path out = Path.load(in);
    
    List<Tuple<Row, Long>> bcnRows = out.beaconRows();
    
    assertEquals(beaconTuples.size(), bcnRows.size());
    for (int index = 0; index < bcnRows.size(); ++index) {
      Tuple<Long,Long> expected = beaconTuples.get(index);
      assertEquals(expected.a.longValue(), bcnRows.get(index).a.rowNumber());
      assertEquals(expected.b, bcnRows.get(index).b);
    }
    
    assertEquals(path.rows(), out.rows());
    assertTrue(out.isSkipPath());
    // following design change..
//    assertTrue(out instanceof SkipPath);
    if (target == lo)
      assertEquals(out.first(), out.target());
    
    assertEquals(target, out.target().rowNumber());
    
    in.close();
    ledger.close();
  }
  
  
  

  
  private void testSerializeAndLoadCamelPath(int size, long lo, long target, long hi, long[] beacons) throws Exception {

    Ledger ledger = newRandomLedger(size);
    
    
    ArrayList<Row> rows = new ArrayList<>();
    
    for (long n : Ledger.skipPathNumbers(lo, hi))
      rows.add(ledger.getRow(n));
    

    final long baseUtc = System.currentTimeMillis();
    
    
    final long delta = 5000;
    
    List<Tuple<Long,Long>> beaconTuples;
    if (beacons == null || beacons.length == 0)
      beaconTuples = Collections.emptyList();
    else {
      beaconTuples = new ArrayList<>(beacons.length);
      long utc = baseUtc;
      for (long beacon : beacons)
        beaconTuples.add(new Tuple<>(beacon, utc += delta));
    }
    
    
    
    SkipPath head = ledger.skipPath(lo, target);
    SkipPath tail = ledger.skipPath(target, hi);

    if (!beaconTuples.isEmpty()) {
      System.out.print(" baseUtc: " + baseUtc);
      int beaconSplitIndex = Arrays.binarySearch(beacons, target);
      if (beaconSplitIndex < 0)
        beaconSplitIndex = -1 - beaconSplitIndex;
      
      head = new SkipPath(head, beaconTuples.subList(0, beaconSplitIndex));
      tail = new SkipPath(tail, beaconTuples.subList(beaconSplitIndex, beaconTuples.size()));
    }
    
    CamelPath path = CamelPath.concatInstance(head, tail);
//    Path path = new Path(rows, beaconTuples);
    
    ByteBuffer buffer = path.serialize();
    
    int flack = 7;  // make sure it's self-delimiting
    byte[] serialized = new byte[buffer.remaining() + flack];
    buffer.get(serialized, 0, buffer.remaining());
    
    InputStream in = new ByteArrayInputStream(serialized);
    
    Path out = Path.load(in);
    
    List<Tuple<Row, Long>> bcnRows = out.beaconRows();
    
    assertEquals(beaconTuples.size(), bcnRows.size());
    for (int index = 0; index < bcnRows.size(); ++index) {
      Tuple<Long,Long> expected = beaconTuples.get(index);
      assertEquals(expected.a.longValue(), bcnRows.get(index).a.rowNumber());
      assertEquals(expected.b, bcnRows.get(index).b);
    }
    
    assertEquals(path.rows(), out.rows());
    // following design change..
//    assertTrue(out instanceof CamelPath);
    assertEquals(target, path.target().rowNumber());
    
    in.close();
    ledger.close();
  }
  
  
  
  public static Ledger newRandomLedger(int rows) {
    if (rows <= 0)
      throw new IllegalArgumentException("rows " + rows);
    
    Ledger ledger = new CompactLedger(new VolatileTable());
    
    Random random = new Random(rows);

    byte[] mockHash = new byte[SldgConstants.HASH_WIDTH];
    ByteBuffer mockHashBuffer = ByteBuffer.wrap(mockHash);
    
    for (int count = rows; count-- > 0; ) {
      random.nextBytes(mockHash);
      mockHashBuffer.clear();
      ledger.appendRows(mockHashBuffer);
    }
    
    assertEquals(rows, ledger.size());
    return ledger;
  }

}
