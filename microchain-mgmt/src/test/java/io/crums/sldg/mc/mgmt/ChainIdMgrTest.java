/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.crums.sldg.mc.mgmt.ChainIdMgr.ChainId;
import io.crums.sldg.mc.mgmt.ChainIdMgr.InfoSansDesc;
import io.crums.util.Lists;

/**
 * 
 */
public class ChainIdMgrTest extends BaseTestCase {
  
  @Test
  public void testEnsureInstance() throws Exception {
    final Object label = new Object() { };
    
    printMessage(label, "Logging is ON (to display schema)..");
    DbEnv env = new DbEnv();
    assertTrue(env.readOnly());
    assertFalse(env.allowDelete());
    
    Connection con = newDatabase(label);
    try {
      ChainIdMgr.ensureInstance(env, con);
      fail();
    } catch (ChainManagementException expected) {
      printExpected(label, expected);
    }
    
    env = env.readOnly(false);
    ChainIdMgr manager = ChainIdMgr.ensureInstance(env, con);
    assertTrue(manager.list().isEmpty());
    assertTrue(manager.listNameUris().isEmpty());
    assertTrue(manager.list().isEmpty());
    assertTrue(manager.findByName("anyName").isEmpty());
    assertTrue(manager.findByUri("https://test.crums.io").isEmpty());
    manager.close();
    assertTrue(con.isClosed());
    System.out.println();
  }
  
  
  @Test
  public void testNewChainInfo() throws Exception {
    final Object label = new Object() { };

    try (var closer = suppressLogging()) {
      
      DbEnv env = new DbEnv().readWrite(true);
      Connection con = newDatabase(label);
      closer.pushClose(con);
      
      ChainIdMgr manager = ChainIdMgr.ensureInstance(env, con);
      
      final ChainInfo args = ChainInfo.argsInstance(
          "inventory-log",
          "      In/Out Inventory Log%nStuff coming in or going out gets recorded here."
          .formatted(),
          "microchain:test.crums.io/made/up/scheme");
      
      assertTrue(args.isArgs());
      assertFalse(args.id().isSet());
      
      final ChainInfo chainInfo = manager.newChainInfo(args);
      
      assertFalse(chainInfo.isArgs());
      assertTrue(chainInfo.id().isSet());
      assertNotEquals(0, chainInfo.id().no());
      
      assertTrue(chainInfo.equalsIgnoringId(args));
      // TODO: test ChainInfo stuff in own test-case..
      // testing equalsIgnoringId this once..
      assertEquals(args.name(), chainInfo.name());
      assertEquals(args.description(), chainInfo.description());
      assertEquals(args.uri(), chainInfo.uri());
      
      // if we pass chainInfo as argument again, it should fail right away
      testNewChainInfoWithBadArgs(manager, chainInfo, label);
      // similarly, if we pass the "args" instance again, it should fail
      testNewChainInfoWithBadArgs(manager, args, label);
      // also, assert duplicate uris aren't allowed
      testNewChainInfoWithBadArgs(
          manager,
          ChainInfo.argsInstance("iv2", null, args.uri().get()),
          label); // print it so we see what it looks like
      
      
      manager.close();
    }
  }
  
  
  private List<ChainInfo> makeDataSet() {
    String[] flatArgs =
      {
          "name-zero", null, null,
          "name-1", "name-1 is a bad body using an unknown URI scheme", "fpx://name-1",
          "name02", null, "https://test.crums.io/name_2",
          "three", null, "https://test.crums.io/name_3",
          "hohoho", "coming to town", "https://test.crums.io/santa",
          "inventory-log",
          "      In/Out Inventory Log\nStuff coming in or going out gets recorded here.",
          "microchain:test.crums.io/made/up/scheme"
      };
    return makeDataSet(flatArgs);
  }
  
  
  private List<ChainInfo> makeDataSet(String[] flatArgs) {
    final int count = flatArgs.length / 3;
    ChainInfo[] array = new ChainInfo[count];
    for (int index = 0; index < count; ++index) {
      int flatIndex = index * 3;
      array[index] = ChainInfo.argsInstance(
          flatArgs[flatIndex], flatArgs[flatIndex +1], flatArgs[flatIndex +2]);
    }
    return List.of(array);
  }
  
  private List<ChainInfo> prepareDataSet(ChainIdMgr manager) {
    return prepareDataSet(manager, makeDataSet());
  }
  
  private List<ChainInfo> prepareDataSet(
      ChainIdMgr manager, List<ChainInfo> dataSet) {
    List<ChainInfo> chains = new ArrayList<>();
    for (var arg : dataSet) {
      var newChain = manager.newChainInfo(arg);
      assertTrue(newChain.equalsIgnoringId(arg));
      chains.add(newChain);
    }
    return chains;
  }
      
  
  @Test
  public void testList() throws Exception {
    final Object label = new Object() { };

    try (var closer = suppressLogging()) {
      
      DbEnv env = new DbEnv().readWrite(true);
      Connection con = newDatabase(label);
      closer.pushClose(con);
      
      ChainIdMgr manager = ChainIdMgr.ensureInstance(env, con);
      
      List<ChainInfo> expected = prepareDataSet(manager);
      
      List<ChainInfo> listing = manager.list();
      assertEquals(expected.size(), listing.size());
      
      for (var chain : expected)
        assertTrue(listing.contains(chain));
      
      var out = System.out;

      out.println();
      printMessage(label, "");
      for (var chain : listing)
        out.println("  " + chain);
      out.println();
      
      manager.close();
    }
  }
  

      
  
  @Test
  public void testListNameUris() throws Exception {
    final Object label = new Object() { };

    try (var closer = suppressLogging()) {
      
      DbEnv env = new DbEnv().readWrite(true);
      Connection con = newDatabase(label);
      closer.pushClose(con);
      
      ChainIdMgr manager = ChainIdMgr.ensureInstance(env, con);
      
      List<ChainInfo> dataSet = prepareDataSet(manager);
      
      List<InfoSansDesc> listing = manager.listNameUris();
      assertEquals(dataSet.size(), listing.size());
      
      List<ChainId> ids = Lists.map(listing, InfoSansDesc::id);
      List<String> names = Lists.map(listing, InfoSansDesc::name);
      List<Optional<String>> uris = Lists.map(listing, InfoSansDesc::uri);
      
      for (var expected : dataSet) {
        ids.contains(expected.id());
        names.contains(expected.name());
        uris.contains(expected.uri());
      }
      
      
      var out = System.out;

      out.println();
      printMessage(label, "");
      for (var chain : listing)
        out.println("  " + chain);
      out.println();
      
      manager.close();
    }
  }
  
  
  @Test
  public void testFindByName() throws Exception {
    final Object label = new Object() { };

    try (var closer = suppressLogging()) {
      
      DbEnv env = new DbEnv().readWrite(true);
      Connection con = newDatabase(label);
      closer.pushClose(con);
      
      ChainIdMgr manager = ChainIdMgr.ensureInstance(env, con);
      
      List<ChainInfo> dataSet = prepareDataSet(manager);
      
      for (var expected : dataSet)
        assertEquals(expected, manager.findByName(expected.name()).get());
      
      manager.close();
    }
  }
  
  
  @Test
  public void testFindByUri() throws Exception {
    final Object label = new Object() { };
    
    try (var closer = suppressLogging()) {
      
      DbEnv env = new DbEnv().readWrite(true);
      Connection con = newDatabase(label);
      closer.pushClose(con);
      
      ChainIdMgr manager = ChainIdMgr.ensureInstance(env, con);
      
      List<ChainInfo> dataSet =
          prepareDataSet(manager).stream()
          .filter(ci -> ci.uri().isPresent()).toList();
      
      
      for (var expected : dataSet)
        assertEquals(
            expected,
            manager.findByUri(expected.uri().get()).get() );
  
      manager.close();
    }
  }
  
  
  @Test
  public void testUpdate() throws Exception {
    final Object label = new Object() { };
    
    try (var closer = suppressLogging()) {
      
      DbEnv env = new DbEnv().readWrite(true);
      Connection con = newDatabase(label);
      closer.pushClose(con);
      
      ChainIdMgr manager = ChainIdMgr.ensureInstance(env, con);
      
      List<ChainInfo> chains = prepareDataSet(manager);
      
      testUpdate(
          manager,
          chains.get(0),
          chains.get(0).description(Optional.of("test description")));
      testUpdate(
          manager,
          chains.get(1),
          chains.get(1).uri(null));
      testUpdate(
          manager,
          chains.get(2),
          chains.get(2).name("ledger-3.0"));
      
      
      ChainInfo badArgs = chains.get(3).name(chains.get(0).name());
      testUpdateWithBadArgs(manager, chains.get(3), badArgs, label);
      badArgs = chains.get(2).uri(chains.get(3).uri());
      testUpdateWithBadArgs(manager, chains.get(2), badArgs, label);
  
      manager.close();
    }
  }
  
  
  @Test
  public void testDelete() throws Exception {
    final Object label = new Object() { };
    
    try (var closer = suppressLogging()) {

      Connection con = newDatabase(label);
      closer.pushClose(con);
    
      DbEnv env = new DbEnv().readWrite(true);
      ChainIdMgr manager = ChainIdMgr.ensureInstance(env, con);
      
      prepareDataSet(manager);
      assertDeleteNotSupported(manager, label);
      closer.push(manager);
      
      env = env.readOnly(true);
      manager = new ChainIdMgr(env, con);
      assertDeleteNotSupported(manager, null);
      manager.list();
      
      closer.pushClose(manager);
      
      env = env.allowDelete(true);
      manager = new ChainIdMgr(env, con);
      
      var preDelete = manager.list();  // same as chains (perhaps different order)
      ChainId id = preDelete.get(1).id();
      assertTrue(manager.deleteChain(id));
      var postDelete = manager.list();
      assertEquals(preDelete.size(), postDelete.size() + 1);
      assertTrue(Lists.map(preDelete, ChainInfo::id).contains(id));
      assertFalse(Lists.map(postDelete, ChainInfo::id).contains(id));
      assertFalse(manager.deleteChain(id));
      manager.close();
    }
  }
  
  
  private void assertDeleteNotSupported(ChainIdMgr manager, Object label) {
    ChainId id = manager.list().get(0).id();
    try {
      manager.deleteChain(id);
      fail();
    } catch (UnsupportedOperationException expected) {
      if (label != null)
        printExpected(label, expected);
    }
  }
  
  
  
  
  
  
  
  private void testUpdate(ChainIdMgr manager, ChainInfo existing, ChainInfo args) {
    ChainInfo updated = manager.update(existing.id(), args);
    assertTrue(updated.equalsIgnoringId(args));
    assertEquals(existing.id(), updated.id());
  }
  
  
  
  private void testUpdateWithBadArgs(
      ChainIdMgr manager, ChainInfo existing, ChainInfo badArgs, Object label) {
    
    try {
      manager.update(existing.id(), badArgs);
      fail("expected update %s => %s to fail".formatted(existing, badArgs));
    } catch (IllegalArgumentException expected) {
      if (label != null)
        printExpected(label, expected);
    }
  }
  
  

  
  
  
  private void testNewChainInfoWithBadArgs(
      ChainIdMgr manager, ChainInfo badArgs, Object label) {
    try {
      manager.newChainInfo(badArgs);
      fail();
    } catch (IllegalArgumentException expected) {
      if (label != null)
        printExpected(label, expected);
    }
  }
  
  
  
  

}






















