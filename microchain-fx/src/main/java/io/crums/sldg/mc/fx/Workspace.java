/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.fx;


import static io.crums.sldg.mc.fx.AppConstants.*;

import java.io.File;
import java.io.FileInputStream;
import java.lang.System.Logger.Level;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.crums.util.Strings;
import io.crums.util.TaskStack;

/**
 * 
 */
public class Workspace implements AutoCloseable {
  
  
  @SuppressWarnings("serial")
  public static class ConfigException extends RuntimeException {
    ConfigException(String msg) { super(msg); }
    ConfigException(String msg, Exception cause) { super(msg, cause); }
    ConfigException(Exception cause) { super(cause); }
  }
  
  
  public static List<Workspace> listWorkspaces() {
    Preferences pkgPrefs = Preferences.userNodeForPackage(Workspace.class);
    
    try {
      if (!pkgPrefs.nodeExists(WORKSPACES))
        return List.of();
      
      Preferences spaces = pkgPrefs.node(WORKSPACES);
      String[] names = spaces.childrenNames();
      if (names.length == 0)
        return List.of();
      
      var list = new ArrayList<Workspace>(names.length);
      for (var name : names) {
        Preferences node = spaces.node(name);
        String hikariPath = node.get(HIKARI_PATH, null);
        String driverPath = node.get(DRIVER_PATH, null);
        String tablePrefix = node.get(SYS_TABLE_PREFIX, null);
        Workspace ws;
        try {
          ws = new Workspace(
              name, new File(hikariPath), new File(driverPath), tablePrefix);
        } catch (Exception x) {
          getLogger().log(
              Level.WARNING,
              "ignoring (skipping) malformed workspace '%s'. Error detail: %s"
              .formatted(name, x));
          continue;
        }
        list.add(ws);
      }
      
      return List.copyOf(list);
      
    } catch (BackingStoreException bsx) {
      throw new AppException(bsx);
    }
  }
  
  public final static int MAX_PREFIX_LEN = 10;
  
  final static String WORKSPACES = "WorkSpaces";
  final static String HIKARI_PATH = "hikariPath";
  final static String DRIVER_PATH = "driverPath";
  final static String SYS_TABLE_PREFIX = "sysTablePrefix";
  
  private final String name;
  
  private final File hikariConfigFile;
  private final File jdbcDriverPath;
  private final String sysTablePrefix;
  

  /**
   * 
   */
  Workspace(
      String name, File hikariConfigFile, File jdbcDriverPath,
      String sysTablePrefix) {
    
    this.name = name.trim();
    this.hikariConfigFile = hikariConfigFile.getAbsoluteFile();
    this.jdbcDriverPath = jdbcDriverPath.getAbsoluteFile();
    this.sysTablePrefix = normalizePrefix(sysTablePrefix);
    
    if (name.isEmpty())
      throw new IllegalArgumentException("blank or empty name");
    
    
  }
  
  
  private String normalizePrefix(String prefix) {
    if (prefix == null)
      return null;
    prefix = prefix.trim();
    final int len = prefix.length();
    if (len == 0)
      return null;
    if (len > MAX_PREFIX_LEN)
      throw new IllegalArgumentException(
          "table prefix too long (%d); prefix (quoted): '%s'"
          .formatted(len, prefix));
    for (int index = 0; index < len; ++index) {
      char c = prefix.charAt(index);
      if (Strings.isAlphabet(c))
        continue;
      if (c != '_')
        throw new IllegalArgumentException(
            "illegal table prefix char at index [%d]; prefix (quoted): '%s'"
            .formatted(index, prefix));
    }
    return prefix;
  }
  
  
  public Workspace sysTablePrefix(String prefix) {
    prefix = normalizePrefix(prefix);
    
    return 
        Objects.equals(prefix, sysTablePrefix) ?
            this :
              new Workspace(name, hikariConfigFile, jdbcDriverPath, prefix);
    
  }
  
  
  public Properties hikariProps() {
    var props = new Properties();
    try (var in = new FileInputStream(hikariConfigFile)) {
      props.load(in);
    } catch (Exception x) {
      throw new AppException(
          "on loading hikari properties for workspace '%s': caused by %s"
          .formatted(name, x.toString()),
          x);
    }
    return props;
  }
  
  
  private List<Driver> drivers;
  private HikariDataSource dataSource;
  
  
  @Override
  public void close() {
    if (dataSource != null)
      dataSource.close();
    drivers = null;
  }
  
  
  List<Driver> drivers() {
    return drivers == null ? List.of() : drivers;
  }
  
  
  List<Driver> loadDrivers() {
    
    if (!drivers().isEmpty())
      return drivers;
    
    var moduleFinder = ModuleFinder.of(jdbcDriverPath.toPath());
    
    var moduleNames = moduleFinder.findAll()
        .stream()
        .map(ModuleReference::descriptor)
        .map(ModuleDescriptor::name)
        .collect(Collectors.toSet());
    
    var debug = System.out;
    debug.println(this + " module names: " + moduleNames);
    
    
  
    var bootLayer = ModuleLayer.boot();
    
    var configuration =
        bootLayer.configuration()
        .resolve(moduleFinder, ModuleFinder.of(), moduleNames);
    
    ModuleLayer driverLayer =
      bootLayer.defineModulesWithOneLoader(
          configuration, ClassLoader.getSystemClassLoader());
    
    this.drivers =
        ServiceLoader.load(driverLayer, java.sql.Driver.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .toList();
  
    debug.println(this + " drivers loaded: " + drivers);
    
    if (drivers.isEmpty())
      throw new ConfigException("no JDBC driver found in " + jdbcDriverPath);
    
    return drivers;
  }
  
  HikariDataSource loadDataSource() {
    
    if (dataSource != null && !dataSource.isClosed()) {
      return dataSource;
    }
    
    loadDrivers();
    
    final ClassLoader originalLoader =
        Thread.currentThread().getContextClassLoader();
    
    
    try {
      
      Thread.currentThread().setContextClassLoader(
          drivers.get(0).getClass().getClassLoader());
      
      this.dataSource = new HikariDataSource(new HikariConfig(hikariProps()));
      return dataSource;
      
    } catch (ConfigException cx) {
      throw cx;
    } catch (Exception x) {
      throw new ConfigException(x);
    } finally {
      Thread.currentThread().setContextClassLoader(originalLoader);
    }
  }
  
  
  public String name() {
    return name;
  }
  
  
  public File hikariConfigFile() {
    return hikariConfigFile;
  }
  
  
  public File jdbcDriverFile() {
    return jdbcDriverPath;
  }
  
  
  
  
  
  
  
  public Optional<String> sysTablePrefix() {
    return Optional.ofNullable(sysTablePrefix);
  }
  
  
  
  void save() {
    Preferences wsPrefs =
        Preferences.userNodeForPackage(Workspace.class).node(WORKSPACES);
    
    Preferences node = wsPrefs.node(name);
    
    node.put(HIKARI_PATH, hikariConfigFile.getPath());
    node.put(DRIVER_PATH, jdbcDriverPath.getPath());
    if (sysTablePrefix != null)
      node.put(SYS_TABLE_PREFIX, sysTablePrefix);
    
    try {
      wsPrefs.flush();
    } catch (BackingStoreException bsx) {
      throw new AppException(
          "on saving workspace '%s': caused by %s".formatted(name, bsx), bsx);
    }
  }
}
