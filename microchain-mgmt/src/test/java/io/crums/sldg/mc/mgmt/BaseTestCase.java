/*
 * Copyright 2025 Babak Farhang
 */
package io.crums.sldg.mc.mgmt;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import io.crums.testing.IoTestCase;
import io.crums.util.TaskStack;


/**
 * Helper base class.
 * 
 * @see #newDatabase(Object, boolean)
 */
public abstract class BaseTestCase extends IoTestCase {

  final static String MOCK_DB_NAME = "mock_db";
  
  
  /**
   * Returns a connection to a new in-memory h2 database.
   * 
   * @param label method label
   */
  protected Connection newDatabase(Object label)
      throws ClassNotFoundException, SQLException {
    return newDatabase(label, true);
  }
  
  /**
   * Returns a connection to a new, either in-memory. or file-backed
   * h2 database.
   * 
   * @param label    method label
   * @param inMemory if {@code false}, then a file-backed h2 database
   */
  protected Connection newDatabase(Object label, boolean inMemory)
      throws ClassNotFoundException, SQLException {
    if (inMemory) {
      Class.forName("org.h2.Driver");
      return DriverManager.getConnection("jdbc:h2:mem:" + method(label));
    } else {
      File dir = getMethodOutputFilepath(label);
      dir.mkdirs();
      return newDatabase(new File(dir, MOCK_DB_NAME));
    }
  }
  
  /**
   * Returns a connection to a file-backed h2 database
   * 
   * @param dbPath      h2 database file (parent dir must exist)
   */
  protected Connection newDatabase(File dbPath)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    return DriverManager.getConnection("jdbc:h2:" + dbPath.getPath());
  }
  
  
  
  protected void printExpected(Object label, Exception expected) {
    System.out.printf(
        "[%s]: [EXPECTED ERROR]%n%s%n%n", method(label), expected);
  }
  
  protected void printMessage(Object label, String msg) {
    System.out.printf("[%s]: %s%n", method(label), msg);
    
  }
  
  
  protected TaskStack suppressLogging() {
    return suppressLogging(new TaskStack());
  }
  
  protected TaskStack suppressLogging(TaskStack closer) {
    Logger rootLogger = LogManager.getLogManager().getLogger("");
    Level level = rootLogger.getLevel();
    rootLogger.setLevel(Level.OFF);
    closer.pushRun(() -> rootLogger.setLevel(level));
    return closer;
  }
  
}










