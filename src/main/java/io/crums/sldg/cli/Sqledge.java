/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.cli;

import static io.crums.util.Strings.*;

import java.io.Console;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.crums.sldg.sql.ConfigFileBuilder;
import io.crums.sldg.sql.SqlSourceQuery;
import io.crums.util.Strings;
import io.crums.util.cc.ThreadUtils;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import io.crums.util.main.TablePrint;

/**
 * 
 */
public class Sqledge extends MainTemplate {
  
  private String command;

  /**
   * @param args
   */
  public static void main(String[] args) {
    new Sqledge().doMain(args);
  }
  
  
  

  @Override
  protected void init(String[] args) throws IllegalArgumentException, Exception {
    ArgList argList = newArgList(args);
    this.command = argList.removeCommand(SETUP);
    switch (command) {
    case SETUP:
      break;
    }
  }

  @Override
  protected void start() throws InterruptedException, Exception {
    switch (command) {
    case SETUP:
      setup();
      break;
    }
  }
  
  
  
  
  
  
  private ConfigFileBuilder builder;
  private Console console;

  private void setup() {
    this.console = System.console();
    if (console == null) {
      System.err.println("[ERROR] No console. Aborting.");
      StdExit.GENERAL_ERROR.exit();
    }
    console.printf("%nEntering interactive mode..%nEnter <CTRL-c> to abort at any time.%n%n");
    PrintSupport printer = new PrintSupport();
    var paragraph =
        "The steps to create a valid (or at least well-formed) configuration file for this program consist " +
        "of configuring a connection to the source ledger (the append-only table), optionally " +
        "(recommended) a separate database connection for the hash-ledger, and then declaring which " +
        "columns (and in what order) define a source-row's data to be tracked.";
    printer.printParagraph(paragraph);
    printer.println();
    paragraph =
        "2 connections URLs is recommended, instead of just the one, because that way " +
        "it's possible to configure a read-only connection for the source-table while allowing a " +
        "read/write connection dedicated to the hash-ledger tables that track it.";
    printer.printParagraph(paragraph);
    printer.println();
    printer.println("Here's an outline of what we'll be doing..");
    printer.println();
    printer.setIndentation(2);
    printer.println("1. Configure DB connection to source table (the ledger source)");
    printer.println("  1.a JDBC URL configuration (Required)");
    printer.println("  1.b Connection name/value paramaters (Optional if already in URL)");
    printer.println("  1.c JDBC Driver class (Optional if already registered at boot time)");
    printer.println("  1.d JDBC Driver classpath (Optional if already defined at boot time)");
    printer.println();
    printer.println("2. Configure DB connection for hash ledger tables as above (Optional)");
    printer.println();
    printer.println("3. Configure source- and hash-ledger schemas");
    printer.println("  3.a Source table (or view) name");
    printer.println("  3.b Source table PRIMARY_KEY column");
    printer.println("  3.c Other source table columns");
    printer.println();
    printer.println("4. Save the file");
    printer.println();
    
    File targetConfigFile = getTargetConfigFile();
    this.builder = new ConfigFileBuilder(targetConfigFile);
    setSrcConUrl();
    setSrcConInfo();
    if (setSrcDriverClass())
      setSrcDriverClasspath();
    testSrcCon();
    if (setHashConUrl()) {
      setHashConInfo();
      if (setHashDriverClass())
        setHashDriverClasspath();
    }
    setSourceTablename();
    setSourceColumns();
    printf("%n");
    printf("Generating random salt%n");
    printf("%n");
    builder.seedSourceSalt();
    saveConfigFile();
  }









  private void saveConfigFile() {
    
    var path = builder.getConfigFile().getPath();
    printf("Saving configuration to <%s>", path);
    try {
      builder.save();
      
    } catch (Exception x) {
      printf("%nERROR: on attempt to save <%s>%n%s%n", path, x);
    }
    printf("%n");
    printf("%nDone.");
    printf("%n");
  }




  private void setSourceColumns() {
    printf("%nGood.");
    printf(" Now name the column names in table%n   <%s>%n", builder.getHashTablePrefix());
    
    var paragraph =
        "that are to be tracked. " +
        "The first of these should be the table's PRIMARY KEY column " +
        "(or the equivalent of). The values in this column must be monotonically " +
        "increasing, whenever new rows are appended to the table. There are no restrictions " +
        "on the other columns (most any SQL data type, or NULL value should work).";
    
    PrintSupport printer = new PrintSupport();
    printer.printParagraph(paragraph);
    printer.println();
    
    for (int count = MAX_TRIALS; count-- > 0; ) {
      printf("Enter the column names (separate with spaces):%n");
      String line = readLine();
      if (line.isEmpty())
        continue;
      var tokenizer = new StringTokenizer(line);
      final int cc = tokenizer.countTokens();
      if (cc < 2) {
        printf("Need at least 2 column names.. Try Again.%n");
        continue;
      }
      
      ArrayList<String> columnNames = new ArrayList<>(cc);
      while (tokenizer.hasMoreTokens()) {
        String colName = tokenizer.nextToken();
        if (malformedColumnname(colName)) {
          printf("Woops, with '%s' (quoted column-name).. Try Again.%n", colName);
          break;
        }
        columnNames.add(colName);
      }
      if (columnNames.size() != cc)
        continue; // (user input error already notified)
      
      var queryBuilder = new SqlSourceQuery.DefaultBuilder(
          builder.getHashTablePrefix(),
          columnNames);
      
      builder.setSourceSizeQuery(queryBuilder.getPreparedSizeQuery());
      builder.setSourceRowQuery(queryBuilder.getPreparedRowByNumberQuery());
      printf("%nExcellent.");
      return;
    }
    
    giveupAbort();
  }

  
  private void printf(String format, Object... args) {
    try {
      ThreadUtils.ensureSleepMillis(100);
    } catch (InterruptedException ix) {
      Thread.currentThread().interrupt();
    }
    console.printf(format, args);
  }


  private void setSourceTablename() {
    for (int count = MAX_TRIALS; count-- > 0; ) {
      printf("%nEnter the name of the table that is to be tracked:%n");
      String table = readLine();
      if (table.isEmpty())
        continue;
      if (malformedTablename(table)) {
        printf("%nThat can't be right. Try Again.");
        continue;
      }
      builder.setHashTablePrefix(table);
      return;
    }
    giveupAbort();
  }



  private boolean malformedTablename(String table) {
    // stub
    return table.length() < 2 || malformedSqlThing(table);
  }
  
  
  private boolean malformedColumnname(String column) {
    // stub
    return malformedSqlThing(column);
  }
  
  private boolean malformedSqlThing(String thing) {
    if (!Strings.isAlphabet(thing.charAt(0)))
      return true;
    for (int index = 1; index < thing.length(); ++index) {
      char c = thing.charAt(index);
      boolean ok = Strings.isAlphabet(c) || Strings.isDigit(c) || c == '_';
      if (!ok)
        return true;
    }
    return false;
  }



  private final static int MAX_TRIALS = 3;


  private File getTargetConfigFile() {
    File target = null;
    int count = MAX_TRIALS;
    int blanks = 0;
    while (target == null && count-- > 0) {
      printf("Enter the destination path:%n");
      String path = readLine();
      
      if (path.isEmpty()) {
        if (++blanks < 2)
          ++count;
        continue;
      } else
        blanks = 0;
      
      try {
        target = new File(path);
        if (target.exists()) {
          target = null;
          if (count > 0)
            printf("%nWoops.. that file path already exists. Try again.%n");
        }
      } catch (Exception x) {
        if (count > 0)
          printf(
              "%nThat doesn't seem to work. (Error msg: %s)%n" +
              "Try again.%n", x.getMessage());
      }
      if (target == null)
        giveupAbort();
    }
    return target;
  }
  
  
  private String readLine() {
    String line = console.readLine();
    if (line == null)
      giveupAbort();
    return line.trim();
  }
  
  
  private void giveupAbort() {
    System.err.println();
    System.err.println("OK, giving up. Aborting.");
    System.err.println();
    StdExit.ILLEGAL_ARG.exit();
  }
  
  
  private void setSrcConUrl() {
    printf("%nGood.");
    printf(
        " We need a connection URL to the database the source table lives in.%n" +
        "For e.g. jdbc:postgres.. (We'll add parameters and credentials after.)%n%n");
    int blanks = 0;
    for (int count = MAX_TRIALS; count-- > 0; ) {
      printf("Enter the connection URL for the source table:%n");
      String url = readLine();
      if (url.isEmpty()) {
        if (++blanks < 2)
          ++count;
        continue;
      } else
        blanks = 0;
      if (malformedUrl(url)) {
        console.printf("Not a valid URL. Try Again.%n");
        continue;
      }
      
      builder.setSourceUrl(url);
      
      printf("%nNice. ");
      break;
    }
  }
  
  
  private boolean malformedUrl(String url) {
    return url.length() < 10 || ! url.toLowerCase().startsWith("jdbc:");
  }
  
  
  private void setSrcConInfo() {
    printf("How about credentials and other parameters for this database?%n");
    printf(
        "These are passed in as name/value pairs. For example%n" +
        " user=admin%n pwd=123%n" +
        "If your driver supports it, you can set this connection to read-only (e.g.%n" +
        "readonly=true).%n%n");
    printf("Enter as many name/value pairs as needed followed by an empty line:%n");
    
    doAddConInfoLoop(
        (name,value) -> { builder.setSourceConProperty(name, value); return Boolean.TRUE;},
        "source");
    
  }
  
  
  
  private void doAddConInfoLoop(BiFunction<String,String,?> func, String conName) {

    int count = MAX_TRIALS;
    var tally = new HashMap<>();
    while (true) {
      String line = readLine();
      if (line.isEmpty()) {
        int props = tally.size();
        if (props < 2) {
          // confirm
          String msg = props == 0 ? "Confirm no parameters" : "Confirm just 1 paramater";
          msg += " for " + conName + " connection [y]:%n";
          printf(msg);
          String ack = readLine();
          boolean yes = "y".equalsIgnoreCase(ack) || "yes".equalsIgnoreCase(ack);
          
          if (!yes) {
            printf("%nOK, continue adding name/value pairs, followed by an empty line:%n");
            continue;
          }
        }
        break;
      }
      int delimiterIndex = delimiterIndex(line);
      if (delimiterIndex == -1) {
        warnInvalidProp(--count);
        continue;
      }
      String name = line.substring(0, delimiterIndex).trim();
      String value = line.substring(delimiterIndex + 1, line.length()).trim();
      if (name.isEmpty() || value.isEmpty()) {
        warnInvalidProp(--count);
        continue;
      }
      func.apply(name, value);
//      builder.setSourceConProperty(name, value);
      Object oldValue = tally.put(name, value);
      if (oldValue != null)
        console.printf("(Overwrote old value '%s')%n", oldValue);
      count = MAX_TRIALS;
    }
    int props = tally.size();
    String msg = props < 2 ? "%nOK, " : "%nGreat. ";
    msg += props + pluralize(" name/value pair", props) + " added.%n%n";
    printf(msg);
  }
  




  private void setHashConInfo() {
    printf(
        "Enter credentials and other parameters for this DB connection as you did for the%n" +
        "connection to the source-table's DB.%n%n");
    
    printf("Enter as many name/value pairs as needed followed by an empty line:%n");
    
    doAddConInfoLoop(
        (name,value) -> { builder.setHashConProperty(name, value); return Boolean.TRUE;},
        "hash-ledger");
  }


  private boolean setDriverClass(String url, Consumer<String> func) {
    printf(
        "%nDo you need to set the JDBC Driver class associated with the connection URL%n '%s' ?%n%n",
        url);
    printf(
        "You can specify the Driver class (and then its classpath) here, if it's not%n" +
        "pre-registered with the Runtime's DriverManager.%n%n");
    
    while (true) {
      printf("Enter the driver class name (enter blank to skip):%n");
      String line = readLine();
      if (line.isEmpty()) {
        console.printf("%nNot setting any driver class / classpath.%n%n");
        return false;
      }
      if (Strings.isPermissableJavaName(line)) {
        func.accept(line);
//        builder.setSourceDriverClass(line);
        return true;
      }
      console.printf("%nNot a valid Java class name. Try again:%n");
    }
  }


  private boolean setHashDriverClass() {
    return setDriverClass(
        builder.getHashUrl(),
        s -> builder.setHashDriverClass(s));
  }
  
  
  


  private void setHashDriverClasspath() {
    setDriverClasspath(
        builder.getHashDriverClass(),
        s -> builder.setHashDriverClasspath(s));
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  private int delimiterIndex(String line) {
    int equalIndex = supIndexOf(line, '=');
    int colonIndex = supIndexOf(line, ':');
    int supIndex = Math.min(equalIndex, colonIndex);
    return supIndex == line.length() ? -1 : supIndex;
  }
  
  int supIndexOf(String line, char c) {
    int index = line.indexOf(c);
    return index == -1 ? line.length() : index;
  }
  
  private void warnInvalidProp(int countdown) {
    warn("That's not a valid name/value pair. Try again:%n", countdown);
  }

  private void warn(String message, int countdown) {
    if (countdown > 0)
      console.printf(message);
    else
      giveupAbort();
  }

  private boolean setSrcDriverClass() {
    return setDriverClass(
        builder.getSourceUrl(),
        s -> builder.setSourceDriverClass(s));
//    console.printf(
//        "%nDo you need to set the JDBC Driver class associated with the connection URL%n '%s' ?%n%n" +
//        "You can specify the Driver class (and then its classpath) here, if it's not%n" +
//        "pre-registered with the Runtime's DriverManager.%n%n" +
//        "Enter the driver class name (enter blank to skip):%n", builder.getSourceUrl());
//    
//    while (true) {
//      String line = readLine();
//      if (line.isEmpty()) {
//        console.printf("%nNot setting any driver class / classpath.%n%n");
//        return false;
//      }
//      if (Strings.isPermissableJavaName(line)) {
//        builder.setSourceDriverClass(line);
//        return true;
//      }
//      console.printf("%nNot a valid Java class name. Try again:%n");
//    }
  }


  private void setSrcDriverClasspath() {
    setDriverClasspath(
        builder.getSourceDriverClass(),
        s -> builder.setSourceDriverClasspath(s));
  }
  
  
  private void setDriverClasspath(String driverClass, Consumer<String> func) {
    console.printf(
        "%nGreat. Do you need to set the .jar file from which%n     %s%nwill be loaded?%n%n" +
        "You can set the path to its .jar file either absolutely, or relative to the%n" +
        "(parent directory of the) configuration file.%n%n" +
        
        "Enter the location of the .jar file (enter blank to skip):%n",
        driverClass);
    
    
    while (true) {
      String path = readLine();
      if (path.isEmpty())
        break;
      if (!path.endsWith(".jar")) {
        console.printf("Error. Must be a .jar file. Try again:%n");
        continue;
      }
      
      try {
        File resolvedPath = new File(path);
        boolean relative = !resolvedPath.isAbsolute();
        if (relative)
          resolvedPath = new File(builder.getBaseDir(), path);
        
        if (resolvedPath.isFile()) {
          console.printf("%nGot it.%n%n");
          func.accept(path);
//          builder.setSourceDriverClasspath(path);
          break;
        } else {
          String msg = "Error. " + path;
          if (relative)
            msg += " which resolves to " + resolvedPath;
          msg += " does not exist. Try again:%n";
          console.printf(msg);
          continue;
        }
      } catch (Exception x) {
        console.printf("Woops. That didn't work (" + x.getMessage() + "). Try again:%n");
      }
    }
    
  }
  
  private void testSrcCon() {
    // TODO
  }
  
  
  private boolean setHashConUrl() {
    PrintSupport printer = new PrintSupport();
    var paragraph =
        "The hash-ledger (the data structure that tracks the source-table) reads and " +
        "writes to its own tables. The information in this ledger is almost entirely " +
        "opaque and consists of undecipherable hashes: it does reveal the number of " +
        "rows in the source-table and their history (when the hashes were witnessed), " +
        "however.";
    printer.printParagraph(paragraph);
    printf("%n");
    paragraph =
        "So the hash-ledger needs a (read/write) DB connection--which can either be one " +
        "dedicated to itself, or the same one used to access the source-table.";
    printer.printParagraph(paragraph);
    printf("%n");
    while (true) {
      printf(
          "Enter the connection URL for the hash ledger (enter blank to skip):%n");
      String url = readLine();
      if (url.isEmpty())
        return false;
      if (malformedUrl(url)) {
        console.printf("%nNot a valid URL. Try Again.%n");
        continue;
      }
      
      builder.setHashUrl(url);
      printf("%nGot it.");
      return true;
    }
  }
  
  private final static int INDENT = 1;
  private final static int RM = 80;

  @Override
  protected void printDescription() {
    String paragraph;
    PrintSupport printer = new PrintSupport();
    printer.setMargins(INDENT, RM);
    printer.println();
    System.out.println("DESCRIPTION:");
    printer.println();
    paragraph =
        "Command line tool for managing historical ledgers on relational databases.";
    printer.printParagraph(paragraph);
    printer.println();
  }

  private final static int LEFT_TBL_COL_WIDTH = 15;
  private final static int RIGHT_TBAL_COL_WIDTH = RM - INDENT - LEFT_TBL_COL_WIDTH;

  @Override
  protected void printUsage(PrintStream out) {
    PrintSupport printer = new PrintSupport(out);
    printer.setMargins(INDENT, RM);
    printer.println();
    out.println("USAGE:");
    printer.println();
    String paragraph =
        "Commands are listed in the table below.";
    printer.printParagraph(paragraph);
    printer.println();

    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    
    table.printRow(SETUP, "interactive config file setup");
    table.println();
  }
  
  
  private final static String SETUP = "setup";

}
