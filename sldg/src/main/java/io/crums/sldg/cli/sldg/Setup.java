package io.crums.sldg.cli.sldg;


import static io.crums.util.Strings.pluralize;

import java.io.Console;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.crums.sldg.sql.ConfigFileBuilder;
import io.crums.sldg.sql.ConnectionInfo;
import io.crums.sldg.sql.SqlSourceQuery;
import io.crums.util.Strings;
import io.crums.util.cc.ThreadUtils;
import io.crums.util.main.PrintSupport;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Model.CommandSpec;

/**
 * This {@linkplain Sldg} subcommand, being long and verbose, is factored out
 * in a separate file. Ordinarily this'd live in the same file as {@code Sldg.java}.
 */
@Command(
    name = Setup.NAME,
    description = {
        "Interactive ledger definition/configuration file setup",
        "",
        "You'll need the following in order to complete this step:",
        "",
        "  - JDBC connection URL",
        "  - Database access credentials",
        "  - path to JDBC driver jar file",
        "  - Table name, primary-key column name, other column names",
        "    (you can JOIN values from other tables to complete the view later)",
        ""
    })
class Setup implements Runnable {
  
  final static String NAME = "setup";

  @ParentCommand
  Sldg sldg;
  
  @Spec
  private CommandSpec spec;
  
  

  @Option(names = {"-b", "--make-it-brief"}, description = "Skip the preamble, less chatty")
  boolean makeItBrief;
  
  private ConfigFileBuilder builder;
  private Console console;

  @Override
  public void run() {
    if (sldg.getConfigFile() != null)
      throw new ParameterException(spec.commandLine(), "command " + NAME + " takes no arguments");
    console = System.console();
    if (console == null) {
      System.err.printf("[ERROR] command %s invoked without console%n", NAME);
      System.exit(Sldg.ERR_USER);
    }
    final var out = System.out;
    
    
    out.printf("%n" + Ansi.AUTO.string("@|bold Entering interactive mode..|@") + "%n");
    
    printf("  Use <%s> to abort at any time%n%n", "@|fg(yellow) CTRL-c|@");
    
    preamble();
    
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
    printf("%n");
    

    var paragraph =
        """
        Generating default schema definitions (CREATE TABLE statements) for the hash-ledger. 
        These are simple enough to work on most any database engine. Still, if you need to define
        these schemas some other way you can edit the configuration file (or create the
        tables on the database yourself).
        """;
    new PrintSupport().printParagraph(paragraph);
    builder.setDefaultHashSchemas();
    
    saveConfigFile();
    
  }
  
  void preamble() {
    if (makeItBrief)
      return;
    PrintSupport printer = new PrintSupport();
    var paragraph =
        """
        The steps to create a valid (or at least well-formed) configuration properties file for this program consist
        of configuring a connection to the source ledger (the append-only table or view), optionally
        a separate database connection for the hash-ledger that tracks it, and then declaring which
        columns (and in what order) define a source-row's data to be tracked.
        """;
    printer.printParagraph(paragraph);
    printer.println();
    
    printf("Note %s.%n%n", "@|italic nothing is written to any database|@");
    paragraph =
        """
        2 connection URLs are possible, instead of just the one. That way one
        can configure a read-only connection for the source-table while allowing a
        read/write connection (possibly at another, maybe centralized database instance)
        dedicated to the hash-ledger tables that track it.
        """;
    printer.printParagraph(paragraph);
    
    printer.println();
    printer.println("Here's an outline of what we'll be doing..");
    printer.println();
    printer.setIndentation(2);
    printer.println("1. Configure DB connection to source table (the ledger source)");
    printer.println("     1.a JDBC URL configuration (Required)");
    printer.println("     1.b Connection name/value paramaters (Optional if specified in URL)");
    printer.println("     1.c JDBC Driver class (Optional if registered at boot time)");
    printer.println("     1.d JDBC Driver classpath (Optional if registered at boot time)");
    printer.println();
    printer.println("2. Configure DB connection for hash ledger tables as above (Optional)");
    printer.println();
    printer.println("3. Configure source-query and hash-ledger schemas");
    printer.println("   We'll construct a \"by-row-number\" query using the following");
    printer.println("     3.a Source table (or view) name");
    printer.println("     3.b Source table PRIMARY_KEY column (or equivalent of)");
    printer.println("     3.c Other source table columns");
    printer.println("   You can edit this query before creating the hash ledger. For example, you");
    printer.println("   can JOIN column values from other relevant tables into the view. (There's");
    printer.println("   no storage overhead for additional columns.)");
    printer.println();
    printer.println("4. Save the file. Contains default schema definition for the hash ledger.");
    printer.println("   Works for most any SQL database. Customized in here in case you need to.");
    printer.println();
  }
  
  void setSrcConInfo() {
    printf("How about credentials and other parameters for this database?%n");
    if (!makeItBrief)
      printf(
          "These are passed in as name/value pairs. For example%n" +
          " user=admin%n pwd=123%n" +
          "If your driver supports it, you can set this connection to read-only (e.g.%n" +
          "readonly=true).%n%n");
    printf("Enter as many name/value pairs as needed followed by an empty line:%n");
    
    doAddConInfoLoop(builder::setSourceConProperty, "source");
    
  }
  
  void setHashConInfo() {
    printf(
        "Enter credentials and other parameters for this DB connection as you did for the%n" +
        "connection to the source-table's DB.%n%n");
    
    printf("Enter as many name/value pairs as needed followed by an empty line:%n");
    
    doAddConInfoLoop(builder::setHashConProperty, "hash-ledger");
  }
  
  boolean setSrcDriverClass() {
    return setDriverClass(
        builder.getSourceUrl(),
        s -> builder.setSourceDriverClass(s));
  }
  
  void setSrcDriverClasspath() {
    setDriverClasspath(
        builder.getSourceDriverClass(),
        s -> builder.setSourceDriverClasspath(s));
  }
  
  void testSrcCon() {
    printf("%nWould you like to test this DB connection? [y]:");
    if ("y".equalsIgnoreCase(readLine())) {
      printf("%n%nAttempting connnection.. ");
      if (srcConWorks())
        printf(" Works!%n%n");
      else {
        printf(" Woops, that didn't work.");
        printf("%nYou can edit these configuration properties later.%n%n");
      }
    } else {
      printf("%n%nOK, skipping DB connnection test%n");
    }
  }

  boolean setHashConUrl() {
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
  
  void setHashDriverClasspath() {
    setDriverClasspath(
        builder.getHashDriverClass(),
        builder::setHashDriverClasspath);
  }
  
  void setSourceTablename() {
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
  
  void setSourceColumns() {
    printf("%nGood.");
    printf(" Now name the column names in table%n   <%s>%n", builder.getHashTablePrefix());
    
    var paragraph =
        """
        that are to be tracked. 
        The first of these should be the table's PRIMARY KEY column 
        (or the equivalent of). The values in this column must be monotonically 
        increasing, whenever new rows are appended to the table. There are no restrictions 
        on the other columns (most any SQL data type, or NULL value should work).
        """;
    
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
  
  void saveConfigFile() {
    
    var path = builder.getConfigFile().getPath();
    printf("Saving configuration to%n  <%s>", path);
    try {
      builder.save();
      
    } catch (Exception x) {
      printf("%nERROR: on attempt to save <%s>%n%s%n", path, x);
    }
    printf("%n");
    printf("%nDone.%n");
    printf("%n");
    String note =
        "Review the contents of the config file (adjust per SQL dialect) and then invoke the '" + Create.NAME + "' command. " +
        "You can modify the rows and columns pulled in on the SELECT \"by row number\" statement there. For example, " +
        "you may denormalize the data (pull in other column values related to FOREIGN KEYs). " +
        "Note you can redact any column when outputing a morsel, and that the storage overhead in the hash ledger is small and fixed per row, regardless how " +
        "many columns are returned in the SELECT statement.";
    new PrintSupport().printParagraph(note);
    printf("%n");
  }
  
  
  private boolean malformedTablename(String table) {
    return table.length() < 2 || malformedSqlThing(table);
  }
  
  private boolean malformedColumnname(String column) {
    return column.isEmpty() || malformedSqlThing(column);
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
  
  private boolean srcConWorks() {
    ConnectionInfo conInfo =
        new ConnectionInfo(
            builder.getSourceUrl(),
            builder.getSourceDriverClass(),
            builder.getSourceDriverClasspath());
    try {
      conInfo.open(builder.getBaseDir(), builder.getSourceConProperties()).close();
      return true;
    } catch (Exception x) {
      return false;
    }
  }

  private final static int MAX_TRIALS = 3;

  private File getTargetConfigFile() {
    File target = null;
    int count = MAX_TRIALS;
    int blanks = 0;
    while (target == null && count-- > 0) {
      printf("Enter the destination path for configuration file:%n");
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
            printf("%n%s.. that file path already exists. Try again.%n", "@|italic Woops|@");
        } else {
          var dir = target.getParentFile();
          if (dir != null && !dir.exists()) {
            ++count;
            target = null;
            printf("%nWon't create parent directory %s. Try again.%n", "@|italic " + dir + "|@");
          }
        }
      } catch (Exception x) {
        if (count > 0)
          printf(
              "%nThat doesn't seem to work. (Error msg: %s)%n" +
              "Try again.%n", "@|fg(red) " + x.getMessage() + "|@");
      }
    }
    if (target == null)
      giveupAbort();
    return target;
  }
  
  

  void setSrcConUrl() {
    printf("%nGood.");
    printf(
        " We need a connection URL to the database the source table lives in.%n" +
        "For e.g. jdbc:postgres.. (We'll add parameters and credentials after.)%n%n");
    int blanks = 0;
    for (int count = MAX_TRIALS; count-- > 0; ) {
      printf("Enter the connection URL for the source table's database:%n");
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
      
      return;
    }
    giveupAbort();;
  }
  


  boolean setHashDriverClass() {
    return setDriverClass(
        builder.getHashUrl(),
        builder::setHashDriverClass);
  }
  
  
  
  
  private void setDriverClasspath(String driverClass, Consumer<String> func) {
    console.printf(
        "%nGreat. Do you need to set the .jar file from which%n     <%s>%nwill be loaded?%n%n" +
        "You can set the path to its .jar file either absolutely, or relative to the%n" +
        "(parent directory of the) configuration file.%n%n" +
        
        "Enter the location of the .jar file (enter blank to skip):%n",
        driverClass);
    
    while (true) {
      String path = readLine();
      if (path.isBlank())
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
  
  
  private boolean setDriverClass(String url, Consumer<String> func) {
    printf(
        "%nDo you need to set the JDBC Driver class associated with the connection URL%n <%s> ?%n%n",
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
        return true;
      }
      console.printf("%nNot a valid Java class name. Try again:%n");
    }
  }



  private void doAddConInfoLoop(BiConsumer<String,String> func, String conName) {

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
      func.accept(conName, value);
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
  
  
  

  private boolean malformedUrl(String url) {
    return url.length() < 10 || ! url.toLowerCase().startsWith("jdbc:");
  }


  private int delimiterIndex(String line) {
    int equalIndex = supIndexOf(line, '=');
    int colonIndex = supIndexOf(line, ':');
    int supIndex = Math.min(equalIndex, colonIndex);
    return supIndex == line.length() ? -1 : supIndex;
  }
  
  private int supIndexOf(String line, char c) {
    int index = line.indexOf(c);
    return index == -1 ? line.length() : index;
  }
  
  
  private String readLine() {
    String line = console.readLine();
    if (line == null)
      giveupAbort();
    return line.trim();
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
  
  
  private void giveupAbort() {
    System.err.println();
    System.err.println("OK, giving up. Aborting.");
    System.err.println();
    System.exit(Sldg.ERR_USER);
  }
  
  
  
  
  /*
   * The following pause 100 millis per line to draw user's attention..
   */
  private void printf(String format, Object... args) {
    pause();
    sldg.printf(format, args);
  }
  
  
  private void pause() {
    pause(100);
  }
  
  private void pause(long millis) {
    if (makeItBrief)
      return;
    try {
      ThreadUtils.ensureSleepMillis(millis);
    } catch (InterruptedException ix) {
      Thread.currentThread().interrupt();
    }
  }
  
  
  
}