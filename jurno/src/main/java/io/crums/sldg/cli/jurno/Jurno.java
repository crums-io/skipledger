/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.cli.jurno;


import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import io.crums.sldg.MorselFile;
import io.crums.sldg.Path;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.logs.text.FullRow;
import io.crums.sldg.logs.text.LogHashRecorder;
import io.crums.sldg.logs.text.LogledgeConstants;
import io.crums.sldg.logs.text.OffsetConflictException;
import io.crums.sldg.logs.text.RowHashConflictException;
import io.crums.sldg.logs.text.State;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.time.TrailedRow;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.Strings;
import io.crums.util.TaskStack;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

/**
 * 
 */
@Command(
    name = "jurno",
    mixinStandardHelpOptions = true,
    version = "jurno 0.5",  // TODO: setup version file
    synopsisHeading = "",
    customSynopsis = {
        "Journal (log) file monitoring, tracking and reporting tool.",
        "",
        "A journal is a text-based file whose contents is never @|italic logically|@ modified but",
        "which may be appended to.",
        "",
        "@|bold Parsing & Hashing:|@",
        "",
        "Each line in the file is potentially ledgerable as a row. Blank or empty lines",
        "do not count; nor do comment-lines, if enabled. Each line is parsed into tokens",
        "(words). By default, tokens are delimited by whitespace characters. A salted",
        "hash of each token is then used to compute the row's @|italic input|@ hash. More info",
        "on configuration options available via: @|bold jurno help " + Seal.NAME + "|@",
        "",
        "@|bold Usage:|@",
        "",
        "  @|bold jurno|@ @|fg(yellow) FILE|@ COMMAND",
        "  @|bold jurno help|@ COMMAND",
        "  @|bold jurno|@ [@|fg(yellow) -hV|@]",
        "",
        },
    subcommands = {
        HelpCommand.class,
        Status.class,
        History.class,
        Seal.class,
        Witness.class,
        Verify.class,
        FixOffsets.class,
        Rollback.class,
        Morsel.class,
        Clean.class,
    })
public class Jurno {
  

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Jurno()).execute(args);
    System.exit(exitCode);
  }

  
  static void printFancyState(State state) {
    System.out.println(Ansi.AUTO.string("[@|fg(green),bold STATE|@]:"));
    printState(state);
  }
  
  
  
  static void printState(State state) {
    System.out.println("  row  no.: " +  state.rowNumber());
    System.out.println("  line no.: " +  state.lineNo());
    System.out.println("  EOL  off: " +  state.eolOffset());
    System.out.println("  row hash: " +
        IntegralStrings.toHex(state.rowHash().limit(4)) + "..");
  }
  
  static void printFancyHistorySummary(LogHashRecorder recorder) {
    System.out.println(Ansi.AUTO.string("[@|fg(blue),bold HISTORY|@]:"));
    printHistorySummary(recorder);
  }
  
  static void printHistorySummary(LogHashRecorder recorder) {

    int tcount = recorder.getTrailCount();
    var printer = new PrintSupport();
    printer.setIndentation(2);
    printer.print(Strings.nOf(tcount, "witnessed row") + " found");
    if (tcount > 0) {
      
      printer.println(":");
      var first = recorder.getTrailByIndex(0);
      var last = tcount > 1 ? recorder.getTrailByIndex(tcount - 1) : first;
      printTrailRange(first, last, printer);
      var pendingTrailedRows = recorder.pendingTrailedRows();
      if (pendingTrailedRows.isEmpty()) {
        printer.println("All ledgered rows witnessed.");
      } else {
        int count = pendingTrailedRows.size();
        printer.print(
            "%s pending witness: [%d]".formatted(
                Strings.nOf(count, "row"),
                pendingTrailedRows.get(0)));
        if (count == 1)
          printer.println();
        else
          printer.println(
              " %s [%d]".formatted(
                  count == 2 ? "and" : "..",
                  pendingTrailedRows.get(count - 1)));
      }
    } else
      printer.println(".");
  }
  
  
  
  static void printTrailRange(TrailedRow first, TrailedRow last, PrintSupport printer) {
    
    boolean one = first.rowNumber() == last.rowNumber();
    
    String pad;
    {
      int width =
          Long.toString(last.rowNumber()).length() -
          Long.toString(first.rowNumber()).length();
      var space = new StringBuilder();
      while (space.length() < width)
        space.append(' ');
      pad = space.toString();
    }
    
    printer.incrIndentation(2);
    printer.print("%s[%d] %s".formatted(
        pad,
        first.rowNumber(),
        new Date(first.utc()).toString()));
    
    if (one) {
      printer.println();
    } else {
      printer.println("  (first)");
      printer.println("[%d] %s  (last)".formatted(
          last.rowNumber(),
          new Date(last.utc()).toString()));
    }
    printer.decrIndentation(2);
  }
  
  
  static void printConflict(RowHashConflictException rhcx) {
    var out = System.err;
    out.println(Ansi.AUTO.string(
        "[@|fg(red),bold HASH|@]: hashes conflict at row [@|fg(red),bold " +
        rhcx.rowNumber() + "|@]"));
    out.println(Ansi.AUTO.string("@|italic " + rhcx.getMessage() + "|@"));
  }
  
  
  static void printConflict(OffsetConflictException ocx) {
    var out = System.err;
    out.println(Ansi.AUTO.string(
        "[@|fg(yellow),bold OFF|@]: offsets conflict at row [@|fg(red),bold " +
        ocx.rowNumber() + "|@]"));
    out.println(Ansi.AUTO.string("@|italic " + ocx.getMessage() + "|@"));
  }
  
  
  
  @Spec
  private CommandSpec spec;
  
  
  private File journalFile;
  
  
  @Parameters(
      arity = "1",
      paramLabel = "FILE",
      description = {
          "Text-based journal (log)",
      })
  public void setJournalFile(File journalFile) {
    this.journalFile = journalFile;
    if (!journalFile.isFile()) {
      throw new ParameterException(spec.commandLine(), "not a file: " + journalFile);
    } else if (!journalFile.canRead()) {
      throw new ParameterException(spec.commandLine(), "need read permission: " + journalFile);
    }
  }
  
  
  public File getJournalFile() {
    return journalFile;
  }
  
  
  public Optional<LogHashRecorder> loadRecorder(boolean readOnly) throws IOException {
    if (!LogHashRecorder.trackDirExists(journalFile))
      return Optional.empty();
    var recorder = new LogHashRecorder(journalFile, readOnly);
    return Optional.of(recorder);
  }
  
  
  void printConflictAdvice(OffsetConflictException ocx) {
    Jurno.printConflict(ocx);
    printAdvice(ocx);
  }
  
  void printConflictAdvice(RowHashConflictException rhcx) {
    Jurno.printConflict(rhcx);
    printAdvice(rhcx);
  }
  
  void printAdvice(OffsetConflictException ocx) {
    var printer = new PrintSupport();
    printer.setIndentation(2);
    printer.println();
    printer.printParagraph(
        """
        The journal has been modified at or before byte-offset %d. Some modifications 
        may be legal: for e.g. empty / blank lines, or new comment lines (if enabled).
        If so, the ledgerable content has remained the same but row (line)
        offsets have changed. See if fixing the saved offsets works:
        """.formatted(ocx.getExpectedOffset())
        );
    printer.println();
    
    printer.println(Ansi.AUTO.string(
        "  jurno " + getJournalFile() + " @|bold " + FixOffsets.NAME + "|@"));
  }
  
  
  
  void printAdvice(RowHashConflictException rhcx) {
    var printer = new PrintSupport();
    printer.println();
    printer.setIndentation(2);

    long rn = rhcx.rowNumber();
    
    if (rn == 1)
      printer.printParagraph(
          """
          The content of the first row (ledgerable line) in the journal has changed. Either restore
          the first row to its previous state, or rebuild a new hash ledger by deleting
          or moving this one.
          """
          );
    else {
      printer.printParagraph(
          """
          The contents of row [%d] (or rows before it) have changed. You may see this error
          if recorded row (line) offsets become stale (for e.g. if blank spaces or comment-lines
          have been added or removed). If so, first try the '%s' command:
          """.formatted(rn, FixOffsets.NAME)
          );
      printer.println();
      printer.println(Ansi.AUTO.string("  jurno %s @|fg(yellow) %s|@".formatted(
          getJournalFile().toString(), FixOffsets.NAME)
          ));
      printer.println();
      printer.println(Ansi.AUTO.string(
          "If that doesn't work, you may need to @|italic rollback|@ the hash ledger. See:"
          ));
      printer.println();
      printer.println(Ansi.AUTO.string(
          "  jurno help @|fg(yellow) %s|@".formatted(Rollback.NAME)
          ));
    }
    printer.println();
    
  }
  
  
  
  
  
  

}





@Command(
    name = "status",
    description = {
        "Prints the status of the journal",
        "",
    }
    )
class Status implements Runnable {
  
  @ParentCommand
  private Jurno jurno;
  
  
  @Option(
      names = "-p",
      description = {
          "Displays special parsing rules:",
          "comment-line prefix, if enabled, and any special token delimiters",
          }
      )
  private boolean printGrammar;
  
  @Override
  public void run() {
    
    try (var closer = new TaskStack()) {
      var file = jurno.getJournalFile();
      if (!LogHashRecorder.trackDirExists(file)) {
        printNotTracked(file);
        return;
      }
      var recorder = new LogHashRecorder(file, true);
      closer.pushClose(recorder);
      var state = recorder.getState();
      if (state.isEmpty()) {
        printNotTracked(file);
        return;
      }
      State s = state.get();
      Jurno.printFancyState(s);
      var lastTrail = recorder.lastTrailedRow();
      System.out.print("  wit date: ");
      if (lastTrail.isEmpty()) {
        System.out.println(Ansi.AUTO.string("@|fg(yellow) pending|@"));
      } else {
        long witRn = lastTrail.get().rowNumber();
        long rn = state.get().rowNumber();
        if (witRn == rn)
          System.out.println(new Date(lastTrail.get().utc()));
        else if (witRn < rn)
          System.out.println(Ansi.AUTO.string("@|fg(yellow) pending|@"));
        else
          throw new IllegalStateException("witRn " + witRn + " > last rn " + rn);
      }
      System.out.println();
      long lenDiff = file.length() - s.eolOffset();
      if (lenDiff >= 0)
        System.out.println(Strings.nOf(lenDiff, "byte") + " not ledgered.");
      else {
        System.out.println(
            Ansi.AUTO.string(
                "[@|fg(yellow),bold WARNING|@]: File has been trimmed by " +
                Strings.nOf(lenDiff, "byte") + "!"));
        System.out.println(
            Ansi.AUTO.string("Invoking @|fg(yellow) fix-offsets|@ may fix the problem;"));
        System.out.println("otherwise, you may have to rollback the ledger.");
      }
      System.out.println();
      Jurno.printFancyHistorySummary(recorder);
      
      if (printGrammar) {
        System.out.println();
        System.out.println(Ansi.AUTO.string("[@|fg(cyan) GRAMMAR|@]:"));
        System.out.print(Ansi.AUTO.string("  Comment-line prefix (@|faint ><|@): "));
        printQuotedSpecial(recorder.commentLinePrefix(), "not enabled");
        System.out.print(Ansi.AUTO.string("     Token delimiters (@|faint ><|@): "));
        printQuotedSpecial(recorder.tokenDelimiters(), "whitespace");
      }

      System.out.println();
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  private void printQuotedSpecial(Optional<String> str, String fallback) {
    if (str.isEmpty())
      System.out.println(Ansi.AUTO.string("@|bold " + fallback + "|@"));
    else {
      System.out.print(Ansi.AUTO.string("  @|faint >|@"));
      System.out.print(escapeFunkyWs(str.get()));
      System.out.println(Ansi.AUTO.string("@|faint <|@"));
    }
  }
  
  static String escapeFunkyWs(String str) {
    str = str.replace("\f", "\\f");
    str = str.replace("\t", "\\t");
    str = str.replace("\r", "\\r");
    str = str.replace("\n", "\\n");
    return str;
  }
  
  static void printNotTracked(File file) {
    System.out.println(file + " is not tracked.");
    System.out.println(Ansi.AUTO.string(
        "Invoke '@|bold " + Seal.NAME + "|@' to begin tracking."));
  }
}


@Command(
    name = Seal.NAME,
    description = {
        "Tracks and seals journal state",
        "The hash of the last ledgered row is recorded.",
        "Use @|fg(yellow) " + Seal.MAX + "|@ to limit how many rows are added.",
        "",
        "@|underline First-pass Options:|@",
        "",
        "Both parsing grammar and storage parameters can be specified on the",
        "first pass at tracking thru the following options:",
        "",
        "  @|fg(yellow) " + Seal.COM + "|@ for comment-lines",
        "  @|fg(yellow) " + Seal.DEL + "|@, @|fg(yellow) " + Seal.DEL_PLUS + "|@ for parsing grammar",
        "  @|fg(yellow) " + Seal.DEX + "|@ for how fine-grained hash info is recorded",
        "",
        "This information is recorded in and read from the tracking files thereafter.",
        "",
    })
class Seal implements Runnable {
  
  final static String NAME = "seal";

  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  
  @Option(
      names = COM,
      paramLabel = "PREFIX",
      description = {
          "Lines starting with the given prefix are ignored;",
          "they're treated as if they are comments."
      }
      )
  private String commentPrefix;
  
 final static String COM = "--com";
  
  
  
  private String delimiters;
  

  @Option(
      names = DEL,
      paramLabel = "DELCHARS",
      description = {
          "Chars defining how text is divided into tokens (words).",
          "By default, these are the standard whitespace characters.",
          "See also: @|fg(yellow) " + DEL_PLUS + "|@ option"
      })
  public void setDelimiters(String delimiters) {
    
  }
  
  final static String DEL = "--delimiters";
  
  @Option(
      names = DEL_PLUS,
      paramLabel = "DELCHARS",
      description = {
          "Space plus char delimiters. Includes the given delimiters",
          "@|italic in addition to whitespace|@ chars."
      })
  public void setWsPlusDelimters(String delimiters) {
    this.delimiters = delimiters.isEmpty() ? null : WS + delimiters;
  }
  
  final static String WS = " \n\r\t\f";
  
  final static String DEL_PLUS = "--delimiters-plus";
  
  private int dex = LogHashRecorder.NO_DEX;
  
  
  @Option(
      names = DEX,
      paramLabel = "EXPONENT",
      description = {
          "Defines the row-delta: 2 to the power of given exponent.",
          "When set, then hashes and offsets of rows numbered at",
          "multiples of the row-delta are also recorded. (The hash",
          "of the last row is always recorded in any case.)",
          "Valid values: 0 thru 63"
      })
  public void setDex(int dex) {
    if (dex < 0 || dex > LogHashRecorder.NO_DEX)
      throw new ParameterException(
          spec.commandLine(),
          DEX + " out-of-bounds: " + dex);
    this.dex = dex;
  }
  final static String DEX = "--dex";
  
  
  private int max = -1;
  
  
  @Option(
      names = MAX,
      paramLabel = "NEW_ROWS",
      description = {
          "Maximum number of rows added. If not specified, then all",
          "remaining ledgerable lines are appended to the ledger."
      })
  public void setMaxNewRows(int max) {
    if (max <= 0)
      throw new ParameterException(
          spec.commandLine(),
          "max-rows " + max + " <= 0");
    this.max = max;
  }
  final static String MAX = "--max";
  
  
  
  
  

  @Override
  public void run() {
    try (var closer = new TaskStack()) {
      var file = jurno.getJournalFile();
      LogHashRecorder recorder;
      if (LogHashRecorder.trackDirExists(file)) {
        // TODO: nag if first-pass args set
        recorder = new LogHashRecorder(file);
        System.out.println("Updating..");
      } else {
        String msg = Ansi.AUTO.string("[@|fg(blue) STARTING|@]: " + file);
        System.out.println(msg);
        msg = "  token delimiters: ";
        if (delimiters == null)
          msg += "@|bold whitespace|@";
        else if (delimiters.startsWith(WS))
          msg += "@|bold whitespace+|@ " + delimiters.substring(WS.length());
        else
          msg += "@|italic custom|@";
        
        msg = Ansi.AUTO.string(msg);
        System.out.println(msg);
        
        if (commentPrefix == null || commentPrefix.isEmpty())
          msg = "  comment-lines not enabled";
        else
          msg = "  comment-line prefix: '" + commentPrefix + "'";
        System.out.println(msg);
        
        System.out.print("  dex: " + dex);
        if (dex == LogHashRecorder.NO_DEX)
          System.out.println(" (no " + LogledgeConstants.OFFSETS_FILE + " file)");
        else if (dex == 0)
          System.out.println(" (every row)");
        else if (dex == 1)
          System.out.println(" (every other row)");
        else {
          long delta = 1L << dex;
          System.out.println(" (every " + Strings.nTh(delta) + " row)");
        }
        
        recorder = new LogHashRecorder(file, commentPrefix, delimiters, dex);

        System.out.println();
      }
      closer.pushClose(recorder);
      
      var preState = recorder.getState();
      
      
      var state = max == -1 ? recorder.update() : recorder.update(max);
      
      Jurno.printFancyState(state);

      System.out.println();
      long rowsAdded = state.rowNumber() - preState.map(State::rowNumber).orElse(0L);
      System.out.println(Ansi.AUTO.string(
          "@|bold %d|@ %s added.".formatted(
              rowsAdded,
              Strings.pluralize("row", rowsAdded))));
      
      
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
}

@Command(
    name = Witness.NAME,
    description = {
        "Witnesses the last ledgered row (line) in the journal",
        "Because witness records take a few minutes to cure, this is a 2-step process.",
        "Rows actually witnessed are those recorded at conclusion of the @|bold " + Seal.NAME + "|@",
        "command. Not all witness records are saved: if 2 or more share the same time,",
        "only the record with the highest row number is saved.",
        "",
    })
class Witness implements Runnable {
  
  final static String NAME = "witness";
  


  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  

  @Override
  public void run() {

    var file = jurno.getJournalFile();
    if (!LogHashRecorder.trackDirExists(file))
      throw new ParameterException(
          spec.commandLine(),
          "File not tracked. Invoke '" + Seal.NAME + "' first.");
    
    try (var recorder = new LogHashRecorder(file)) {
      var records = recorder.witness();
      if (records.isEmpty()) {
        System.out.println("All ledgered rows already witnessed.");
        return;
      }
      var stored = recorder.toStored(records);
      
      System.out.println(
          "%s witnessed; %s stored.".formatted(
          Strings.nOf(records.size(), "row"),
          Strings.nOf(stored.size(), "witness record")));
      
      var last = records.get(records.size() - 1);
      if (last.isTrailed()) {
        System.out.println("All ledgered rows witnessed and recorded.");
        return;
      }
      
      String time;
      {
        long agoSeconds = (System.currentTimeMillis() - last.utc()) / 1000;
        if (agoSeconds < 60)
          time = "seconds";
        else {
          time = Strings.nOf(agoSeconds / 60, "minute");
        }
      }
      System.out.println(
          "Last row [%d] witnessed %s ago; final record pending.".formatted(
              last.rowNum(), time));
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
}

@Command(
    name = Verify.NAME,
    description = {
        "Verifies ledgered lines from the journal have not been modified",
        "",
        "Modifying an already ledgered line is @|italic considered an error|@",
        "unless the only modifications are",
        "",
        "   1. new/deleted blank lines",
        "   2. [white]space between words on a line",
        "   3. new/modified/deleted comment-lines (if comments are enabled)",
        "",
    })
class Verify implements Callable<Integer> {
  
  final static String NAME = "verify";
  


  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  
  

  @Override
  public Integer call() throws IOException {
    
    var recorderOpt = jurno.loadRecorder(true);
    if (recorderOpt.isEmpty()) {
      Status.printNotTracked(jurno.getJournalFile());
      return 1;
    }
    var recorder = recorderOpt.get();
    try (recorder) {
      var state = recorder.verify();
      System.out.println(Ansi.AUTO.string("[@|fg(green),bold VERIFIED|@]: up to.."));
      System.out.println(Ansi.AUTO.string(
                          "  row  no.: @|bold " + state.rowNumber() + "|@"));
      System.out.println( "  line no.: " + state.lineNo());
      System.out.println( "  EOL  off: " + state.eolOffset());
      System.out.println();
      return 0;
      
    } catch (OffsetConflictException ocx) {
      
      jurno.printConflictAdvice(ocx);
    
    } catch (RowHashConflictException rhcx) {

      jurno.printConflictAdvice(rhcx);
      
    }
    return 1;
  }
  
  
  static void printTrailsToLose(LogHashRecorder recorder, long size, PrintSupport printer) {
    long rn = size + 1;
    var trailedRns = recorder.getTrailedRowNumbers();
    int trailsToLose;
    if (trailedRns.isEmpty())
      trailsToLose = 0;
    else {
      int searchIndex = Collections.binarySearch(trailedRns, rn);
      if (searchIndex < 0)
        searchIndex = -1 - searchIndex;
      trailsToLose = trailedRns.size() - searchIndex;
    }
    printer.println(Ansi.AUTO.string(
         "%s to lose on @|fg(yellow) %s|@ to [%d]"
        .formatted(
            Strings.nOf(trailsToLose, "witnessed row"),
            Rollback.NAME,
            rn - 1)
        ));
    
    if (trailsToLose > 0) {
      var first = recorder.getTrailByIndex(trailedRns.size() - trailsToLose);
      var last = trailsToLose > 1 ? recorder.getTrailByIndex(trailedRns.size() - 1) : first;
      Jurno.printTrailRange(first, last, printer);
    }
    
  }
  
}



@Command(
    name = FixOffsets.NAME,
    description = {
        "Attempts to repair recorded EOL offsets and/or line no.s",
        "If the contents and order of the ledgerable lines have not logically changed",
        "but their offsets or the their line numbers have (whether by the addition or",
        "removal of space characters or comment-lines) then this command can fix it.",
        "",
    }
    )
class FixOffsets implements Callable<Integer> {
  
  
  final static String NAME = "fixoffs";
  


  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  
  private long startRn = 0;
  private final static String START_RN = "--startRn";
  
  @Parameters(
      arity = "0,1",
      paramLabel = "START_ROW",
      description = {
          "Starting (minimum) row number whose offsets are corrected",
          "Default: 1."
      })
  public void setStartRn(long startRn) {
    if (startRn < 1)
      throw new ParameterException(
          spec.commandLine(),
          "starting row [%d] must be >= 1".formatted(startRn));
    this.startRn = startRn;
  }
  
  
  
  @Override
  public Integer call() throws IOException {
    
    var opt = jurno.loadRecorder(false);
    if (opt.isEmpty()) {
      Status.printNotTracked(jurno.getJournalFile());
      return 0;
    }
    
    var recorder = opt.get();
    
    var out = System.out;
    
    try {
      
      boolean defaulted = startRn == 0;
      long rn = defaulted ? 1 : startRn;
      out.print(
          "Repairing recorded offsets starting from row [%d]"
          .formatted(rn));
      if (defaulted)
        out.println(" (defaulted)");
      else
        out.println();

      out.print("  ..");
      
      var state = recorder.fixOffsets(rn);
      
      out.println(Ansi.AUTO.string(" @|bold done|@."));
      out.println();
      
      Jurno.printFancyState(state);
      out.println();
      
      return 0;
      
    } catch (OffsetConflictException ocx) {

      out = System.err;
      out.println();
      
      long rn = ocx.rowNumber();
      if (rn >= startRn || !recorder.hasBlocks())
        throw new RuntimeException(
            "Assertion failure: startRn [%d]; ocx [%d]; %s blocks; caused by %s"
            .formatted(startRn, rn, recorder.hasBlocks() ? "has" : "no", ocx.toString()),
            ocx);
      
      Jurno.printConflict(ocx);
      out.println();
      out.println(Ansi.AUTO.string(
          "Retry this command with @|fg(yellow) %s|@ @|bold %d|@"
          .formatted(START_RN, rn)
          ));
      
    } catch (RowHashConflictException rhcx) {

      out = System.err;
      out.println();
      Jurno.printConflict(rhcx);
      
      out.println();
      out.println(Ansi.AUTO.string(
          "If you can't fix the ledgered row, you may have to @|bold %s|@ to row [@|bold %d|@]."
          .formatted(Rollback.NAME, rhcx.rowNumber() - 1)
          ));
    }
    out.println();
    return 1;
  }
}







@Command(
    name = Rollback.NAME,
    description = {
        "Rolls back the ledger to given row no.",
        "",
        "@|italic,bold Warning!|@ @|italic This command destroys history!|@",
        "While ledgered rows cannot be modified, mistakes happen. If discovered early",
        "it may be desirable to undo recently added row by rolling back (trimming) the",
        "number of rows in the ledger.",
        "",
        "Since this can destroy history, interactive confirmation is required.",
        "",
    }
    )
class Rollback implements Callable<Integer> {
  
  
  final static String NAME = "rollback";
  
  private final static String SIZE_LABEL = "SIZE";
  private final static String NO_INTERACT = "--no-console";
  private final static String CONFIRM_NOD = "y";


  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  
  
  private long newSize;
  
  
  @Parameters(
      arity = "1",
      paramLabel = SIZE_LABEL,
      description = {
          "Number of ledgered rows on completion of rollback",
      }
      )
  public void setNewSize(long size) {
    if (size < 1)
      throw new ParameterException(spec.commandLine(), "illegal " + SIZE_LABEL + ": " + size);
    newSize = size;
  }
  
  
  @Option(
      names = NO_INTERACT,
      description = {
          "If set, then no console confirmation is required"
      }
      )
  private boolean nonInteractive;
  

  @Override
  public Integer call() throws IOException {
    
    var opt = jurno.loadRecorder(false);
    if (opt.isEmpty()) {
      printErrorLabel();
      System.out.println(jurno.getJournalFile() + " is not tracked.");
      System.out.println();
      return StdExit.ILLEGAL_ARG.code;
    }
    
    try (var recorder = opt.get()) {
    
      var stateOpt = recorder.getState();
      if (stateOpt.isEmpty()) {
        printErrorLabel();
        Status.printNotTracked(jurno.getJournalFile());
        System.out.println();
        return StdExit.ILLEGAL_ARG.code;
      }
      
      long size = stateOpt.get().rowNumber();
      if (size < newSize)
        throw new ParameterException(spec.commandLine(),
            "%s %d > ledger size %d".formatted(SIZE_LABEL, newSize, size));
      
      if (size == newSize) {
        System.out.println("Ledger already at %d rows. Nothing to rollback.%n".formatted(size));
        return 0;
      }
      
      if (!nonInteractive) {
        var console = System.console();
        if (console == null) {
          System.err.println("No console in interactive mode");
          return StdExit.ILLEGAL_ARG.code;
        }
        long rowsDropped = size - newSize;
        var printer = new PrintSupport();
        printer.println("Preparing to drop " + Strings.nOf(rowsDropped, "row") + "..");
        Verify.printTrailsToLose(recorder, newSize, printer);
        printer.println();
        System.out.print(Ansi.AUTO.string("Confirm by typing '@|bold " + CONFIRM_NOD + "|@': "));
        var nod = console.readLine();
        if (nod == null || !nod.trim().equalsIgnoreCase(CONFIRM_NOD)) {
          System.out.println("Rollback aborted.");
          return 0;
        }
      }
      System.out.print("Rolling back to [%d] ..".formatted(newSize));
      State state = recorder.rollback(newSize);
      System.out.println(Ansi.AUTO.string(" @|bold done|@."));
      System.out.println();
      Jurno.printFancyState(state);
      System.out.println();
      return 0;
      
    } catch (OffsetConflictException ocx) {
      
      var out = System.err;
      out.println(Ansi.AUTO.string("@|fg(red) Rollback failed:|@"));
      Jurno.printConflict(ocx);
      
      out.println();
      
    }
    return 1;
    
  }
  
  
  
  
  void printErrorLabel() {
    System.out.print(Ansi.AUTO.string("[@|fg(red),bold ERROR|@]: "));
  }
  
}


@Command(
    name = History.NAME,
    description = {
        "Lists witnessed row no.s and time witnessed",
        "Each entry establishes the minimum age of the row and every",
        "row ahead of it (since each row's hash depends on the previous",
        "row's hash).",
        "",
    },
    sortOptions = false
    )
class History implements Runnable {
  
  final static String NAME = "history";

  private final static String START_OPT = "--start";
  private final static String COUNT_OPT = "--count";
  private final static String REV_OPT = "--reverse";

  private final static int DEFAULT_COUNT = 100;
  

  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  
  
  private long startRn;
  
  private int maxCount = DEFAULT_COUNT;
  
  
  @Option(
      names = START_OPT,
      paramLabel = "NUM",
      description = {
          "Starting witnessed row no. in listing",
          "@|italic minimum|@ listed witnessed row no.; @|italic maximum|@",
          "witnesseed row no. if @|fg(yellow) " + REV_OPT + "|@ is enabled",
      }
      )
  public void setStartRn(long startRn) {
    if (startRn < 1)
      throw new ParameterException(spec.commandLine(),
          "%s %d < 1".formatted(START_OPT, startRn));
    this.startRn = startRn;
  }
  
  @Option(
      names = COUNT_OPT,
      paramLabel = "COUNT",
      description = {
          "Maximum number of witnessed rows listed",
          "Default: " + DEFAULT_COUNT
      }
      )
  public void setMaxCount(int count) {
    if (count < 1)
      throw new ParameterException(spec.commandLine(),
          "%s %d < 1".formatted(COUNT_OPT, count));
    this.maxCount = count;
  }

  
  @Option(
      names = REV_OPT,
      description = {
          "List in reverse chronological order"
      }
      )
  private boolean reverse;
  
  
  @Override
  public void run() {
    try (var closer = new TaskStack()) {
      
      var opt = jurno.loadRecorder(true);
      if (opt.isEmpty()) {
        Status.printNotTracked(jurno.getJournalFile());
        return;
      }
      
      var recorder = opt.get();
      
      closer.pushClose(recorder);
      
      var witRns = recorder.getTrailedRowNumbers();
      
      int witIndex = getStartIndex(witRns); // (inclusive)
      
      if (witIndex == -1) {
        System.out.println(
            witRns.isEmpty() ?
                "No ledgered rows witnessed." :
                "No witnessed rows matched." );
        return;
      }
      
      final int lastIndex =     // (exclusive)
          reverse ?
              Math.max(-1, witIndex - maxCount) :
                Math.min(witRns.size(), witIndex + maxCount);
              
      final int maxDigits;
      {
        int startDigits = Long.toString(witRns.get(witIndex)).length();
        int lastIndexInc = reverse ? lastIndex + 1 : lastIndex - 1;
        int endDigits = Long.toString(witRns.get(lastIndexInc)).length();
        maxDigits = Math.max(startDigits, endDigits);
      }
      
      do {
        var trail = recorder.getTrailByIndex(witIndex);
        long rn = trail.rowNumber();
        System.out.println(Ansi.AUTO.string(
            "  %s[@|bold %d|@]   %s".formatted(
                pad(rn, maxDigits),
                rn,
                new Date(trail.utc()).toString())
            ));
        witIndex += reverse ? -1 : 1;
      } while (witIndex != lastIndex);
      
      
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  private String pad(long rn, int maxDigits) {
    int spaces = maxDigits - Long.toString(rn).length();
    if (spaces <= 0)
      return "";
    var pad = new StringBuilder(spaces);
    while (spaces-- > 0)
      pad.append(' ');
    return pad.toString();
  }
  
  private int getStartIndex(List<Long> witRns) {
    final int witCount = witRns.size();
    if (witCount == 0)
      return -1;
    if (startRn == 0)
      return reverse ? witRns.size() - 1 : 0;
    
    int searchIndex = Collections.binarySearch(witRns, startRn);
    if (searchIndex >= 0)
      return searchIndex;
    
    int insertIndex = -1 - searchIndex;
    
    return reverse ? insertIndex - 1 // i.e. insertIndex == 0 ? -1 : insertIndex - 1
        : insertIndex == witCount ? -1 : insertIndex;
  }
  
}



@Command(
    name = Morsel.NAME,
    description = {
        "Creates a morsel file (@|faint .mrsl|@)",
        "",
        "Morsel files are compact, tamper proof archives of ledger state which may",
        "optionally contain hash proofs of row (line) contents and dates witnessed.",
        "(Use the @|bold mrsl|@ tool to retrieve information from morsel files,",
        "as well as to redact or merge information.)",
        "",
        "@|underline State Morsel|@:",
        "",
        "A morsel file only asserting how many rows the ledger had and what the hash of",
        "the ledger's last row was. State morsels serve as ledger state fingerprints: as",
        "the ledger evolves with more rows being added, its new fingerprint's lineage is",
        "verifiable against its older fingerprints.",
        "",
    }
    )
class Morsel implements Callable<Integer> {
  
  final static String NAME = "morsel";
  
  private final static String SRC_OPT = "--src";

  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  
  @Option(
      names = "--no-history",
      description = {
          "Witness records are @|italic not|@ included",
          "(Included by default)"
      }
      )
  private boolean noTrails;
  
  
  @Option(
      names = "--dest",
      paramLabel = "PATH",
      description = {
          "Destination morsel filepath",
          "An existing directory, or path to @|italic new|@ file",
          "If a directory, then the new file's name is be based",
          "on the current highest row no. and any source row no.s",
          "specified thru the @|fg(yellow) " + SRC_OPT + "|@ option",
          "Default: . (current directory)"
      }
      )
  private File morselFile;
  
  
  private List<Long> srcRns = List.of();
  private Set<String> redactTokens = Set.of();
  
  
  @Option(
      names = SRC_OPT,
      split = ",",
      description = {
          "Include source contents for specified rows no.s",
          "Separate row no.s using commas (@|faint ,|@)"
      }
      )
  public void setSrcRns(List<Long> rns) {
    if (rns.isEmpty())
      return;
    rns = Lists.sortRemoveDups(rns);
    if (rns.get(0) < 1L)
      throw new ParameterException(
          spec.commandLine(), "illegal source row no. " + rns.get(0));
    srcRns = rns;
  }
  
  
  @Option(
      names = "--red",
      paramLabel = "TOKEN",
      arity = "0..*",
      description = {
          "Redacts the given token (word)",
          "If it occurs in a sourced row (@|fg(yellow) " + SRC_OPT + "|@), then the",
          "token is substituted with its salted hash.",
      }
      )
  public void setRedact(Set<String> tokens) {
    this.redactTokens = tokens;
  }
  
  
  
  
  
  @Option(
      names = "--comment",
      paramLabel = "MSG",
      description = {
          "Optional, informal (not verified) message embedded in morsel"
      }
      )
  private String comment;
  
  @Override
  public Integer call() throws IOException {
    
    try (var recorder = jurno.loadRecorder(true).orElseThrow(this::notTracked)) {
      
      var state = recorder.getState().orElseThrow(this::notTracked);
      
      List<Long> trailedRns = noTrails ? List.of() : recorder.getTrailedRowNumbers();
      
      Optional<TrailedRow> lastTrail = noTrails ? Optional.empty() : recorder.lastTrailedRow();
      
      long lastTrailRn = lastTrail.map(TrailedRow::rowNumber).orElse(0L);
      
      long hi = state.rowNumber();
      
      if (lastTrailRn != 0L) {
        if (lastTrailRn > hi)
          throw new IllegalStateException(
              "assertion failure: last row [%d]; last witnessed [%d]"
              .formatted(hi, lastTrailRn));
      }
      
      if (hi < 2)
        throw new ParameterException(spec.commandLine(), "too few ledgered rows: " + hi);
      
      
      
      List<Long> anchorRns;
      
      List<TrailedRow> trailSet;
      
      if (srcRns.isEmpty()) {
        if (trailedRns.isEmpty()) {
          trailSet = List.of();
          anchorRns =  List.of(1L, hi);
        
        } else {
          trailSet = List.of(lastTrail.get());
          
          anchorRns = (lastTrailRn == 1L || lastTrailRn == hi) ?
              List.of(1L, hi) : List.of(1L, lastTrailRn, hi);
        }
      } else {
        
        var anchorSet = new TreeSet<Long>();
        anchorSet.add(1L);
        anchorSet.addAll(srcRns);
        anchorSet.add(hi);
        
        if (trailedRns.isEmpty())
          trailSet = List.of();
        else {
          
          trailSet = new ArrayList<>();
          final int tcount = trailedRns.size();
          
          
          for (Long srcRn : srcRns) {
            
            if (!trailSet.isEmpty() && trailSet.get(trailSet.size() - 1).rowNumber() >= srcRn)
              continue;
            
            int index = Collections.binarySearch(trailedRns, srcRn);
            
            if (index < 0) {
              index = -1 - index;
              if (index == tcount)
                break;  // (since srcRns are ascending)
            }
            
            trailSet.add(recorder.getTrailByIndex(index));
            
          } // for (
          
          trailSet.stream().map(TrailedRow::rowNumber).forEach(anchorSet::add);
        }
        
        
        anchorRns = anchorSet.stream().toList();
      }
      
      var pathRns = SkipLedger.stitch(anchorRns);
      
      var fullRows = recorder.getFullRows(pathRns);
      
      Path path = new Path(Lists.map(fullRows, FullRow::row));
      
      var builder = new MorselPackBuilder();
      
      builder.initPath(path, srcRns, comment);
      
      
      fullRows.stream().filter(fr -> srcRns.contains(fr.rowNumber()))
      .map(this::redact).forEach(builder::addSourceRow);
      
      trailSet.stream().forEach(builder::addTrail);
      
          
      ensureMorselFile();
      
      File file = MorselFile.createMorselFile(morselFile, builder);
      
      System.out.println("Morsel file created: " + file);
    }
    
    return 0;
  }
  
  
  private SourceRow redact(FullRow row) {
    SourceRow srcRow = row.sourceRow();
    if (redactTokens.isEmpty())
      return srcRow;

    var columns = srcRow.getColumns();
    var colNos = new ArrayList<Integer>();
    for (int index = 0; index < columns.size(); ++index) {
      var col = columns.get(index);
      if (redactTokens.contains(col.getValue()))
        colNos.add(index + 1);
    }
    return srcRow.redactColumns(colNos);
  }
  
  
  
  private ParameterException notTracked() {
    return new ParameterException(spec.commandLine(), jurno.getJournalFile() + " is not tracked");
  }
  
  
  
  private void ensureMorselFile() {
    if (morselFile == null) {
      morselFile = new File(".");
    }
  }
  
}


@Command(
    name = Clean.NAME,
    description = {
        "Cleans backup files",
        "",
        "Fixup commands like @|bold " + FixOffsets.NAME + "|@ and @|bold " + Rollback.NAME + "|@ often backup",
        "files before modifying them."
    }
    )
class Clean implements Callable<Integer> {
  
  final static String NAME = "clean";


  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  
  
  public Integer call() throws IOException {
    var out = System.out;
    var opt = jurno.loadRecorder(true);
    if (opt.isEmpty()) {
      out.println(jurno.getJournalFile() + " is not tracked.");
      out.println("Nothing to do.");
      return 0;
    }
    try (var recorder = opt.get()) {
      int count = recorder.clean();
      if (count == 0) {
        out.println("Ledger already clean.");
        out.println("Nothing to do.");
      } else {
        out.println("Ledger cleaned.");
        out.println(Strings.nOf(count, "backup file") + " deleted.");
      }
    }
    return 0;
  }
}







