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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import io.crums.client.ClientException;
import io.crums.io.FileUtils;
import io.crums.model.Crum;
import io.crums.model.CrumRecord;
import io.crums.model.CrumTrail;
import io.crums.sldg.Path;
import io.crums.sldg.SkipLedger;
import io.crums.sldg.cache.HashFrontier;
import io.crums.sldg.fs.Filenaming;
import io.crums.sldg.logs.text.ContextedHasher.Context;
import io.crums.sldg.mrsl.MorselFile;
import io.crums.sldg.logs.text.ContextedHasher;
import io.crums.sldg.logs.text.Fro;
import io.crums.sldg.logs.text.FullRow;
import io.crums.sldg.logs.text.Grammar;
import io.crums.sldg.logs.text.HashingGrammar;
import io.crums.sldg.logs.text.LogHashRecorder;
import io.crums.sldg.logs.text.LogledgeConstants;
import io.crums.sldg.logs.text.OffsetConflictException;
import io.crums.sldg.logs.text.RowHashConflictException;
import io.crums.sldg.logs.text.Seal;
import io.crums.sldg.logs.text.Sealer;
import io.crums.sldg.logs.text.State;
import io.crums.sldg.logs.text.StateHasher;
import io.crums.sldg.packs.MorselPackBuilder;
import io.crums.sldg.src.ColumnValue;
import io.crums.sldg.src.SourceRow;
import io.crums.sldg.time.TrailedRow;
import io.crums.sldg.time.WitnessRecord;
import io.crums.util.IntegralStrings;
import io.crums.util.Lists;
import io.crums.util.Strings;
import io.crums.util.TaskStack;
import io.crums.util.main.NumbersArg;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.StdExit;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Mixin;
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
        "on configuration options available via: @|bold jurno help " + SealCmd.NAME + "|@",
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
        ListCmd.class,
        History.class,
        SealCmd.class,
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
  
  static void printSeal(Seal seal) {
    System.out.println("  row  no.: " +  seal.rowNumber());
    System.out.println("  row hash: " +
        IntegralStrings.toHex(seal.hash().limit(4)) + "..");
    System.out.print("  wit date: ");
    if (seal.isTrailed())
      System.out.println(new Date(seal.trail().get().utc()));
    else
      System.out.println(Ansi.AUTO.string("@|fg(yellow) pending|@"));
  }
  
  static void printFancySeal(Seal seal) {
    String label = seal.isTrailed() ? "[@|fg(green),bold SEAL|@]:" : "[@|fg(blue),bold SEAL|@]:";
    System.out.println(Ansi.AUTO.string(label));
    printSeal(seal);
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
    
    boolean one = first.no() == last.no();
    
    String pad;
    {
      int width =
          Long.toString(last.no()).length() -
          Long.toString(first.no()).length();
      var space = new StringBuilder();
      while (space.length() < width)
        space.append(' ');
      pad = space.toString();
    }
    
    printer.incrIndentation(2);
    printer.print("%s[%d] %s".formatted(
        pad,
        first.no(),
        new Date(first.utc()).toString()));
    
    if (one) {
      printer.println();
    } else {
      printer.println("  (first)");
      printer.println("[%d] %s  (last)".formatted(
          last.no(),
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
  
  
  private final static String FILE = "FILE";
  
  
  @Parameters(
      arity = "1",
      paramLabel = FILE,
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
  
  
  public File getJournalFile() throws ParameterException {
    if (journalFile == null)
      throw new ParameterException(spec.commandLine(), "missing " + FILE + " parameter");
    return journalFile;
  }
  
  
  public Optional<Seal> loadSeal() throws IOException, ParameterException {
    return Sealer.loadForLog(getJournalFile());
  }
  
  
  public Optional<LogHashRecorder> loadRecorder(boolean readOnly) throws IOException {
    getJournalFile(); // ensure not null
    if (!LogHashRecorder.trackDirExists(journalFile))
      return Optional.empty();
    var recorder = new LogHashRecorder(journalFile, readOnly);
    return Optional.of(recorder);
  }
  
  
  public boolean isTracked() {
    getJournalFile(); // ensure not null
    return
        LogHashRecorder.trackDirExists(journalFile) ||
        Sealer.getSealFile(journalFile).isPresent();
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
  

  ParameterException notTracked() {
    return new ParameterException(
        spec.commandLine(),
        getJournalFile() + " is not tracked");
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
      
      LogHashRecorder recorder;
      {
        var r = jurno.loadRecorder(true);
        if (r.isPresent()) {
          recorder = r.get();
          closer.pushClose(recorder);
        } else {
          var file = jurno.getJournalFile();
          var s = Sealer.loadForLog(file);
          
          if (s.isEmpty()) {
            printNotTracked(file);
            return;
          }
          
          Seal seal = s.get();
          Jurno.printFancySeal(seal);
          if (printGrammar)
            printGrammar(seal.rules().grammar());
          
          System.out.println();
          return;
        }
        
      }
      
      var state = recorder.getState();
      if (state.isEmpty()) {
        printNotTracked(recorder.getJournal());
        return;
      }
      
      State s = state.get();
      Jurno.printFancyState(s);
      var lastTrail = recorder.lastTrailedRow();
      System.out.print("  wit date: ");
      if (lastTrail.isEmpty()) {
        System.out.println(Ansi.AUTO.string("@|fg(yellow) pending|@"));
      } else {
        long witRn = lastTrail.get().no();
        long rn = state.get().rowNumber();
        if (witRn == rn)
          System.out.println(new Date(lastTrail.get().utc()));
        else if (witRn < rn)
          System.out.println(Ansi.AUTO.string("@|fg(yellow) pending|@"));
        else
          throw new IllegalStateException("witRn " + witRn + " > last rn " + rn);
      }
      System.out.println();
      long lenDiff = recorder.getJournal().length() - s.eolOffset();
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
        printGrammar(recorder.grammar());
      }

      System.out.println();
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  static void printGrammar(Grammar grammar) {
    System.out.println();
    System.out.println(Ansi.AUTO.string("[@|fg(cyan) GRAMMAR|@]:"));
    System.out.print(Ansi.AUTO.string("  Comment-line prefix (@|faint <>|@): "));
    printQuotedSpecial(grammar.commentPrefix(), "not enabled");
    System.out.print(Ansi.AUTO.string("     Token delimiters (@|faint <>|@): "));
    printQuotedSpecial(grammar.tokenDelimiters(), "whitespace");
    
  }
  
  
  private static void printQuotedSpecial(Optional<String> str, String fallback) {
    if (str.isEmpty())
      System.out.println(Ansi.AUTO.string("@|bold " + fallback + "|@"));
    else {
      System.out.print(Ansi.AUTO.string("  @|faint <|@"));
      System.out.print(escapeFunkyWs(str.get()));
      System.out.println(Ansi.AUTO.string("@|faint >|@"));
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
        "Invoke '@|bold " + SealCmd.NAME + "|@' to begin tracking."));
  }
}




@Command(
    name = ListCmd.NAME,
    description = {
        "Lists the ledgerable lines in the log",
        "",
        "Blank and comment-lines (if enabled), are not ledgered and hence ignored. If the",
        "file is not yet tracked, then this is a dry run and you can experiment with the",
        "token delimiters (@|fg(yellow) " + GrammarSettings.DEL  + "|@, @|fg(yellow) " + GrammarSettings.DEL_PLUS +  "|@) and comment-line prefix",
        "(@|fg(yellow) " + GrammarSettings.COM + "|@) settings.",
        "",
    }
    )
class ListCmd implements Callable<Integer> {
  
  final static String NAME = "list";


  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  
  
  
  
  private List<Long> rns = List.of();
  
  
  @Option(
      names = "--eol",
      description = {
          "Show end-of-line offsets",
          "By default, EOL offsets are @|italic not|@ listed."
      }
      )
  private boolean showEol;
  
  
  @Option(
      names = "--no-lineNos",
      description = {
          "Don't show line no.s",
          "By default, line no.s are listed."
      }
      )
  private boolean noLineNums;
  
  @Option(
      names = "--rows",
      paramLabel = "NUMS",
      description = {
          "Comma separated list of row no.s and/or ranges",
          "Ranges are expressed using dashes. Example:",
          "    2,3,53-58,99"
      })
  public void setRows(String rows) {
    var nums = NumbersArg.parse(rows);
    if (nums == null)
      throw new ParameterException(spec.commandLine(), "Cannot parse '" + rows + "'");
    
    if (nums.isEmpty())
      return;
    
    this.rns = Lists.sortRemoveDups(nums);
    if (rns.get(0) < 1)
      throw new ParameterException(spec.commandLine(),
          "Illegal row number %d in '%s'".formatted(rns.get(0), rows));
  }
  
  
  @Mixin
  private GrammarSettings grammarSettings = new GrammarSettings();
  
  
  
  
  @Override
  public Integer call() throws IOException {
    
    boolean dry = !jurno.isTracked();
    if (!dry)
      grammarSettings.checkNotSet(spec);
    
    long rows;
    
    if (rns.isEmpty()) {
      rows = printAllRows();
    } else {
      printSelectedRows();
      rows = rns.size();
    }

    System.out.println();
    if (dry)
      System.out.println(Ansi.AUTO.string("@|fg(yellow),bold Dry run|@ (@|italic file not tracked|@)"));
    
    System.out.println(Strings.nOf(rows, "row") + " listed.");
    return 0;
  }
  
  
  
  
  private long printAllRows() throws IOException {
    final File log = jurno.getJournalFile();
    try (var closer = new TaskStack()) {
      
      Context printer = new Context() {
        @Override
        public void observeRow(
            HashFrontier preFrontier, List<ColumnValue> cols, long offset,
            long endOffset, long lineNo) throws IOException {
          printRow(preFrontier.rowNumber() + 1, cols, endOffset, lineNo);
        }
      };
      
      StateHasher hasher;
      
      if (!jurno.isTracked()) {
        hasher = new HashingGrammar(grammarSettings.getGrammar()).stateHasher();
      } else {
        var r = jurno.loadRecorder(true);
        if (r.isPresent()) {
          closer.pushClose(r.get());
          hasher = r.get().stateHasher();
        } else {
          hasher = Sealer.loadForLog(log)
              .map(s -> s.rules().stateHasher()).get();
        }
      }
      
      return 
          new ContextedHasher(hasher, printer).play(log)
          .rowNumber();
    }
  }
  
  
  
  private void printSelectedRows() throws IOException {
    final File log = jurno.getJournalFile();
    try (var closer = new TaskStack()) {
      
      var r = jurno.loadRecorder(true);

      List<FullRow> fullRows;
      if (r.isPresent()) {
        closer.pushClose(r.get());
        fullRows = r.get().getFullRows(rns);
      } else {
        StateHasher hasher = Sealer.loadForLog(log)
            .map(s -> s.rules().stateHasher()).orElse(
                new HashingGrammar(grammarSettings.getGrammar()).stateHasher());
        
        fullRows = hasher.getFullRows(rns, log);
      }
      
      
      for (var row : fullRows)
        printRow(row.rowNumber(), row.columns(), row.eolOffset(), row.lineNo());

    } catch (NoSuchElementException nsex) {
      throw new ParameterException(spec.commandLine(), nsex.getMessage());
    }
  }
  
  
  
  private void printRow(long rowNo, List<ColumnValue> cols, long endOffset, long lineNo) {
    var out = System.out;
    out.print(Ansi.AUTO.string(
        "%s[@|fg(yellow) %d|@]".formatted(numPadding(rowNo), rowNo)
        ));
    if (!noLineNums)
      out.print(Ansi.AUTO.string(
          showEol ?
              " %s@|faint %d|@".formatted(numPadding(lineNo), lineNo) :
              " @|faint %d|@ %s".formatted(lineNo, numPadding(lineNo))
          ));
    
    if (showEol)
      out.print(Ansi.AUTO.string(":@|faint %d|@%s".formatted(endOffset, numPadding(endOffset))));
    
    
    for (var col : cols) {
      out.print(' ');
      out.print(col.getValue());
    }
    out.println();
  }
  
  
  private String numPadding(long rn) {
    if (rn >= NO_PAD_MIN)
      return "";
    if (rn >= 100)
      return " ";
    if (rn >= 10)
      return "  ";
    return   "   ";
  }
  
  private final static long NO_PAD_MIN = 1_000;
  
  
  
  
  
  
}


class GrammarSettings {
  
  final static String COM = "--com";
  final static String DEL = "--delimiters";
  final static String DEL_PLUS = "--delimiters-plus";

  final static String WS = " \n\r\t\f";
  private final static String DELCHARS = "DELCHARS";



  @Option(
      names = COM,
      paramLabel = "PREFIX",
      description = {
          "Lines starting with the given prefix are ignored;",
          "they're treated as if they are comments."
      }
      )
  String commentPrefix;



  @Option(
      names = DEL,
      paramLabel = DELCHARS,
      description = {
          "Chars defining how text is divided into tokens (words).",
          "By default, these are the standard whitespace characters.",
          "See also: @|fg(yellow) " + DEL_PLUS + "|@ option"
      })
  String delimiters;




  @Option(
      names = DEL_PLUS,
      paramLabel = DELCHARS,
      description = {
          "Space@|italic,bold +|@ char delimiters. Adds the the given",
          "characters to the default (whitespace) delimiter set."
      })
  public void setWsPlusDelimters(String delimiters) {
    this.delimiters = delimiters.isBlank() ? null : WS + delimiters;
  }
  
  
  Grammar getGrammar() {
    return new Grammar(delimiters, commentPrefix);
  }
  
  public void checkArgs(CommandSpec spec) {
    if (delimiters == null)
      return;
    
    var tokenSet = new TreeSet<Character>();
    for (int index = delimiters.length(); index-- > 0; )
      tokenSet.add(delimiters.charAt(index));
    if (tokenSet.size() != delimiters.length())
      throw new ParameterException(spec.commandLine(),
          DELCHARS + " contains duplicates (quoted): '" + delimiters + "'");
  }
  

  public void checkNotSet(CommandSpec spec) {
    String illegal;
    if (delimiters != null)
      illegal = DEL + " or " + DEL_PLUS;
    else if (commentPrefix != null)
      illegal = COM;
    else
      return;
    
    throw new ParameterException(spec.commandLine(), "illegal " + illegal + " init option");
  }
  
}


class DexSetting {
  
  
  final static String DEX = "--dex";
  

  @Option(
      names = DEX,
      paramLabel = "EXPONENT",
      description = {
          "Defines the row-delta: 2 to the power of given exponent.",
          "When set, then hashes and offsets of rows numbered at",
          "multiples of the row-delta are also recorded. (The hash",
          "of the last row is always recorded in any case.)",
          "Valid values: 0 thru 63",
          "Default: 63 (means not enabled)"
      })
  int dex = LogHashRecorder.NO_DEX;
  
  void checkArg(CommandSpec spec) {
    if (dex < 0 || dex > LogHashRecorder.NO_DEX)
      throw new ParameterException(spec.commandLine(),
          DEX + " out-of-bounds: " + dex);
    
  }
}


class RepoSettings {
  
  final static String DEX = "--dex";
 
 
  @Mixin
  GrammarSettings grammarSettings = new GrammarSettings();
 
  int dex() {
    return dexSetting.dex;
  }
  
  @Mixin
  DexSetting dexSetting = new DexSetting();
  
  
  
  Grammar getGrammar() {
    return grammarSettings.getGrammar();
  }
  
  
  boolean hasBlocks() {
    return dex() != LogHashRecorder.NO_DEX;
  }
  
  
  public void checkArgs(CommandSpec spec) {
    dexSetting.checkArg(spec);
    grammarSettings.checkArgs(spec);
  }
  
  
  
  public void checkNotSet(CommandSpec spec) {
    if (dex() != LogHashRecorder.NO_DEX)
      throw new ParameterException(spec.commandLine(), "illegal " + DEX + " init option");
    checkGrammarNotSet(spec);
  }
  
  
  public void checkGrammarNotSet(CommandSpec spec) {
    grammarSettings.checkNotSet(spec);
  }
  
}










@Command(
    name = SealCmd.NAME,
    description = {
        "Tracks and seals journal state",
        "The hash of the last ledgered row is recorded.",
        "Use @|fg(yellow) " + SealCmd.ADD_OPT + "|@ to limit how many rows are added.",
        "",
        "@|underline First-pass Options:|@",
        "",
        "Both parsing grammar and repo storage parameters can be customized on the",
        "first pass thru the following options:",
        "",
        "  @|fg(yellow) " + GrammarSettings.COM + "|@ for comment-lines",
        "  @|fg(yellow) " + GrammarSettings.DEL + "|@, @|fg(yellow) " + GrammarSettings.DEL_PLUS + "|@ for parsing grammar",
        "  @|fg(yellow) " + RepoSettings.DEX + "|@ for how fine-grained hash info is recorded",
        "",
        "This information is recorded in and read from the tracking files thereafter.",
        "Tracking files are maintained as either a single hidden seal file or under a",
        "repo directory whose name is prefixed @|faint " + LogledgeConstants.PREFIX + "|@ followed by the journal's",
        "filename. Seal files are named similarly, but with the @|faint " + LogledgeConstants.SEAL_EXT + "|@ or @|faint " + LogledgeConstants.PSEAL_EXT + "|@",
        "extensions added to signify complete or pending-witness seals, resp.",
        "",
        "Seals record the minimum data necessary for documenting the witnessed state",
        "of ledgers and are suited for one-shot, write-once logs. Unless overridden,",
        "journals (logs) are first tracked and recorded using hidden seal files.",
        "",
        "Repo directories are better suited for evolving, append-only ledgers. They",
        "may contain multiple witness records (a seal contains at most one), as well",
        "as optional auxilliary files designed to speed up lookups.",
        "",
    })
class SealCmd implements Callable<Integer> {
  
  final static String NAME = "seal";
  final static String ADD_OPT = "--add";

  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  
  @Mixin
  private RepoSettings settings = new RepoSettings();
  
  
  
  private int max = -1;
  
  
  @Option(
      names = ADD_OPT,
      paramLabel = "NEW_ROWS",
      description = {
          "Maximum number of rows added. If not specified, then all",
          "remaining ledgerable lines are appended to the ledger."
      })
  public void setMaxNewRows(int max) {
    if (max <= 0)
      throw new ParameterException(
          spec.commandLine(),
          "invalid " + ADD_OPT + " argument " + max + " (must be greater than zero)");
    this.max = max;
  }
  
  
  
  @Option(
      names = "--repo",
      description = {
          "Ensures a repo is created for the journal",
          "(If @|fg(yellow) " + RepoSettings.DEX + "|@ is set then a repo is created anyway.)",
      }
      )
  private boolean createRepo;
  
  
  
  @Option(
      names = "--witness",
      description = {
          "Witness the final hash",
          "You must follow up with @|bold " + Witness.NAME + "|@ a few minutes after",
          "in order to save the permanently verifiable crumtrail",
          "(witness record)."
      }
      )
  private boolean witness;
  
  

  @Override
  public Integer call() throws IOException {
    try (var closer = new TaskStack()) {
      
      var file = jurno.getJournalFile();
      if (file == null)
        throw new ParameterException(spec.commandLine(), "required journal file parameter missing");
      
      LogHashRecorder recorder;
      
      if (LogHashRecorder.trackDirExists(file)) {
        
        settings.checkNotSet(spec);
        
        recorder = new LogHashRecorder(file);
        closer.pushClose(recorder);
        
        System.out.println("Updating..");
      
      } else {  // no repo
        
        var existingSeal = Sealer.loadForLog(file);
        
        
        if (existingSeal.isEmpty()) { // no seal
          
          settings.checkArgs(spec);
          printGrammarGreetings();
          
          boolean createSeal = !createRepo && !settings.hasBlocks() && max == -1;
          
          if (createSeal) {
            System.out.print("Generating seal..");
            System.out.flush();
            Seal seal = Sealer.seal(file, settings.getGrammar());
            System.out.println(Ansi.AUTO.string(" @|bold done|@."));
            System.out.println(Strings.nOf(seal.rowNumber(), "row") + " sealed.");
            
            return witness ? Witness.witnessSealAndReport(file) : 0;
          }
          

          System.out.print("Creating repo..");
          System.out.flush();
          recorder = new LogHashRecorder(file, settings.getGrammar(), settings.dex());
          closer.pushClose(recorder);
          System.out.println(Ansi.AUTO.string(" @|bold done|@."));
          
        } else { // gotta seal
          
          Seal seal = existingSeal.get();
          settings.checkArgs(spec);
          settings.checkGrammarNotSet(spec);

          System.out.print("Verifying seal..");
          System.out.flush();
          
          boolean brokenSeal;
          try {
            Fro fro = seal.verify(file);
            
            brokenSeal = false;
            System.out.println(Ansi.AUTO.string(" @|bold done|@."));
            System.out.println(Strings.nOf(seal.rowNumber(), "row") + " in seal.");
            
            if (fro.rowNumber() == seal.rowNumber()) {
              System.out.println("Seal hash up to date with file contents.");
              return witness && !seal.isTrailed() ? Witness.witnessSealAndReport(file) : 0;
            }
            
          } catch (RowHashConflictException rhcx) {
            if (seal.isTrailed()) {
              System.err.println(Ansi.AUTO.string(" @|fg(red),bold error|@!"));
              System.err.println(Ansi.AUTO.string("[@|fg(red),bold HASH|@]: witnessed seal hash conficts with source."));
              System.err.println(Ansi.AUTO.string("Detail: @|italic " + rhcx.getMessage() + "|@"));
              return 1;
            }
            brokenSeal = true;
            System.out.println(Ansi.AUTO.string(" @|fg(yellow),bold warning|@!"));
            System.out.println(Ansi.AUTO.string("[@|fg(yellow),bold HASH|@]: seal hash conficts with source."));
          }
          
          boolean updateSeal = !brokenSeal && !createRepo && !settings.hasBlocks() && max == -1 && !seal.isTrailed();
          
          if (updateSeal) {
            
            System.out.print("Replacing seal..");
            
            var sFile = LogledgeConstants.pendingSealFile(file);
            sFile.delete();
            Seal replacement;
            {
              State state = seal.rules().stateHasher().play(file);
              replacement = new Seal(state.rowNumber(), state.rowHash(), seal.rules());
            }
            Sealer.writeSeal(replacement, sFile);
            System.out.println(Ansi.AUTO.string(" @|bold done|@."));
            
            return witness ? Witness.witnessSealAndReport(file) : 0;
          }
          
          // - - - - - -
          
          if (brokenSeal)
            System.out.print("Creating repo ..");
          else
            System.out.print("Initializing repo up to sealed row [" + seal.rowNumber() + "] ..");
          
          System.out.flush();
          recorder = new LogHashRecorder(file, seal.rules(), settings.dex());
          closer.pushClose(recorder);
          
          

          boolean cleaned;
          if (brokenSeal) {
            cleaned = (seal.isTrailed() ?
                LogledgeConstants.sealFile(file) :
                  LogledgeConstants.pendingSealFile(file)).delete();
            
          } else {

            // update the recorder up to the seal
            recorder.update(seal.rowNumber());
            
            if (seal.isTrailed()) {
              var trail = seal.trail().get();
              var cRecord =
                  new CrumRecord() {
                    @Override
                    public CrumTrail trail() {
                      return trail.trail();
                    }
                    @Override
                    public boolean isTrailed() {
                      return true;
                    }
                    @Override
                    public Crum crum() {
                      return trail().crum();
                    }
                  };
              boolean added = recorder.addTrail(new WitnessRecord(trail, cRecord));
              if (!added)
                throw new RuntimeException("Failed assertion: trail " + trail + " not added");
              System.out.println("Trail " + trail + " in seal added to repo.");
              cleaned = LogledgeConstants.sealFile(file).delete();
            } else
              cleaned = LogledgeConstants.pendingSealFile(file).delete();
          }
          
          
          if (!cleaned) {
            var out = System.err;
            out.print(Ansi.AUTO.string("[@|fg(yellow),bold WARNING|@]:"));
            out.println(" failed to clean up ununsed seal file.");
          }
          
        } // end gotta seal

        System.out.println();
        
      } // end no repo
      
      
      var preState = recorder.getState();
      var state = max == -1 ? recorder.update() : recorder.update(max);
      
      Jurno.printFancyState(state);

      System.out.println();
      long rowsAdded = state.rowNumber() - preState.map(State::rowNumber).orElse(0L);
      System.out.println(Ansi.AUTO.string(
          "@|bold %d|@ %s added.".formatted(
              rowsAdded,
              Strings.pluralize("row", rowsAdded))));
      
      if (witness)
        Witness.witnessAndReport(recorder);
      
      
      return 0;
    }
  }
  
  
  
  
  private void printGrammarGreetings() {

    var grammar = settings.grammarSettings;
    String msg = Ansi.AUTO.string("[@|fg(blue) STARTING|@]: " + jurno.getJournalFile());
    System.out.println(msg);
    msg = "  token delimiters: ";
    
    
    String delimiters = grammar.delimiters;
    if (delimiters == null)
      msg += "@|bold whitespace|@";
    else if (delimiters.startsWith(GrammarSettings.WS))
      msg += "@|bold whitespace|@@|bold,italic +|@ " + delimiters.substring(GrammarSettings.WS.length());
    else
      msg += "@|italic custom|@";
    
    msg = Ansi.AUTO.string(msg);
    System.out.println(msg);
    
    String commentPrefix = grammar.commentPrefix;
    if (commentPrefix == null || commentPrefix.isEmpty())
      msg = "  comment-lines not enabled";
    else
      msg = "  comment-line prefix: '" + commentPrefix + "'";
    System.out.println(msg);
    
    
    int dex = settings.dex();
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
  }
  
  
}

@Command(
    name = Witness.NAME,
    description = {
        "Witnesses the last ledgered row (line) in the journal",
        "",
        "Requires a network connection.",
        "Because witness records take a few minutes to cure, this is a 2-step process.",
        "Rows actually witnessed are those recorded at conclusion of the @|bold " + SealCmd.NAME + "|@ command.",
        "Not all witness records are saved: if 2 records share the same time, then the",
        "one at the lower row number is discarded.",
        "",
    })
class Witness implements Callable<Integer> {
  
  final static String NAME = "witness";
  


  
  @ParentCommand
  private Jurno jurno;
  
  @Spec
  private CommandSpec spec;
  
  

  @Override
  public Integer call() throws IOException {
    
    var r = jurno.loadRecorder(false);
    
    if (r.isPresent()) try (var recorder = r.get()) {
      witnessAndReport(recorder);
    } else {
      var file = jurno.getJournalFile();
      
      Seal seal = Sealer.loadForLog(file).orElseThrow(
          () -> new ParameterException(
              spec.commandLine(),
              "File not tracked. Invoke '" + SealCmd.NAME + "' first."));
      
      if (seal.isTrailed()) {
        System.out.println("Already witnessed.");
        Jurno.printFancySeal(seal);
      
      } else
        return witnessSealAndReport(file);
    }
    
    return 0;
  }
  
  
  
  static void witnessAndReport(LogHashRecorder recorder) throws IOException {
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
        time = Strings.nOf(agoSeconds, "second");
      else {
        time = Strings.nOf(agoSeconds / 60, "minute");
      }
    }
    System.out.println(
        "Last row [%d] witnessed %s ago; final record pending.".formatted(
            last.rowNum(), time));
  }
  
  
  static int witnessSealAndReport(File log) throws IOException {
    try {
      
      System.out.print("Witnessing seal..");
      System.out.flush();
      Optional<Seal> s = Sealer.witnessLog(log);
      if (s.isEmpty()) {
        System.err.println(Ansi.AUTO.string(" @|fg(red),bold failed|@: seal not found"));
        return 1;
      }
      System.out.println(Ansi.AUTO.string(" @|bold done|@."));
      
      var seal = s.get();
      if (seal.isTrailed())
        Jurno.printFancySeal(seal);
      else
        System.out.println(Ansi.AUTO.string(
            "Use @|bold " + Witness.NAME + "|@ in a few minutes to save the witness record."));
      
      return 0;
    
    } catch (ClientException remoteError) {
      
      System.err.print(Ansi.AUTO.string("[@|fg(yellow),bold WARNING|@]:"));
      System.err.println(Ansi.AUTO.string(" Failed to witness seal. This can be caused by a network/service error."));
      System.err.println(Ansi.AUTO.string("Error message: @|italic " + remoteError.getMessage() + "|@"));
      return 1;
    }
  }
  
}

@Command(
    name = Verify.NAME,
    description = {
        "Verifies ledgered lines in the journal have not been modified",
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
      var sealOpt = jurno.loadSeal();
      if (sealOpt.isEmpty()) {
        Status.printNotTracked(jurno.getJournalFile());
        return 1;
      }
      Seal seal = sealOpt.get();
      try {
        Fro fro = seal.verify(jurno.getJournalFile());
        boolean extended = fro.rowNumber() > seal.rowNumber();
        printVerified(extended ? fro.preState() : fro.state());
        if (extended)
          System.out.println("More ledgerable lines remain to be sealed.");
        else
          System.out.println("Seal is up to date.");
        return 0;
      } catch (RowHashConflictException rhcx) {
        Jurno.printConflict(rhcx);
        return 1;
      }
    }
    var recorder = recorderOpt.get();
    try (recorder) {
      
      var state = recorder.verify();
      printVerified(state);
      return 0;
      
    } catch (OffsetConflictException ocx) {
      jurno.printConflictAdvice(ocx);
    
    } catch (RowHashConflictException rhcx) {
      jurno.printConflictAdvice(rhcx);
      
    }
    return 1;
  }
  
  
  
  private void printVerified(State state) {
    System.out.println(Ansi.AUTO.string("[@|fg(green),bold VERIFIED|@]: up to.."));
    System.out.println(Ansi.AUTO.string(
                        "  row  no.: @|bold " + state.rowNumber() + "|@"));
    System.out.println( "  line no.: " + state.lineNo());
    System.out.println( "  EOL  off: " + state.eolOffset());
    System.out.println();
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
        "If the contents and order of the ledgerable lines have not logically changed but",
        "their offsets and/or line no.s have (whether caused by the addition or removal of",
        "whitespace characters or comment-lines) then this command will repair them.",
        "Note: seals do not record offsets, so this command does not apply to them.",
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
      if (jurno.loadSeal().isEmpty()) {
        System.out.println("File is not tracked: there are no offsets to repair.");
      } else {
        System.out.println(
            "File tracked using seal file: there are no offsets to repair.");
      }
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
      System.out.println(jurno.getJournalFile() + " is not tracked in a repo.");
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
    
    if (!jurno.isTracked()) {
      Status.printNotTracked(jurno.getJournalFile());
      return;
    }
    
    try (var closer = new TaskStack()) {
      
      var opt = jurno.loadRecorder(true);
      
      if (opt.isPresent()) {
        closer.pushClose(opt.get());
        listRepoHistory(opt.get());
      } else {
        Seal seal = Sealer.loadForLog(jurno.getJournalFile()).get();
        printSealHistory(seal);
      }
      
      
    } catch (IOException iox) {
      throw new UncheckedIOException(iox);
    }
  }
  
  
  private void printSealHistory(Seal seal) {
    if (seal.isTrailed()) {
      System.out.println(Ansi.AUTO.string(
          "  [@|bold %d|@]   %s".formatted(
              seal.rowNumber(),
              new Date(seal.trail().get().utc()))
          ));
    } else {
      System.out.println("No ledgered rows witnessed.");
    }
  }
  
  private void listRepoHistory(LogHashRecorder recorder) {
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
      long rn = trail.no();
      System.out.println(Ansi.AUTO.string(
          "  %s[@|bold %d|@]   %s".formatted(
              pad(rn, maxDigits),
              rn,
              new Date(trail.utc()).toString())
          ));
      witIndex += reverse ? -1 : 1;
    } while (witIndex != lastIndex);
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
        "Creates a morsel file",
        "",
        "Morsel files @|faint (.mrsl)|@ are compact, tamper proof archives of ledger state which",
        "may optionally contain hash proofs of row (line) contents and dates witnessed.",
        "",
        "(Use the @|bold mrsl|@ tool to retrieve information from morsel files.)",
        "",
        "@|underline State Morsel|@:",
        "",
        "A morsel (created with no @|fg(yellow) " + Morsel.SRC_OPT + "|@ option) only asserting how many rows the",
        "ledger had and what the hash of the ledger's last row was. State morsels",
        "serve as ledger state fingerprints: as the ledger evolves with the addition",
        "of new rows, the lineage of its new fingerprint is verifiable against its",
        "older ones.",
        "",
    }
    )
class Morsel implements Callable<Integer> {
  
  final static String NAME = "morsel";
  
  final static String SRC_OPT = "--src";
  
  final static String DST_OPT = "--dest";
  
  final static String REDACT_OPT = "--red";
  
  final static String REPO_OPT = "--repo";

  
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
      names = DST_OPT,
      paramLabel = "PATH",
      description = {
          "Destination morsel filepath",
          "An existing directory, or path to a @|italic new|@ file.",
          "If a directory, then the new file's name is be based",
          "on the current highest row no. and any source row no.s",
          "specified thru the @|fg(yellow) " + SRC_OPT + "|@ option",
          "Default: . @|faint (current directory)|@"
      }
      )
  private File morselFile;
  
  
  private List<Long> srcRns = List.of();
  private Set<String> redactTokens = Set.of();
  
  
  @Option(
      names = SRC_OPT,
      paramLabel = "NUMS",
      description = {
          "Comma separated list of @|italic source|@ row no.s and/or ranges",
          "to include. Ranges are expressed using dashes. Example:",
          "    2,3,53-58,99",
          "(If not set, then it's a @|italic state|@ morsel.)"
      }
      )
  public void setSrcRns(String srcs) {
    var rns = NumbersArg.parse(srcs);
    if (rns == null || rns.isEmpty())
      throw new ParameterException(spec.commandLine(),
          "illegal " + SRC_OPT + " " + srcs);
    
    rns = Lists.sortRemoveDups(rns);
    if (rns.get(0) < 1L)
      throw new ParameterException(spec.commandLine(),
          "illegal row no. [%d]: %s %s".formatted(rns.get(0), SRC_OPT, srcs));
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
      names = REPO_OPT,
      description = {
          "Saves the @|italic state|@ morsel in the default repo location",
          "(Must not be combined with @|fg(yellow) " + SRC_OPT + "|@)",
          "If @|fg(yellow) " + DST_OPT + "|@ is set, then the state morsel",
          "is also @|italic copied|@ to the specified destination path."
      }
      )
  boolean saveInRepo;
  
  
  
  private void checkArgs() {
    boolean stateMorsel = srcRns.isEmpty();
    if (stateMorsel && !redactTokens.isEmpty())
      throw new ParameterException(spec.commandLine(),
          REDACT_OPT + " must be used in combination with " + SRC_OPT);
    if (!stateMorsel && saveInRepo)
      throw new ParameterException(spec.commandLine(),
          REPO_OPT + " may not be used in combination with " + SRC_OPT);
  }
  
  
  
  
  
  @Option(
      names = "--comment",
      paramLabel = "MSG",
      description = {
          "Optional, informal (unverified) embedded message"
      }
      )
  private String comment;
  
  @Override
  public Integer call() throws IOException {
    
    checkArgs();
    
    try (var recorder = jurno.loadRecorder(true).orElseThrow(jurno::notTracked)) {
      
      if (saveInRepo) {
        boolean created = recorder.updateStateMorsel(!noTrails);
        if (created)
          System.out.println("State morsel created and saved in repo.");
        else if (morselFile == null)
          System.out.println("State morsel already in repo.");
        else
          System.out.println("State morsel found in repo.");
        
        if (morselFile != null) {
          var morsel = recorder.getStateMorsel().get();
          
          if (morselFile.isDirectory())
            morselFile = Filenaming.INSTANCE.newMorselFile(morselFile, morsel.getMorselPack());
          
          FileUtils.copy(morsel.getFile(), morselFile);
          System.out.println("Copied to " + morselFile);
        }
        
        return 0;
      }
      
      
      State state = recorder.getState().orElseThrow(jurno::notTracked);
      
      
      
      List<Long> trailedRns = noTrails ? List.of() : recorder.getTrailedRowNumbers();
      
      
      final long hi = state.rowNumber();
      final long lastTrailRn = trailedRns.isEmpty() ? 0 : trailedRns.get(trailedRns.size() - 1);
      
      if (lastTrailRn > hi) {
        throw new IllegalStateException(
            "assertion failure: last row [%d] < last witnessed row [%d]"
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
          trailSet = List.of(recorder.lastTrailedRow().get());
          
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
            
            if (!trailSet.isEmpty() && trailSet.get(trailSet.size() - 1).no() >= srcRn)
              continue;
            
            int index = Collections.binarySearch(trailedRns, srcRn);
            
            if (index < 0) {
              index = -1 - index;
              if (index == tcount)
                break;  // (since srcRns are ascending)
            }
            
            trailSet.add(recorder.getTrailByIndex(index));
            
          } // for (
          
          trailSet.stream().map(TrailedRow::no).forEach(anchorSet::add);
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
      
      
      File file = MorselFile.createMorselFile(morselFile, builder);
      
      System.out.println("Morsel file created: " + file);
      
    } // try (
    
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
  
  
}

















@Command(
    name = Clean.NAME,
    description = {
        "Cleans backup files",
        "",
        "Fixup commands like @|bold " + FixOffsets.NAME + "|@ and @|bold " + Rollback.NAME + "|@ often backup",
        "files before modifying them.",
        ""
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







