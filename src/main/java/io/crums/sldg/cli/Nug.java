/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.util.IntegralStrings.*;

import java.io.File;
import java.io.PrintStream;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import io.crums.server.Constants;
import io.crums.sldg.Nugget;
import io.crums.sldg.Path;
import io.crums.sldg.db.EntitySerializer;
import io.crums.sldg.db.Format;
import io.crums.sldg.db.VersionedSerializers;
import io.crums.util.IntegralStrings;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.TablePrint;

/**
 * 
 */
public class Nug extends MainTemplate {
  
  
  private boolean desc;
  
  private File file;
  
  
  
  
  
  
  
  
  

  // Invoked by main(..)
  private Nug() {  }
  
  
  public static void main(String[] args) {
    Nug nug = new Nug();
    nug.doMain(args);
  }
  

  @Override
  protected void init(String[] args) throws IllegalArgumentException, Exception {
    ArgList argList = new ArgList(args);
    
    boolean configured = configDesc(argList);
    
    if (!configured)
      throw new IllegalArgumentException("missing command");
    
    if (!argList.isEmpty())
      throw new IllegalArgumentException(
          "illegal or out-of-context arguments: " + argList.getArgsRemainingString());
  }
  

  @Override
  protected void start() throws InterruptedException, Exception {
    if (desc) {
      describe();
    }
  }
  
  
  private final static int DESC_FIRST_COL_WIDTH = 13;
  private final static int DIV_COL_WIDTH = 2;
  private final static String DIV = "|";
  
  private void describe() {
    Nugget nugget = null;
    Path path = null;
    if (FilenamingConvention.INSTANCE.isNugget(this.file)) {
      nugget = loadNugget(file);
      if (nugget == null)
        path = loadPath(file);
    } else {
      path = loadPath(file);
      if (path == null)
        nugget = loadNugget(file);
    }
    
    if (nugget != null)
      describe(nugget);
    else if (path != null)
      describe(path);
    else
      System.out.println("Can't grok " + file + " (" + file.length() + " bytes)");
    
    
//      try {
//
//        nugget = VersionedSerializers.NUGGET_SERIALIZER.load(file);
//      
//      } catch (Exception x) {
//        String msg = "Not a valid nugget (!)";
//        String xMsg = x.getMessage();
//        if (xMsg != null && !xMsg.isEmpty())
//          msg += ": " + xMsg;
//        System.out.println(msg);
//        return;
//      }
    
  }
  
  
  
  
  private void describe(Path path) {

    TablePrint table = newDescribeTable();
    System.out.println("<PATH>");
    System.out.println();
    System.out.println("First Row:");
    table.printHorizontalTableEdge('_');
    table.printRow("Row #", DIV, path.first().rowNumber());
    table.printRow("Hash", DIV, toHex(path.first().hash()));
    table.printHorizontalTableEdge('-');
    table.println();
    System.out.println("Last Row:");
    table.printHorizontalTableEdge('_');
    table.printRow("Row #", DIV, path.hiRowNumber());
    table.printRow("Hash", DIV, toHex(path.last().hash()));
    table.printHorizontalTableEdge('-');
    table.println();
    
    System.out.println("Last-to-First:");
    table.printHorizontalTableEdge('_');
    String nums = spacedReverseNumbers( path );
    table.printRow("Row #s", DIV,  nums);
    table.printHorizontalTableEdge('-');
    table.println();
  }
  
  
  private TablePrint newDescribeTable() {

    TablePrint table = new TablePrint(
        DESC_FIRST_COL_WIDTH,
        DIV_COL_WIDTH,
        RM - INDENT - DESC_FIRST_COL_WIDTH - DIV_COL_WIDTH);
    table.setIndentation(INDENT);
    return table;
  }

  private final static int REF_UTC_BUFFER = 3 * 65536;
  private final static int REF_URL_COUNT = 4;

  private void describe(Nugget nugget) {

    // much dup code to Path type; but don't want to wed the 2 together, yet
    TablePrint table = newDescribeTable();
    System.out.println("<NUGGET>");
    System.out.println();
    System.out.println("Target Row:");
    table.printHorizontalTableEdge('_');
    table.printRow("Row #", DIV, nugget.target().rowNumber());
    table.printRow("Entry", DIV, toHex(nugget.target().inputHash()));
    table.printRow("Hash", DIV, toHex(nugget.target().hash()));
    Optional<Long> createdAfter = nugget.afterUtc();
    if (createdAfter.isPresent()) {
      table.printRow(
                   "Created", DIV,   "(after) " + new Date(createdAfter.get()));
    }
    table.printRow("Witnessed", DIV, "        " + new Date(nugget.firstWitness().utc()));
    if (createdAfter.isPresent()) {
      String url =
          "https://crums.io" + Constants.LIST_ROOTS_PATH + "?" +
          Constants.QS_UTC_NAME + "=" + createdAfter.get() +
          "&" + Constants.QS_COUNT_NAME + "=-1";
      
      String beaconHex = IntegralStrings.toHex(
          nugget.ledgerPath().beaconRows().get(0).a.inputHash());
      table.printRow(
                   "Beacon ", DIV, beaconHex);
      table.printRow(
                   "Bcn Ref URL", DIV, url);
    }
    table.printRow("Wit Root", DIV, toHex(nugget.firstWitness().trail().rootHash()));
    String witUrl =
        "https://crums.io" + Constants.LIST_ROOTS_PATH + "?" +
         Constants.QS_UTC_NAME + "=" + (nugget.firstWitness().utc() + REF_UTC_BUFFER) +
         "&" + Constants.QS_COUNT_NAME + "=-" + REF_URL_COUNT;
    table.printRow("Wit Ref URL", DIV, witUrl);
    table.printHorizontalTableEdge('-');
    table.println();
    System.out.println("Referring Row:");
    table.printHorizontalTableEdge('_');
    table.printRow("Row #", DIV, nugget.ledgerPath().hiRowNumber());
    table.printRow("Hash", DIV, toHex(nugget.ledgerPath().last().hash()));
    table.printHorizontalTableEdge('-');
    table.println();

    System.out.println("Path-to-Target:");
    table.printHorizontalTableEdge('_');
    String nums = spacedReverseNumbers( nugget.ledgerPath() );

    table.printRow("Row #s", DIV,  nums);
    table.printHorizontalTableEdge('-');
    table.println();
  }
  
  
  
  private String spacedReverseNumbers(Path path) {
    List<Long> nums = path.rowNumbers();
    long target = path.target().rowNumber();
    StringBuilder s = new StringBuilder(nums.size() * 8);
    for (int index = nums.size(); index-- > 0; ) {
      long rn = nums.get(index);
      if (rn == target)
        s.append('[');
      s.append(rn);
      if (rn == target)
        s.append(']');
      s.append(' ');
    }
    return s.substring(0, s.length() - 1);
  }
  
  
  
  private Path loadPath(File file) {
    return load(file, VersionedSerializers.PATH_SERIALIZER);
  }
  
  
  private Nugget loadNugget(File file) {
    return load(file, VersionedSerializers.NUGGET_SERIALIZER);
  }
  
  
  private <T> T load(File file, EntitySerializer<T> serializer) {
    Format format = FilenamingConvention.INSTANCE.guessFormat(file);
    try {
      return serializer.load(file, format);
    } catch (Exception x) {  }
    
    format = format == Format.BINARY ? Format.JSON : Format.BINARY;
    
    try {
      return serializer.load(file, format);
    } catch (Exception x) {  }
    
    return null;
  }


  private boolean configDesc(ArgList argList) {
    
    switch (argList.removeContained(DESC).size()) {
    case 0:
      return false;
    case 1:
      break;
    default:
      throw new IllegalArgumentException("duplicate '" + DESC + "' command");
    }
    
    List<File> files = argList.removeExistingFiles();
    
    switch (files.size()) {
    case 0:
      throw new IllegalArgumentException("missing file rows for '" + DESC + "' command");
    case 1:
      break;
    default:
      throw new IllegalArgumentException("'" + DESC + "' command accepts a single file only");
    }
    
    this.file = files.get(0);
    
    return this.desc = true;
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
        "Command line tool for inspecting nuggets, paths, and other tamper-proof, self-verifying objects " +
        "extracted from a skip ledger.";
    printer.printParagraph(paragraph);
    printer.println();
    paragraph =
        "";
    // TODO
  }
  
  private final static int LEFT_TBL_COL_WIDTH = 15;
  private final static int RIGHT_TBAL_COL_WIDTH =
      RM - INDENT - LEFT_TBL_COL_WIDTH;

  @Override
  protected void printUsage(PrintStream out) {
    PrintSupport printer = new PrintSupport(out);
    printer.setMargins(INDENT, RM);
    printer.println();
    out.println("USAGE:");
    printer.println();
    String paragraph =
        "Arguments that are specified as name/value pairs are designated in the form " +
        "'name=*' below, with '*' standing for user input.";
    printer.printParagraph(paragraph);
    printer.println();
    
    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    table.printRow(DESC, "describes the object referenced in the given file rows");
    table.println();
    
  }
  
  

  private final static String DESC = "desc";
  private final static String ENTRY = "entry";
  private final static String RN = "rn";
  private final static String ROW_HASH = "rh";

}
