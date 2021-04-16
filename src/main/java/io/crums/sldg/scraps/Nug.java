/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.scraps;


import static io.crums.util.IntegralStrings.*;
import static io.crums.util.Strings.*;

import java.io.File;
import java.io.PrintStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import io.crums.model.Constants;
import io.crums.sldg.Path;
import io.crums.sldg.PathIntersector;
import io.crums.sldg.Row;
import io.crums.sldg.RowIntersect;
import io.crums.sldg.RowIntersection;
import io.crums.sldg.db.EntitySerializer;
import io.crums.sldg.db.Filenaming;
import io.crums.sldg.db.Format;
import io.crums.sldg.db.VersionedSerializers;
import io.crums.util.IntegralStrings;
import io.crums.util.Tuple;
import io.crums.util.main.ArgList;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.PrintSupport;
import io.crums.util.main.TablePrint;

/**
 * 
 */
public class Nug extends MainTemplate {
  
  
  private static class DescribeCommand {
    File file;
  }
  
  private static class CompareCommand {
    File a;
    File b;
    
    Map<Object, File> fileMapping = new HashMap<>();
  }
  
  private DescribeCommand descCommand;
  
  private CompareCommand compCommand;
  
  
//  private boolean desc;
//  
//  private File file;
  
  
  
  
  
  
  
  
  

  // Invoked by main(..)
  private Nug() {  }
  
  
  public static void main(String[] args) {
    Nug nug = new Nug();
    nug.doMain(args);
  }
  

  @Override
  protected void init(String[] args) throws IllegalArgumentException, Exception {
    ArgList argList = new ArgList(args);
    
    boolean configured = configDesc(argList) || configComp(argList);
    
    if (!configured)
      throw new IllegalArgumentException("missing command");

    
    argList.enforceNoRemaining();
  }
  

  private boolean configComp(ArgList argList) {
    
    if (!removeCommand(argList, COMP))
      return false;
    
    this.compCommand = new CompareCommand();
    
    List<File> files = argList.removeExistingFiles();
    if (files.size() != 2)
      throw new IllegalArgumentException("'" + COMP + "' command takes 2 file path arguments");
    
    compCommand.a = files.get(0);
    compCommand.b = files.get(1);
    
    if (compCommand.a.equals(compCommand.b)) {
      System.out.println();
      System.out.println(
          "[WARNING] Comparing file " + compCommand.a + " to itself. Did you mean this?");
      System.out.println();
    }
    return true;
  }
  


  private boolean removeCommand(ArgList argList, String command) {
    
    switch (argList.removeContained(command).size()) {
    case 0:
      return false;
    case 1:
      return true;
      
    default:
      throw new IllegalArgumentException("duplicate '" + command + "' command");
    }
  }

  private boolean configDesc(ArgList argList) {
    
    if (!removeCommand(argList, DESC))
      return false;
    
    
    List<File> files = argList.removeExistingFiles();
    
    switch (files.size()) {
    case 0:
      throw new IllegalArgumentException("missing file path for '" + DESC + "' command");
    case 1:
      break;
    default:
      throw new IllegalArgumentException("'" + DESC + "' command accepts a single file only");
    }
    
    this.descCommand = new DescribeCommand();
    descCommand.file = files.get(0);
    
    return true;
  }


  @Override
  protected void start() throws InterruptedException, Exception {
    if (descCommand != null)
      describe();
    else if (compCommand != null)
      compare();
  }
  
  
  private void compare() {
    try {
    SldgEntity a = load(compCommand.a);
    SldgEntity b = load(compCommand.b);
    compCommand.fileMapping.put(a.getObject(), compCommand.a);
    compCommand.fileMapping.put(b.getObject(), compCommand.b);
    if (a.hasNugget()) {
      Nugget nugget = a.getNugget();
      if (b.hasNugget())
        compareNuggets(nugget, b.getNugget());
      else
        comparePathToNugget(b.getPath(), nugget);
    } else {
      Path path = a.getPath();
      if (b.hasNugget())
        comparePathToNugget(path, b.getNugget());
      else
        comparePaths(path, b.getPath());
    }
    } catch (RuntimeException rx) {
      rx.printStackTrace();
      throw rx;
    }
  }


  private void compareNuggets(Nugget a, Nugget b) {
    Path pathA = a.ledgerPath();
    Path trailA = a.firstWitness().path();
    
    Path pathB = b.ledgerPath();
    Path trailB = b.firstWitness().path();

    TreeSet<RowIntersection> inters = new TreeSet<>(RowIntersection.NUM_COMP);
    TreeSet<RowIntersection> cons = new TreeSet<>(RowIntersection.NUM_COMP);
    
    collect(pathA, pathB, inters, cons);
    collect(pathA, trailB, inters, cons);
    collect(trailA, pathB, inters, cons);
    collect(trailA, trailB, inters, cons);
    

    final long maxTarget = Math.max(a.target().rowNumber(), b.target().rowNumber());
    
    String msg;
    
    if (!cons.isEmpty()) {
      final RowIntersection firstCon = cons.first();
      final boolean aboveTargets = firstCon.rowNumber() > maxTarget;
      final RowIntersection lastGood;
      
      if (aboveTargets) {
        SortedSet<RowIntersection> head = inters.headSet(firstCon);
        if (head.isEmpty())
          lastGood = null;
        else {
          RowIntersection ri = head.last();
          lastGood = ri.rowNumber() >= maxTarget ? ri : null;
        }
      } else
        lastGood = null;
      
      msg = "Nuggets DO NOT BELONG to the same ledger";
      if (aboveTargets) {
        msg += " as recorded";
      }
      msg += ": row [";
      msg += firstCon.rowNumber() + "] conflicts in the 2 objects. ";
      
      if (aboveTargets) {
        msg += " Their targets, however, rows [" + a.target().rowNumber() + "] in ";
        msg += compCommand.fileMapping.get(a) + " and [" + b.target().rowNumber();
        msg += "] in " + compCommand.fileMapping.get(b);
        if (lastGood == null) {
          msg += " may yet share a common ledger since they target rows numbered below ";
          msg += "the conflict. ";
        } else {
          msg += " provably share a common ledger ";
          msg += "since they intersect at row [" + lastGood.rowNumber() + "]";
        }
      }
      
    } else {  // no conflicts
      
      if (inters.isEmpty()) {
        msg = "No information can be gleaned from the 2 nuggets: their numbered rows do not ";
        msg += "cross anywhere.";
      } else {
        
        RowIntersection last = inters.last();
        if (last.rowNumber() >= maxTarget) {
          msg = "Nugget TARGETS BELONG to a common ledger: their ledger paths intersect at row [";
          msg += last.rowNumber() + "]";
          if (pathA.hiRowNumber() == pathB.hiRowNumber()) {
            msg += " the highest row number recorded in both objects.";
          } else {
            msg += " which is above their respective target rows [";
            msg += a.target().rowNumber() + "] in " + compCommand.fileMapping.get(a) + " and [";
            msg += b.target().rowNumber() + "] in " + compCommand.fileMapping.get(b);
          }
        } else {
          long minTarget = Math.min(a.target().rowNumber(), b.target().rowNumber());
          if (last.rowNumber() >= minTarget) {
            Nugget lo, hi;
            // one, but not the other (ie the lo one)
            if (a.target().rowNumber() <= last.rowNumber()) {
              lo = a;
              hi = b;
            } else {
              lo = b;
              hi = a;
            }
            msg = "Nugget " + compCommand.fileMapping.get(hi) + " with target row [";
            msg += "] IMPLIES the target of nugget " + compCommand.fileMapping.get(lo);
            msg += " [" + lo.target().rowNumber() + "] since they intersect at row [";
            msg += last.rowNumber() + "].";
          } else {
            msg = "Nuggets intersect at row [" + last.rowNumber() + "] which is below ";
            msg += "both their targets ([" + a.target().rowNumber() + "] in ";
            msg += compCommand.fileMapping.get(a) + " and [" + b.target().rowNumber();
            msg += "] in " + compCommand.fileMapping.get(b) + " No additonal information is available.";
          }
        }
      }
      
      
    }
    

    PrintSupport printer = new PrintSupport();
    printer.println();
    printer.printParagraph(msg);
    printer.println();
  }
  
  private void collect(
      Path a, Path b,
      TreeSet<RowIntersection> inters, TreeSet<RowIntersection> cons) {
    PathIntersector i = a.intersector(b);
    i.forEach(ri -> inters.add(ri));
    Optional<RowIntersection> conflict = i.firstConflict();
    if (conflict.isPresent())
      cons.add(conflict.get());
  }

  private String snippetForIndirect(RowIntersection i, Path a, Path b) {
    RowIntersect type = i.type();

    String snippet;
    Path first, second;
    if (a.hasRow(i.first().rowNumber())) {
      first = a;
      second = b;
    } else {
      assert b.hasRow(i.first().rowNumber());
      first = b;
      second = a;
    }
    if (type.byLineage()) {
      snippet =
        " referenced from rows [" + i.first().rowNumber() + "] in " +
        compCommand.fileMapping.get(first) + " and [" + i.second().rowNumber() +
        "] in " + compCommand.fileMapping.get(second);
    } else {
      assert type.byReference();
      snippet =
        " in " + compCommand.fileMapping.get(first) + " and referenced from row [" +
        i.second().rowNumber() + "] in " + compCommand.fileMapping.get(second);
    }
    return snippet;
  }

  private void comparePaths(Path a, Path b) {
    
    if (a.rows().equals(b.rows())) {
      boolean sameTarget = a.target().rowNumber() == b.target().rowNumber();
      boolean sameBeacons = a.beacons().equals(b.beacons());
      String msg;
      if (sameTarget && sameBeacons)
        msg = "Paths are exactly the same";
      else if (sameTarget)
        msg = "Path rows are the same but beacon info differ";
      else if (sameBeacons) {
        msg = "Path rows are the same but target rows differ: [";
        msg += a.target().rowNumber() + "] in " + compCommand.fileMapping.get(a);
        msg += " vs. [" + b.target().rowNumber() + "] in ";
        msg += compCommand.fileMapping.get(b);
      } else {
        msg = "Path rows are the same but beacon info and target rows differ. Targets: [";
        msg += a.target().rowNumber() + "] in " + compCommand.fileMapping.get(a);
        msg += " vs. [" + b.target().rowNumber() + "] in ";
        msg += compCommand.fileMapping.get(b);
      }
      System.out.println(msg);
      return;
    }
    
    PathIntersector pi = a.intersector(b);
    List<RowIntersection> inters = pi.collect();
    Optional<RowIntersection> conflict = pi.firstConflict();
    
   
    
    PrintSupport printer = new PrintSupport();
    printer.println();
    
    if (conflict.isPresent()) {
      
      // - C O N F L I C T -
      String msg;
      
      RowIntersection con = conflict.get();
      RowIntersect type = con.type();
      assert con.isConflict();
      msg =
        "Paths DO NOT BELONG to a common ledger. Their hashes for row [" + con.rowNumber() +
        "]";
      if (type.direct()) {
        msg += " conflict. (Both paths cite this row directly.)";
      } else {
        msg += snippetForIndirect(con, a, b) + " conflict.";
      }
      
      printer.printParagraph(msg);
      printer.println();
    }
    
    if (inters.isEmpty()) {
      
      if (conflict.isEmpty()) {
        
        // - I G N O R A N T -
        
        printer.printParagraph(
          "The row numbers in the given two paths are independent. Nothing can be " +
          "inferred by comparing them.");
        
        printer.println();
      }
      
    } else {  // there are valid intersections
      
      RowIntersection lastIntersect = inters.get(inters.size() - 1);
      RowIntersect type = lastIntersect.type();
      
      String msg = conflict.isPresent() ? "However the" : "The";
      msg += " paths share a common ledger up to row [" + lastIntersect.rowNumber() + "]";
      
      if (type.direct()) {
        msg += " which is cited directly in both.";
      } else {
        
        msg += snippetForIndirect(lastIntersect, a, b);
      }
      
      printer.printParagraph(msg);
      printer.println();
    }
  }


  private void comparePathToNugget(Path path, Nugget nugget) {
    PrintSupport printer = new PrintSupport();
    printer.println();
    
    String pathFile = compCommand.fileMapping.get(path).toString();
    String nugFile = compCommand.fileMapping.get(nugget).toString();
    
    Path nugPath = nugget.ledgerPath();
    
    String pName = path.target().rowNumber() == 1 ? "State-path" : "Path";
    
    if (nugPath.rows().equals(path.rows())) {
      printer.printParagraph(
        pName + " and nugget have exactly the same ledger rows.");
      printer.println();
      return;
    }
    
    Path trailPath = nugget.firstWitness().path();
    
    List<RowIntersection> intersNp, intersTp;
    Optional<RowIntersection> conflictNp, conflictTp;
    {
      PathIntersector npi = nugPath.intersector(path);
      intersNp = npi.collect();
      conflictNp = npi.firstConflict();
      
      PathIntersector tpi = trailPath.intersector(path);
      intersTp = tpi.collect();
      conflictTp = tpi.firstConflict();
      
    }
    
    final boolean intersected = intersNp.size() + intersTp.size() > 0;
    final RowIntersection lastInter;
    if (!intersected)
      lastInter = null;
    else if (intersTp.isEmpty())
      lastInter = intersNp.get(intersNp.size() - 1);
    else if (intersNp.isEmpty())
      lastInter = intersTp.get(intersTp.size() - 1);
    else {
      RowIntersection p = intersNp.get(intersNp.size() - 1);
      RowIntersection t = intersTp.get(intersTp.size() - 1);
      lastInter = p.rowNumber() >= t.rowNumber() ? p : t;
    }
    
    if (conflictNp.isPresent() || conflictTp.isPresent()) {
      
      String msg = pName + " and nugget DO NOT BELONG to the same ledger. ";
      
      RowIntersection con;
      
      if (conflictNp.isEmpty())
        con = conflictTp.get();
      else if (conflictTp.isEmpty()) {
        con = conflictNp.get();
      } else {
        // pick the lower numbered conflict
        RowIntersection n = conflictNp.get();
        RowIntersection t = conflictTp.get();
        con = n.rowNumber() <= t.rowNumber() ? n : t;
      }
      
      // check the intersections
      if (intersected) {
        if (con.rowNumber() == lastInter.rowNumber() + 1) {
          msg += "Both shared the same ledger up to row [" + lastInter.rowNumber();
          msg += "]: their ledgers fork on the next row.";
        } else {
          msg += "They shared a common ledger which was forked on or before row [";
          msg += con.rowNumber() + "] but after row [";
          msg += lastInter.rowNumber() + "].";
        }
      } else {
        msg += "Row [" + con.rowNumber() + "] conflicts in the 2 ledger objects.";
      }
      
      printer.printParagraph(msg);
      printer.println();
      
    } else {  // no conflicts
      
      String msg;
      if (intersected) {
        msg =
          pName + " and nugget provably share a common ledger up to row [" +
          lastInter.rowNumber() + "]";
        
        RowIntersection stitchable = null;
        
        boolean pathHigher = path.hiRowNumber() > nugPath.hiRowNumber();
        
        if (pathHigher) {
          
          stitchable = findStitchable(path, nugPath, intersNp);
          if (stitchable == null)
            stitchable = findStitchable(path, trailPath, intersTp);
          
          if (stitchable == null) {
            msg += ". Altho the path in " + pathFile + " has a higher row number [" + path.hiRowNumber() + "] ";
            msg += "there isn't sufficient information to actually extend the nugget's path to that row number.";
          } else {
            msg += ": if you have reason to trust the path object in " + pathFile + " (e.g. it ";
            msg += "matches information advertised by the ledger owner), there is sufficient ";
            msg += "info in this path object to update the internal paths in " + nugFile;
            msg += " as a new nugget file with the same target but referenced from the higher ";
            msg += "(therefore more recent) ledger row number [" + path.hiRowNumber() + "].";
          }
        } else if (path.hiRowNumber() == nugPath.hiRowNumber()) {
          
          msg += ". Both objects record the ledger up to the same row number.";
        } else {
          msg += ".";
        }
        
      } else { // !intersected
        msg =
          pName + " and nugget do not contain common ledger rows. Nothing can be gleaned by comparing them.";
      }
      
      printer.printParagraph(msg);
      printer.println();
    }
    
  }
  
  
  private RowIntersection findStitchable(Path path, Path innerNugPath, List<RowIntersection> inters) {

    RowIntersection stitchable = null;
    for (int index = inters.size(); index-- > 0;) {
      // TODO
      RowIntersection i = inters.get(index);
      RowIntersect type = i.type();
      
      if (type.direct()) {
        stitchable = i;
        break;
      }
      if (type.byLineage())
        continue;
      
      // type.byReference() is true
      if (path.hasRow(i.second().rowNumber())) {
        stitchable = i;
        break;
      }
    }
    return stitchable;
  }


  private final static int DESC_FIRST_COL_WIDTH = 13;
  private final static int DIV_COL_WIDTH = 2;
  private final static String DIV = "|";
  
  private void describe() {
    
    SldgEntity entity = load(descCommand.file);
    
    if (entity.hasNugget())
      describe(entity.getNugget());
    else
      describe(entity.getPath());
  }
  
  
  
  
  private SldgEntity load(File file) {
    Nugget nugget = null;
    Path path = null;
    if (Filenaming.INSTANCE.isNugget(file)) {
      nugget = loadNugget(file);
      if (nugget == null)
        path = loadPath(file);
    } else {
      path = loadPath(file);
      if (path == null)
        nugget = loadNugget(file);
    }
    
    if (nugget != null)
      return new SldgEntity(nugget);
    else if (path != null)
      return new SldgEntity(path);
    else
      throw new IllegalArgumentException("Can't grok " + file + " (" + file.length() + " bytes)");
    
    
  }
  
  
  
  
  private void describe(Path path) {

    TablePrint table = newDescribeTable();
    System.out.println("<PATH>");
    table.println();
    System.out.println("First Row:");
    printLedgerRow(path.first(), table);
    table.println();
    if (path.isTargeted()) {
      System.out.println("Target Row:");
      printLedgerRow(path.target(), table);
      table.println();
    }
    System.out.println("Last Row:");
    printLedgerRow(path.last(), table);
    table.println();
    
    System.out.println("Last-to-First:");
    table.printHorizontalTableEdge('_');
    String nums = spacedReverseNumbers( path );
    table.printRow("Row #s", DIV,  nums);
    table.printHorizontalTableEdge('-');
    table.println();
    
    if (!path.hasBeacon())
      return;
    
    List<Tuple<Row, Long>> beacons = path.beaconRows();
    System.out.println(pluralize("Beacon Row", beacons.size()) + ":");
    table.printHorizontalTableEdge('_');
    
    for (Tuple<Row, Long> beacon : beacons) {
      table.printRow(" [" + beacon.a.rowNumber() + "]", DIV, new Date(beacon.b));
      table.printRow("Entry"  , DIV, toHex(beacon.a.inputHash()));
      String url =
          "https://crums.io" + Constants.LIST_ROOTS_PATH + "?" +
          Constants.QS_UTC_NAME + "=" + beacon.b +
          "&" + Constants.QS_COUNT_NAME + "=-1";
      table.printRow("Ref URL"  , DIV, url);
      table.printHorizontalTableEdge('-');
    }
    table.println();
  }
  
  
  private void printLedgerRow(Row row, TablePrint table) {
    table.printHorizontalTableEdge('_');
    table.printRow("Row #", DIV, row.rowNumber());
    table.printRow("Entry", DIV, toHex(row.inputHash()));
    table.printRow("Hash", DIV, toHex(row.hash()));
    table.printHorizontalTableEdge('-');
    
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
    Format format = Filenaming.INSTANCE.guessFormat(file);
    try {
      return serializer.load(file, format);
    } catch (Exception x) {  }
    
    format = format == Format.BINARY ? Format.JSON : Format.BINARY;
    
    try {
      return serializer.load(file, format);
    } catch (Exception x) {  }
    
    return null;
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
        "In the table that follows '*' stands for an argument (typically a file path) supplied by the user; " +
        "it is not a wild card.";
    printer.printParagraph(paragraph);
    printer.println();
    
    TablePrint table = new TablePrint(out, LEFT_TBL_COL_WIDTH, RIGHT_TBAL_COL_WIDTH);
    table.setIndentation(INDENT);
    table.printRow(DESC + " *", "describes the object referenced in the given file");
    table.println();
    table.printRow(COMP + " * *", "compares the objects referenced in the given 2 files");
    table.println();
    
  }
  
  

  private final static String DESC = "desc";
  private final static String COMP = "comp";
//  private final static String ENTRY = "entry";
//  private final static String RN = "rn";
//  private final static String ROW_HASH = "rh";

}
