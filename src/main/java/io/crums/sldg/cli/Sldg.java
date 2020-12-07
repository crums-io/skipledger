/*
 * Copyright 2020 Babak Farhang
 */
package io.crums.sldg.cli;


import static io.crums.util.main.Args.*;

import java.io.File;
import java.io.PrintStream;

import io.crums.sldg.Db;
import io.crums.util.main.MainTemplate;
import io.crums.util.main.TablePrint;

/**
 * 
 */
public class Sldg extends MainTemplate {
  
  private String command;
  
  private Db db;
  
  private Sldg() {
    
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    new Sldg().doMain(args);
  }

  @Override
  protected void init(String[] args) throws IllegalArgumentException, Exception {
    if (args.length == 0)
      throw new IllegalArgumentException("No arguments specified");
    
    File dir;
    {
      String path = getRequiredParam(args, DIR);
      if (path == null || path.isEmpty())
        throw new IllegalArgumentException("Required parameter " + DIR + " missing");
      
      dir = new File(path);
    }
    
    
    
    
    
  }
  
  
  

  @Override
  protected void start() throws InterruptedException, Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void printDescription() {
    System.out.println();
    System.out.println("DESCRIPTION:");
    System.out.println();
    System.out.println("Command line tool for accessing a skip ledger on the file system.");
    System.out.println();
    
  }

  @Override
  protected void printUsage(PrintStream out) {
    out.println();
    out.println("USAGE:");
    out.println();
    out.println("Arguments are specified as 'name=value' pairs.");
    out.println();
    
    TablePrint table = new TablePrint(out, 10, 65, 3);
    table.setIndentation(1);
    
    table.printRow(DIR + "=*", "path to skip ledger directory", REQ);
    out.println();
    
    table.printRow(IN + "=true", "pushes a stream of hash entries as hex from", REQ_CH);
    table.printRow(null,         "the standard input", null);
    out.println();
    
    table.printRow(OUT + "=*", "output (print) row starting from row number", REQ_CH);
    out.println();
    
    table.printRow(LIMIT + "=*", "maximum number of rows to ", OPT);
    out.println();
    
    table.printRow(REX + "=*", "row exponent", OPT);
  }
  

  private final static String DIR = "dir";

  private final static String REQ = "R";
  private final static String REQ_CH = "R?";
  private final static String OPT = "";
  
  private final static String OUT = "out";
  private final static String LIMIT = "limit";
  private final static String REX = "rex";
  


  private final static String PUSH = "push";
  private final static String IN = "in";
  
  
  
}
