/*
 * Copyright 2023 Babak Farhang
 */
package io.crums.sldg.logs.text;


import static io.crums.sldg.logs.text.LogledgeConstants.LOGNAME;
import static io.crums.sldg.logs.text.LogledgeConstants.PSEAL_EXT;
import static io.crums.sldg.logs.text.LogledgeConstants.SEAL_EXT;
import static io.crums.sldg.logs.text.LogledgeConstants.SEAL_MAGIC;
import static io.crums.sldg.logs.text.LogledgeConstants.assertMagicHeader;
import static io.crums.sldg.logs.text.LogledgeConstants.pendingSealFile;
import static io.crums.sldg.logs.text.LogledgeConstants.sealFile;
import static io.crums.sldg.logs.text.LogledgeConstants.writeMagicHeader;

import java.io.File;
import java.io.IOException;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.util.Optional;

import io.crums.client.ClientException;
import io.crums.io.Opening;
import io.crums.io.channels.ChannelUtils;
import io.crums.sldg.ByteFormatException;

/**
 * Convention for naming, writing and reading {@linkplain Seal seal}-files.
 * The file format is just a fixed magic header followed by whatever is written
 * by {@linkplain Seal#writeTo(ByteBuffer)}.
 * 
 * <h2>Seal Filepath</h2>
 * <p>
 * A seal's filename and location is determined by the filepath of the log it tracks, and
 * whether the {@linkplain Seal#isTrailed() seal is trailed} or not. Seal files
 * live in the <em>same</em> directory as the log files they track and their
 * filenames are prefixed with {@code .sldg.}, followed by the log's filename, and
 * finally, depending on whether or not witnessed, one of the following 2 extensions:
 * </p>
 * <ol>
 * <li>{@code .pseal} - not witnessed (pending)</li>
 * <li>{@code .seal} - witnessed </li>
 * </ol>
 * <p>
 * </p>
 * @see Seal
 */
public class Sealer {
  
  
  private Sealer() {  }
  
  
  private final static int MAX_SEAL_SIZE = 32 * 1024;
  private final static int WBUF_CAP = 16 * 1024;
  
  
  /**
   * Finds an existing seal file for the given {@code log} and if it exists returns it as a seal.
   * The search order is {@linkplain LogledgeConstants#sealFile(File)}, then
   * {@linkplain LogledgeConstants#pendingSealFile(File)}.
   * 
   * @see #loadSealFile(File)
   */
  public static Optional<Seal> loadForLog(File log) throws IOException {
    
    if (!log.isFile()) {
      var msg = log.isDirectory() ?
          " is a directory" : " does not exist";
      throw new IllegalArgumentException(log + msg);
    }
    
    // find an existing seal file or bail..
    var sFile = sealFile(log);
    if (!sFile.isFile()) {
      sFile = pendingSealFile(log);
      if (!sFile.isFile())
        return Optional.empty();
    }
    
    return Optional.of( loadSealFile(sFile));
  }
  
  
  /**
   * Returns the seal file for the given {@code log}, if any.
   */
  public static Optional<File> getSealFile(File log) {
    var sFile = sealFile(log);
    if (sFile.isFile())
      return Optional.of(sFile);
    sFile = pendingSealFile(log);
    return sFile.isFile() ? Optional.of(sFile) : Optional.empty();
  }
  
  
  
  /**
   * Loads and returns a seal from the given seal file. File extensions are ignored.
   * 
   * @param sFile the seal file (not the log file)
   */
  public static Seal loadSealFile(File sFile) throws IOException {
    long len = sFile.length();
    if (len == 0)
      throw new ByteFormatException(sFile + " is empty");
    if (len > MAX_SEAL_SIZE)
      throw new ByteFormatException(
          "%s file length (%d) > %d".formatted(sFile.toString(), len, MAX_SEAL_SIZE) );
    
    var work = ByteBuffer.allocate((int) len);
    
    try (var ch = Opening.READ_ONLY.openChannel(sFile)) {
      ChannelUtils.readRemaining(ch, 0, work).flip();
    }
    
    assertMagicHeader(SEAL_MAGIC, work);
    return Seal.load(work);
  }
  
  
  /**
   * Loads or creates seal for the given {@code log} file.
   * 
   * @param log       the text-based log / journal file
   * @param grammar   the parsing grammar
   * 
   * @throws IllegalStateException if the existing seal for the log file uses a different grammar
   */
  public static Seal loadOrSeal(File log, Grammar grammar) throws IOException, IllegalStateException {
    
    var existing = loadForLog(log);
    if (existing.isPresent())
      return assertGrammar(existing.get(), grammar, log);
    
    
    return newSeal(log, grammar);
  }
  
  
  /**
   * Creates and returns a new seal using the given grammar. A new secure random
   * salt is generated.
   * 
   * @param log       the text-based log / journal file
   * @param grammar   the parsing grammar
   * @return a newly created seal of the given log, written in the default location
   * 
   * @throws IllegalStateException if a seal for the given {@code log} already exists
   */
  public static Seal seal(File log, Grammar grammar) throws IOException, IllegalStateException {
    
    var existing = loadForLog(log);
    if (existing.isPresent()) {
      return assertGrammar(existing.get(), grammar, log);
    }
    
    return newSeal(log, grammar);
  }
  
  
  private static Seal assertGrammar(Seal seal, Grammar grammar, File log) {
    var expGrammar = seal.rules().grammar();
    if (!expGrammar.equals(grammar))
      throw new IllegalStateException(
          "%s already has a seal, but with a different grammar (%s vs %s)"
          .formatted(log, expGrammar, grammar));
    return seal;
  }
  
  
  private static Seal newSeal(File log, Grammar grammar) throws IOException {
    var hashgram = new HashingGrammar(grammar);
    var state = hashgram.stateHasher().play(log);
    Seal seal = new Seal(state.rowNumber(), state.rowHash(), hashgram);
    File sFile = pendingSealFile(log);
    writeSeal(seal, sFile);
    return seal;
  }
  
  
  /** Returns the file extension for trailed and untrailed seals, resp. */
  public static String getFileExtension(Seal seal) {
    return seal.isTrailed() ? SEAL_EXT : PSEAL_EXT;
  }
  
  
  
  /**
   * Writes the seal to the given new file. File extensions are not enforced.
   * 
   * @param seal    trailed or untrailed
   * @param sFile   target file path (must not exist)
   */
  public static void writeSeal(Seal seal, File sFile) throws IOException {

    var buffer = ByteBuffer.allocate(WBUF_CAP);
    
    writeMagicHeader(SEAL_MAGIC, buffer);
    seal.writeTo(buffer).flip();

    try (var ch = Opening.CREATE.openChannel(sFile)) {
      ChannelUtils.writeRemaining(ch, 0L, buffer);
    }
  }
  
  
  /**
   * Loads or creates a seal for given {@code log} file, and if not already witnessed
   * (not ({@linkplain Seal#isTrailed() trailed}), witnesses its hash and attempts
   * to create a <em>trailed</em> version of the seal.
   * <p>
   * This is equivalent to invoking {@link #loadOrSeal(File, Grammar) loadOrSeal(log, grammar)}
   * followed by {@link #witnessLog(File) witnessLog(log)}.
   * </p>
   * 
   * @param log       the text-based log / journal file
   * @param grammar   the parsing grammar
   * 
   * @return trailed or untrailed seal
   * 
   * @throws IllegalStateException if the recorded seal's [parsing] grammar is different than {@code grammar}
   * @throws ClientException remote (possibly network) related error on attempt to witness hash
   * 
   * @see Seal#isTrailed()
   * @see #witnessLog(File)
   */
  public static Seal witnessLog(File log, Grammar grammar) throws IOException, ClientException, IllegalStateException {
    Seal seal = loadOrSeal(log, grammar);
    return witness(seal, log);
  }
  
  
  
  /**
   * Loads the seal for given {@code log} file, and if not already witnessed
   * (not ({@linkplain Seal#isTrailed() trailed}), witnesses its hash and attempts
   * to create a <em>trailed</em> version of the seal.
   * <p>
   * This is equivalent to invoking {@link #loadForLog(File) loadForLog(log)}
   * followed by {@link #witnessLog(File) witnessLog(log)}.
   * </p>
   * 
   * @param log       the text-based log / journal file
   * 
   * @return empty (if {@code log} does not have an exising seal), trailed or untrailed seal
   * 
   * @throws ClientException remote (possibly network) related error on attempt to witness hash
   * 
   * @see Seal#isTrailed()
   * @see #witnessLog(File, Grammar)
   */
  public static Optional<Seal> witnessLog(File log) throws IOException, ClientException {
    
    var existing = loadForLog(log);
    if (existing.isEmpty())
      return existing;
    
    return Optional.of( witness(existing.get(), log) );
  }
  
  
  
  private static Seal witness(Seal seal, File log) throws IOException, ClientException {
    if (seal.isTrailed())
      return seal;
    
    Seal witSeal = seal.witness();
    
    if (witSeal.isTrailed()) {
      var sFile = sealFile(log);
      writeSeal(witSeal, sFile);
      boolean deleteFailed = !pendingSealFile(log).delete();
      
      if (deleteFailed)
        System.getLogger(LOGNAME).log(Level.WARNING, "failed to delete " + pendingSealFile(log));
    }
    
    return witSeal;
  }
  
  

}
