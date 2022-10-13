/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.reports.pdf.cli.rt;

import java.io.File;
import java.util.Objects;

/**
 * File commit utility. By default uses the tilde ('~') to prefix the backup file.
 * To make it hidden, supply a period ('.') in the constructor.
 * 
 * <h2>2-File Model</h2>
 * <p>
 * Despite its name, this doesn't actually have a commit method.
 * The base model uses 2 files: the file itself, and its backup. Typically
 * (not enforced by this class), you first move the existing file
 * {@linkplain #moveToBackup() to backup} (if it exists), and then rewrite
 * the new version of the file. So if the writing of the (fresh) new version
 * of the file fails, there is still the backup copy.
 * </p><p>
 * What if the writing of the new file fails? An exception is thrown, of course;
 * but after that, it's up to the user code what to do next. One sensible option is for
 * the user to {@linkplain #rollback() rollback} immediately. Another, perhaps better
 * option, is to introduce a <em>third</em> (derivative) file to which the user first
 * writes to, and only after having finished writing do they move the files to their
 * respective locations. For now, we keep it simple.
 * </p>
 * <h2>To be Moved</h2>
 * <p>
 * TODO: this is reusable code. Move to io-utils module.
 * </p>
 * 
 * @see #FileCommit(File, char)
 */
public class FileCommit {
  
  private final char tilde;
  
  private final File file;
  
  private final boolean initExists;
  
  /**
   * Creates an instance with tilde ('~') as the prefix for the backup file.
   * 
   * @param file
   */
  public FileCommit(File file) {
    this(file, '~');
  }

  /**
   * Full constructor.
   * 
   * @param file    path to existing or non-existent file
   * @param tilde   backup filename prefix. Commonly '~'. Use '.' to make it hidden.
   */
  public FileCommit(File file, char tilde) throws IllegalArgumentException {
    this.tilde = tilde;
    this.file = Objects.requireNonNull(file, "null file");
    this.initExists = file.exists();
    if (initExists  && file.isDirectory())
      throw new IllegalArgumentException("argument is a directory: " + file);
    if (!initExists) {
      var parent = file.getParentFile();
      if (parent != null && !parent.exists())
        throw new IllegalArgumentException(
            "parent directory (" + parent.getName() + ") does not exist: " + file);
    }
  }
  
  
  
  /**
   * Rollsback the file to its backup version (if it exists). Noop if the backup file
   * doesn't exist ({@code false} return value). If the {@linkplain #getFile file}
   * didn't exist at instantation, then it is simply deleted (no file is restored).
   * (So if you're writing to the file for the first time, and an error occurs,
   * you'll still be able to see how far it got.)
   * 
   * @return {@code true} iff the backup file was moved to the {@linkplain #getFile() file}
   * @see #initExists()
   */
  public boolean rollback() throws IllegalStateException {
    File backup = getBackupFile();
    if (!backup.exists())
      return false;
    
    if (!backup.isFile())
      throw new IllegalStateException("illegal backup file (is a directory): " + backup);
    
    // if the file exists, delete it
    if (file.exists()) {
      if (!file.delete())
        throw new IllegalStateException("failed to delete on rollback: " + file);
    }
    
    // if the file *didn't exist at instantiation, do nothing
    // (lingering-backup/no-original-file corner case)
    if (!initExists)
      return false;
    
    // mv backup file
    if (!backup.renameTo(file))
      throw new IllegalStateException("failed to restore (move) backup file " + backup);
    
    return true;
  }
  
  
  /**
   * Returns {@code code true} iff the {@linkplain #getFile() file} already existed at
   * instantiation. This controls {@linkplain #rollback() rollback}.
   */
  public final boolean initExists() {
    return initExists;
  }
  

  /**
   * Returns the "modifiable" file.
   * 
   * @return depending on state, this file may or may not exist
   */
  public final File getFile() {
    return file;
  }
  
  
  /**
   * Returns the backup file.
   * 
   * @return depending on state, this file may or may not exist
   */
  public File getBackupFile() {
    return new File(file.getParentFile(), tilde + file.getName());
  }
  
  
  /**
   * Moves the {@linkplain #getFile() file} (if it exists) to its backup location.
   * Noop, otherwise.
   * 
   * @return {@code this}
   */
  public final FileCommit moveToBackup() throws IllegalStateException {
    if (file.exists()) {
      File backup = getBackupFile();
      if (backup.exists()) {
        if (!backup.delete())
          throw new IllegalStateException("failed to delete backup file " + backup);
      }
      if (!file.renameTo(backup))
        throw new IllegalStateException("failed to move file to backup name: " + backup);
    }
    return this;
  }

}



