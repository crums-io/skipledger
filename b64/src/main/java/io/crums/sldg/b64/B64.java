/*
 * Copyright 2022 Babak Farhang
 */
package io.crums.sldg.b64;


import io.crums.util.Base64_32;
import io.crums.util.IntegralStrings;

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
 * Utility for converting base-64-32 to hex. Base64-32 is the encoding used in
 * the SQL representation of 32-byte hashes.
 * 
 * @see Base64_32
 */
@Command(
    name = "base64-32",
    mixinStandardHelpOptions = true,
    version = {
        "base64-32  v1.0",
    },
    description = {
        "Converts 32-byte values from base64-32 to hexadecimal, or vice versa.",
        "",
        "A base-64 variant that uses 43 characters to encode 32 bytes. The",
        "hash ledger's SQL schema records SHA-256 hashes using this encoding.",
        "See also: https://crums-io.github.io/io-util/base64-32.html",
        "",
    }
    )
public class B64 implements Runnable {
  
  
  

  @Spec
  private CommandSpec spec;
  

  
  private String[] encodedValues;
  
  
  
  @Parameters(
      arity = "1..*",
      paramLabel = "VALUE",
      description = {
          "32-byte value[s] converted from base64-32 to hex, or vice versa",
          "Either @|bold 43|@ base64-32 chars, or @|bold 64|@ hexadecimal chars",
      }
      )
  public void setEncodedValues(String[] values) {
    this.encodedValues = values;
  }



  @Override
  public void run() {
    byte[] decoded = new byte[32];
    for (var value : encodedValues) {
      String converted;
      switch (value.length()) {
      case 43:
        converted = base64ToHex(value, decoded);
        break;
      case 64:
        converted = hexToBase64(value, decoded);
        break;
      default:
        throw new ParameterException(spec.commandLine(),
            "wrong number of chars (%d) in argument: %s".formatted(value.length(), value));
      }
      System.out.println(converted);
    }
  }
  
  
  private String base64ToHex(String value, byte[] decoded) {
    try {
      return
          IntegralStrings.toHex(
              Base64_32.decode(value, decoded, 0));
    } catch (IllegalArgumentException iax) {
      throw new ParameterException(spec.commandLine(),
          "bad argument '%s': %s".formatted(value, iax.getMessage()));
    }
  }
  
  
  private String hexToBase64(String value, byte[] decoded) {
    try {
      return
          Base64_32.encode(
              IntegralStrings.hexToBytes(value, decoded));
    } catch (IllegalArgumentException iax) {
      throw new ParameterException(spec.commandLine(),
          "bad argument '%s': %s".formatted(value, iax.getMessage()));
    }
  }
  
  
  
  
  public static void main(String[] args) {
    int exitCode = new CommandLine(new B64()).execute(args);
    System.exit(exitCode);
  }
  

}














