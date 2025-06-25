/*
 * Copyright 2025 Babak Farhang
 */
/**
 * Line-delimited, append-only, text files (such as logs) modeled as ledgers.
 * 
 * Each line represents a "tabular" ledger row, composed of <em>cells</em>
 * formed by parsing the line into tokens (usually whitespace-delimited words).
 * The principal goal here is to quickly compute or update the SHA-256
 * skipledger hash of a log file using minimal storage overhead.
 * 
 * <h2>Design</h2>
 * <p>
 * The parsing works much like an XML SAX parser, only simpler. 
 * </p>
 * <h3>Grammar</h3>
 * <p>
 * The line parsing rules are very simple and are presently modeled the same way
 * the standard {@linkplain java.util.StringTokenizer StringTokenizer} class
 * works: by default, tokens are delimited by whitespace. Future versions may
 * support more sophisticated tokenizers.
 * </p>
 * <h4>Comment (Neutral) Lines</h4>
 * <p>
 * The grammar also supports a simple, optional <em>comment-line</em> format:
 * lines prefixed with comment characters (e.g. '{@code #}') may be interpreted
 * as neutral.
 * </p>
 * <h3>Parser</h3>
 * <p>
 * {@linkplain io.crums.sldg.logledger.LogParser LogParser} is the line-based
 * parser with callback-{@linkplain io.crums.sldg.logledger.LogParser.Listener
 * listener}s. The parser itself does not know about the skipledger scheme,
 * though its registered listeners typically do (see next). Nor is it concerned
 * with the grammar's tokenization; the only part of the grammar the parser
 * might care about are comment lines.
 * </p><p>
 * {@linkplain io.crums.sldg.logledger.LogParser.Listener LogParser.Listener}
 * is a "minimally rich" interface. It's minimal, in the sense that the data
 * is unprocessed (raw bytes): it's rich, in the sense that EOL offsets,
 * line numbers, and row numbers are called out. (Row no.s and line no.s do not
 * necessarily coincide if comment-lines are supported and are present.)
 * </p><p>
 * The parser can be "played" up to a specific row no. (line no.) by setting
 * the {@linkplain io.crums.sldg.logledger.LogParser#maxRowNo(long) maxRowNo}.
 * </p>
 * <h3>Hasher</h3>
 * <p>
 * {@linkplain io.crums.sldg.logledger.Hasher LogHasher} implements the
 * parser's callback interface and tracks the log's commitment hash at each
 * line.
 * </p>
 * <h4>Parse State</h4>
 * <p>
 * Recall the parser can be "played" up to any row / line no. The
 * "parse-state" at the end of the run is captured via {@linkplain
 * io.crums.sldg.logledger.Hasher#parseState() LogHasher.parseState()}.
 * If the log is later appended, then recording the {@linkplain io.crums.sldg.logledger.Checkpoint}
 * obviates the need to reparse the log file from the beginning.
 * </p>
 * 
 * @see io.crums.sldg.logledger.Files
 */
package io.crums.sldg.logledger;

