/*
 * Copyright 2022 Babak Farhang
 */
/**
 * Utilities for text-based, or equivalently, log-based ledgers.
 * <p>
 * For now, we concentrate on a stream of lines of text, with each line
 * representing a row. The principal idea here is that we won't be necessarily
 * building a persistent skip ledger as we process the lines of text; instead,
 * we'll keep only enough information to allow calculating the hash of
 * <em>new</em> rows as they come in.
 * </p><p>
 * Use cases for this model mostly involve audit. We are recording the state
 * of a ledger via a judicious hashing strategy without doing much else. In particular,
 * if we later wish to provide a differential proof that contents of some previous
 * row matches the current hash of the ledger, we'd likely have to rebuild much of the
 * skip ledger from scratch in order to provide that proof.
 * </p><p>
 * About the <em>for now</em>.. see, it doesn't have to be <em>text</em>. Any
 * stream of bytes might be choppable this way. Even video streams.
 * </p>
 */
package io.crums.sldg.logs.text;