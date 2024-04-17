/*
 * Copyright 2024 Babak Farhang
 */
/**
 * There's only one parser here and that's for a path.
 * We use skip paths to encode ledger state (why we call them <em>state</em> paths.
 * Being compact, we'd like a text-based representation that works every where,
 * so that you can pass paths around, much like we use 64-char hex strings for
 * straight SHA-256 hashes.
 */
package io.crums.sldg.json;