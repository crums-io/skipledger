/*
 * Copyright 2021 Babak Farhang
 */
/**
 * Interfaces for retrieving a information about a subset of data from a {@linkplain Ledger ledger}.
 * These are broken out into individual compositional components instead of a type hieirarchy. The
 * goal here is to break the implementation down into smaller pieces which can then be combined
 * to form a {@linkplain MorselBag}.
 */
package io.crums.sldg.bags;