/*
 * Copyright 2020-2021 Babak Farhang
 */
/**
 * {@linkplain io.crums.sldg.SkipLedger SkipLedger}s access rows with row
 * numbers that are multiples of higher powers of 2 more often than those
 * that are multiples of lower numbers. This fact, combined with the observation
 * our rows are <em>not</em> supposed to be ever updated (under normal operation),
 * presents unique opportunties for caching in this application.
 */
package io.crums.sldg.cache;