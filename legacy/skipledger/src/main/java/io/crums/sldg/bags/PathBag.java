/*
 * Copyright 2021 Babak Farhang
 */
package io.crums.sldg.bags;


import java.util.List;

import io.crums.sldg.PathInfo;

/**
 * A bag of optional path declarations. Declared paths are informational meta. This
 * library doesn't understand what that meta information means: presently it is
 * meant either for human consumption, or for some yet-to-be-designed tool that makes
 * use of it.
 */
public interface PathBag {
  
  /**
   * Returns the user-defined declared paths.
   */
  List<PathInfo> declaredPaths();

}
