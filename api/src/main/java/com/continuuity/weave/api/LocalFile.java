/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.weave.api;

import javax.annotation.Nullable;
import java.net.URI;

/**
 * This interface represents a local file that will be available for the container running a {@link WeaveRunnable}.
 */
public interface LocalFile {

  String getName();

  URI getURI();

  /**
   * Returns the the last modified time of the file or {@code -1} if unknown.
   */
  long getLastModified();

  /**
   * Returns the size of the file or {@code -1} if unknown.
   */
  long getSize();

  boolean isArchive();

  @Nullable
  String getPattern();
}
