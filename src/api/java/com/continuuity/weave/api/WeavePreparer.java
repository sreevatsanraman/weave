/**
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

import com.continuuity.weave.api.logging.LogHandler;

/**
 *
 */
public interface WeavePreparer {

  /**
   * Adds a {@link LogHandler} for receiving application log.
   * @param handler The {@link LogHandler}.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer addLogHandler(LogHandler handler);

  /**
   * Sets the list of arguments that will be passed to the application. The arguments can be retrieved
   * from {@link com.continuuity.weave.api.WeaveContext#getApplicationArguments()}.
   *
   * @param args Array of arguments.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withApplicationArguments(String... args);

  /**
   * Sets the list of arguments that will be passed to the application. The arguments can be retrieved
   * from {@link com.continuuity.weave.api.WeaveContext#getApplicationArguments()}.
   *
   * @param args Iterable of arguments.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withApplicationArguments(Iterable<String> args);

  /**
   * Sets the list of arguments that will be passed to the {@link WeaveRunnable} identified by the given name.
   * The arguments can be retrieved from {@link com.continuuity.weave.api.WeaveContext#getArguments()}.
   *
   * @param runnableName Name of the {@link WeaveRunnable}.
   * @param args Array of arguments.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withArguments(String runnableName, String...args);

  /**
   * Sets the list of arguments that will be passed to the {@link WeaveRunnable} identified by the given name.
   * The arguments can be retrieved from {@link com.continuuity.weave.api.WeaveContext#getArguments()}.
   *
   * @param runnableName Name of the {@link WeaveRunnable}.
   * @param args Iterable of arguments.
   * @return This {@link WeavePreparer}.
   */
  WeavePreparer withArguments(String runnableName, Iterable<String> args);

  /**
   * Starts the application.
   * @return A {@link WeaveController} for controlling the running application.
   */
  WeaveController start();
}
