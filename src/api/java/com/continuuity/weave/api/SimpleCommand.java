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

import com.google.common.base.Objects;

import java.util.Map;

/**
* Simple implementation of {@link Command}.
*/
final class SimpleCommand implements Command {
  private final String command;
  private final Map<String, String> options;

  SimpleCommand(String command, Map<String, String> options) {
    this.command = command;
    this.options = options;
  }

  @Override
  public String getCommand() {
    return command;
  }

  @Override
  public Map<String, String> getOptions() {
    return options;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(command, options);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(Command.class)
      .add("command", command)
      .add("options", options)
      .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Command)) {
      return false;
    }
    Command other = (Command) obj;
    return command.equals(other.getCommand()) && options.equals(other.getOptions());
  }
}
