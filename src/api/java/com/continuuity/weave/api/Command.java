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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Represents command objects.
 */
public interface Command {

  String getCommand();

  Map<String, String> getOptions();

  /**
   * Builder for creating {@link Command} object.
   */
  static final class Builder {

    private final String command;
    private final ImmutableMap.Builder<String, String> options = ImmutableMap.builder();

    public static Builder of(String command) {
      Preconditions.checkArgument(command != null, "Command cannot be null.");
      return new Builder(command);
    }

    public Builder addOption(String key, String value) {
      options.put(key, value);
      return this;
    }

    public Builder addOptions(Map<String, String> map) {
      options.putAll(map);
      return this;
    }

    public Command build() {
      return new SimpleCommand(command, options.build());
    }

    private Builder(String command) {
      this.command = command;
    }
  }
}
