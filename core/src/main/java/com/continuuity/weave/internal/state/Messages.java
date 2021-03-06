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
package com.continuuity.weave.internal.state;

import com.continuuity.weave.api.Command;

/**
 * Factory class for creating instances of {@link Message}.
 */
public final class Messages {

  public static Message createForRunnable(String runnableName, Command command) {
    return new SimpleMessage(Message.Type.USER, Message.Scope.RUNNABLE, runnableName, command);
  }

  public static Message createForAll(Command command) {
    return new SimpleMessage(Message.Type.USER, Message.Scope.ALL_RUNNABLE, null, command);
  }

  public static Message createForApplication(Command command) {
    return new SimpleMessage(Message.Type.USER, Message.Scope.APPLICATION, null, command);
  }

  private Messages() {
  }
}
