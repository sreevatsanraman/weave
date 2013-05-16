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

import com.continuuity.weave.api.ServiceController;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 *
 */
public final class StateNode {

  private final ServiceController.State state;
  private final StackTraceElement[] stackTraces;

  public StateNode(ServiceController.State state, StackTraceElement[] stackTraces) {
    this.state = state;
    this.stackTraces = stackTraces;
  }

  public ServiceController.State getState() {
    return state;
  }

  public StackTraceElement[] getStackTraces() {
    return stackTraces;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("state=").append(state);

    if (stackTraces != null) {
      builder.append("\n");
      for (StackTraceElement stackTrace : stackTraces) {
        builder.append("\tat ").append(stackTrace.toString()).append("\n");
      }
    }
    return builder.toString();
  }

}
