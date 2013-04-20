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

/**
 * A abstract base class to simplify implementation of {@link ServiceController.Listener}.
 */
public abstract class ListenerAdapter implements ServiceController.Listener {

  @Override
  public void starting() {
    // No-op.
  }

  @Override
  public void running() {
    // No-op.
  }

  @Override
  public void stopping() {
    // No-op.
  }

  @Override
  public void terminated() {
    // No-op.
  }

  @Override
  public void failed(StackTraceElement[] stackTraces) {
    // No-op.
  }
}
