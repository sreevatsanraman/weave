/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.common;

import com.google.common.util.concurrent.Service;

/**
 * An adapter for implementing {@link Service.Listener} with all method default to no-op.
 */
public abstract class ServiceListenerAdapter implements Service.Listener {
  @Override
  public void starting() {
    // No-op
  }

  @Override
  public void running() {
    // No-op
  }

  @Override
  public void stopping(Service.State from) {
    // No-op
  }

  @Override
  public void terminated(Service.State from) {
    // No-op
  }

  @Override
  public void failed(Service.State from, Throwable failure) {
    // No-op
  }
}
