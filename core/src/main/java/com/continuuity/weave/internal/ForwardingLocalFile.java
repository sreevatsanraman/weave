/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal;

import com.continuuity.weave.api.LocalFile;

import javax.annotation.Nullable;
import java.net.URI;

/**
 *
 */
public abstract class ForwardingLocalFile implements LocalFile {

  private final LocalFile delegate;

  protected ForwardingLocalFile(LocalFile delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public URI getURI() {
    return delegate.getURI();
  }

  @Override
  public long getLastModified() {
    return delegate.getLastModified();
  }

  @Override
  public long getSize() {
    return delegate.getSize();
  }

  @Override
  public boolean isArchive() {
    return delegate.isArchive();
  }

  @Override
  @Nullable
  public String getPattern() {
    return delegate.getPattern();
  }
}
