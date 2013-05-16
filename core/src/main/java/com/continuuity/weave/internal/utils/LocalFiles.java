/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.weave.internal.utils;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.internal.ForwardingLocalFile;

import java.io.File;
import java.net.URI;

/**
 *
 */
public final class LocalFiles {

  public static LocalFile create(LocalFile localFile, final File file) {
    return new ForwardingLocalFile(localFile) {
      @Override
      public URI getURI() {
        return file.toURI();
      }

      @Override
      public long getLastModified() {
        return file.lastModified();
      }

      @Override
      public long getSize() {
        return file.length();
      }
    };
  }

  public static LocalFile create(LocalFile localFile, final URI uri, final long lastModified, final long size) {
    return new ForwardingLocalFile(localFile) {
      @Override
      public URI getURI() {
        return uri;
      }

      @Override
      public long getLastModified() {
        return lastModified;
      }

      @Override
      public long getSize() {
        return size;
      }
    };
  }

  private LocalFiles() {
  }
}
