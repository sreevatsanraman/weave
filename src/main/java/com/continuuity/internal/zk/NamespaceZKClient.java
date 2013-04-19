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
package com.continuuity.internal.zk;

import com.continuuity.zookeeper.ForwardingZKClient;
import com.continuuity.zookeeper.NodeChildren;
import com.continuuity.zookeeper.NodeData;
import com.continuuity.zookeeper.OperationFuture;
import com.continuuity.zookeeper.ZKClient;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;

/**
*
*/
public final class NamespaceZKClient extends ForwardingZKClient {

  private static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
  private final String namespace;

  public NamespaceZKClient(ZKClient delegate, String namespace) {
    super(delegate);
    this.namespace = namespace;
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode) {
    return relayPath(super.create(namespace + path, data, createMode), this.<String>createFuture(path));
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode,
                                        boolean createParent) {
    return relayPath(super.create(namespace + path, data, createMode, createParent), this.<String>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> exists(String path) {
    return relayFuture(super.exists(namespace + path), this.<Stat>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> exists(String path, @Nullable Watcher watcher) {
    return relayFuture(super.exists(namespace + path, watcher), this.<Stat>createFuture(path));
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path) {
    return relayFuture(super.getChildren(namespace + path), this.<NodeChildren>createFuture(path));
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path, @Nullable Watcher watcher) {
    return relayFuture(super.getChildren(namespace + path, watcher), this.<NodeChildren>createFuture(path));
  }

  @Override
  public OperationFuture<NodeData> getData(String path) {
    return relayFuture(super.getData(namespace + path), this.<NodeData>createFuture(path));
  }

  @Override
  public OperationFuture<NodeData> getData(String path, @Nullable Watcher watcher) {
    return relayFuture(super.getData(namespace + path, watcher), this.<NodeData>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> setData(String path, byte[] data) {
    return relayFuture(super.setData(namespace + path, data), this.<Stat>createFuture(path));
  }

  @Override
  public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
    return relayFuture(super.setData(namespace + dataPath, data, version), this.<Stat>createFuture(dataPath));
  }

  @Override
  public OperationFuture<String> delete(String path) {
    return relayPath(super.delete(namespace + path), this.<String>createFuture(path));
  }

  @Override
  public OperationFuture<String> delete(String deletePath, int version) {
    return relayPath(super.delete(namespace + deletePath, version), this.<String>createFuture(deletePath));
  }

  private <V> SettableOperationFuture<V> createFuture(String path) {
    return SettableOperationFuture.create(namespace + path, SAME_THREAD_EXECUTOR);
  }

  private <V> OperationFuture<V> relayFuture(final OperationFuture<V> from, final SettableOperationFuture<V> to) {
    from.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          to.set(from.get());
        } catch (Exception e) {
          to.setException(e);
        }
      }
    }, SAME_THREAD_EXECUTOR);
    return to;
  }

  private OperationFuture<String> relayPath(final OperationFuture<String> from,
                                            final SettableOperationFuture<String> to) {
    from.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          String path = from.get();
          to.set(path.substring(namespace.length()));
        } catch (Exception e) {
          to.setException(e);
        }
      }
    }, SAME_THREAD_EXECUTOR);
    return to;
  }
}
