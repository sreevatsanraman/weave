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

import com.continuuity.zk.ForwardingZKClient;
import com.continuuity.zk.NodeChildren;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.OperationFuture;
import com.continuuity.zk.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
*
*/
public final class NamespaceZKClient extends ForwardingZKClient {

  private final String namespace;

  public NamespaceZKClient(ZKClient delegate, String namespace) {
    super(delegate);
    this.namespace = namespace;
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode) {
    SettableOperation
    return super.create(namespace + path, data, createMode);
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode,
                                        boolean createParent) {
    return super.create(namespace + path, data, createMode, createParent);
  }

  @Override
  public OperationFuture<Stat> exists(String path) {
    return super.exists(namespace + path);
  }

  @Override
  public OperationFuture<Stat> exists(String path, @Nullable Watcher watcher) {
    return super.exists(namespace + path, watcher);
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path) {
    return super.getChildren(namespace + path);
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path, @Nullable Watcher watcher) {
    return super.getChildren(namespace + path, watcher);
  }

  @Override
  public OperationFuture<NodeData> getData(String path) {
    return super.getData(namespace + path);
  }

  @Override
  public OperationFuture<NodeData> getData(String path, @Nullable Watcher watcher) {
    return super.getData(namespace + path, watcher);
  }

  @Override
  public OperationFuture<Stat> setData(String path, byte[] data) {
    return super.setData(namespace + path, data);
  }

  @Override
  public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
    return super.setData(namespace + dataPath, data, version);
  }

  @Override
  public OperationFuture<String> delete(String path) {
    return super.delete(namespace + path);
  }

  @Override
  public OperationFuture<String> delete(String deletePath, int version) {
    return super.delete(namespace + deletePath, version);
  }

  private String getPath(String path) {
    return namespace + path;
  }
}
