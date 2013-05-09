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
package com.continuuity.zookeeper;

import com.google.common.base.Preconditions;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 * Provides static factory method to create {@link ZKClientService} with modified behaviors.
 */
public final class ZKClientServices {

  /**
   * Creates a {@link ZKClientService} from the given {@link ZKClient} if the given {@link ZKClient} is an instance of
   * {@link ZKClientService} or is a {@link ForwardingZKClient} that eventually trace back to a delegate of type
   * {@link ZKClientService}. If such a {@link ZKClientService} instance is found, this method returns
   * an instance by invoking {@link #delegate(ZKClient, ZKClientService)} with the given {@link ZKClient} and
   * the {@link ZKClientService} found respectively.
   *
   * @param client The {@link ZKClient}.
   * @return A {@link ZKClientService}.
   * @throws IllegalArgumentException If no {@link ZKClientService} is found.
   */
  public static ZKClientService delegate(ZKClient client) {
    ZKClient zkClient = client;
    while (!(zkClient instanceof ZKClientService) && zkClient instanceof ForwardingZKClient) {
      zkClient = ((ForwardingZKClient) zkClient).getDelegate();
    }
    Preconditions.checkArgument(zkClient instanceof ZKClientService,
                                "No ZKClientService found from the delegation hierarchy");
    return delegate(client, (ZKClientService) zkClient);
  }

  /**
   * Creates a {@link ZKClientService} that for all {@link ZKClient} methods would be delegated to another
   * {@link ZKClient}, while methods for {@link ZKClientService} would be delegated to another {@link ZKClientService},
   * which the given {@link ZKClient} and {@link ZKClientService} could be different instances.
   *
   * @param client The {@link ZKClient} for delegation
   * @param clientService The {@link ZKClientService} for delegation.
   * @return A {@link ZKClientService}.
   */
  public static ZKClientService delegate(final ZKClient client, ZKClientService clientService) {
    return new ForwardingZKClientService(clientService) {
      public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode) {
        return client.create(path, data, createMode);
      }

      public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode,
                                            boolean createParent) {
        return client.create(path, data, createMode, createParent);
      }

      public OperationFuture<Stat> exists(String path) {
        return client.exists(path);
      }

      public OperationFuture<Stat> exists(String path, @Nullable Watcher watcher) {
        return client.exists(path, watcher);
      }

      public OperationFuture<NodeChildren> getChildren(String path) {
        return client.getChildren(path);
      }

      public OperationFuture<NodeChildren> getChildren(String path, @Nullable Watcher watcher) {
        return client.getChildren(path, watcher);
      }

      public OperationFuture<NodeData> getData(String path) {
        return client.getData(path);
      }

      public OperationFuture<NodeData> getData(String path, @Nullable Watcher watcher) {
        return client.getData(path, watcher);
      }

      public OperationFuture<Stat> setData(String path, byte[] data) {
        return client.setData(path, data);
      }

      public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
        return client.setData(dataPath, data, version);
      }

      public OperationFuture<String> delete(String path) {
        return client.delete(path);
      }

      public OperationFuture<String> delete(String deletePath, int version) {
        return client.delete(deletePath, version);
      }
    };
  }

  private ZKClientServices() {
  }
}
