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
package com.continuuity.zk;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Collection of helper methods for common operations that usually needed when interacting with ZooKeeper
 */
public final class ZKOperations {

  private static final Logger LOG = LoggerFactory.getLogger(ZKOperations.class);
  private static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();

  public interface DataCallback {
    /**
     * Invoked when data of the node changed.
     * @param nodeData New data of the node, or {@code null} if the node has been deleted.
     */
    void updated(NodeData nodeData);
  }

  public interface DataSupplier extends Supplier<NodeData>, Cancellable {
  }

  /**
   * Returns a supplier that suppliers latest known node data of the given path.
   *
   * @param zkClient The {@link ZKClient} for the operation.
   * @param path Path for fetching data
   * @return A {@link Supplier} of {@link NodeData}, which upon calling {@link com.google.common.base.Supplier#get()},
   *         the latest copy of NodeData will be returned or {@code null} if the node has been deleted.
   */
  public static DataSupplier getDataSupplier(ZKClient zkClient, String path) {
    final AtomicReference<NodeData> nodeDataRef = new AtomicReference<NodeData>();
    final Cancellable cancellable = watchData(zkClient, path, new DataCallback() {
      @Override
      public void updated(NodeData nodeData) {
        nodeDataRef.set(nodeData);
      }
    });

    return new DataSupplier() {
      @Override
      public NodeData get() {
        return nodeDataRef.get();
      }

      @Override
      public void cancel() {
        cancellable.cancel();
      }
    };
  }

  /**
   * Watch for data changes of the given path. The callback will be triggered whenever changes has been
   * detected. Note that the callback won't see every single changes, as that's not the guarantee of ZooKeeper.
   * If the node doesn't exists, it will watch for its creation then starts watching for data changes.
   * When the node is deleted afterwards,
   *
   * @param zkClient The {@link ZKClient} for the operation
   * @param path Path to watch
   * @param callback Callback to be invoked when data changes is detected.
   * @return A {@link Cancellable} to cancel the watch.
   */
  public static Cancellable watchData(final ZKClient zkClient, final String path, final DataCallback callback) {
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    Futures.addCallback(zkClient.getData(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!cancelled.get()) {
          watchData(zkClient, path, callback);
        }
      }
    }), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        if (!cancelled.get()) {
          callback.updated(result);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException && ((KeeperException) t).code() == KeeperException.Code.NONODE) {
          final SettableFuture<String> existCompletion = SettableFuture.create();
          existCompletion.addListener(new Runnable() {
            @Override
            public void run() {
              try {
                if (!cancelled.get()) {
                  watchData(zkClient, existCompletion.get(), callback);
                }
              } catch (Exception e) {
                LOG.error("Failed to watch data for path " + path, e);
              }
            }
          }, SAME_THREAD_EXECUTOR);
          watchExists(zkClient, path, existCompletion);
          return;
        }
        LOG.error("Failed to watch data for path " + path, t);
      }
    });
    return new Cancellable() {
      @Override
      public void cancel() {
        cancelled.set(true);
      }
    };
  }

  public static ListenableFuture<String> watchDeleted(final ZKClient zkClient, final String path) {
    SettableFuture<String> completion = SettableFuture.create();
    doWatchDeleted(zkClient, path, completion);
    return completion;
  }

  private static void doWatchDeleted(final ZKClient zkClient, final String path,
                                     final SettableFuture<String> completion) {

    Futures.addCallback(zkClient.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!completion.isDone()) {
          if (event.getType() == Event.EventType.NodeDeleted) {
            completion.set(path);
          } else {
            doWatchDeleted(zkClient, path, completion);
          }
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result == null) {
          completion.set(path);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        completion.setException(t);
      }
    });
  }

  /**
   * Watch for the given path until it exists.
   * @param zkClient
   * @param path
   */
  private static void watchExists(final ZKClient zkClient, final String path, final SettableFuture<String> completion) {
    Futures.addCallback(zkClient.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!completion.isDone()) {
          watchExists(zkClient, path, completion);
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null) {
          completion.set(path);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        completion.setException(t);
      }
    });
  }

  private ZKOperations() {
  }
}
