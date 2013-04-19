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
package com.continuuity.weave.internal.state;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.internal.StackTraceElementCodec;
import com.continuuity.weave.internal.utils.Threads;
import com.continuuity.zk.ForwardingZKClient;
import com.continuuity.zk.NodeChildren;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.OperationFuture;
import com.continuuity.zk.RetryStrategies;
import com.continuuity.zk.ZKClient;
import com.continuuity.zk.ZKClientService;
import com.continuuity.zk.ZKClientServices;
import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Service} decorator that wrap another {@link Service} with the service states reflected
 * to ZooKeeper.
 */
public final class ZKServiceDecorator extends AbstractService {

  private static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();
  private static final Logger LOG = LoggerFactory.getLogger(ZKServiceDecorator.class);

  private final ZKClientService zkClient;
  private final RunId id;
  private final Supplier<? extends JsonElement> liveNodeData;
  private final Service decoratedService;
  private final MessageCallbackCaller messageCallback;
  private ExecutorService callbackExecutor;

  public ZKServiceDecorator(String zkConnect, int zkTimeout, RunId id,
                            Supplier<? extends JsonElement> liveNodeData, Service decoratedService) {
    // Creates a retry on failure zk client
    this.zkClient = ZKClientServices.reWatchOnExpire(
                      ZKClientServices.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                                        .setSessionTimeout(zkTimeout)
                                                        .setConnectionWatcher(createConnectionWatcher())
                                                        .build(),
                                                      RetryStrategies.exponentialDelay(100, 2000,
                                                                                       TimeUnit.MILLISECONDS)));
    this.id = id;
    this.liveNodeData = liveNodeData;
    this.decoratedService = decoratedService;
    if (decoratedService instanceof MessageCallback) {
      this.messageCallback = new MessageCallbackCaller((MessageCallback) decoratedService, zkClient);
    } else {
      this.messageCallback = new MessageCallbackCaller(zkClient);
    }
  }

  /**
   * Returns the {@link ZKClient} used by this decorator.
   * @return
   *
   * TODO: This method is a bit hacky. Need to find a better way to share a ZK connection between different components.
   */
  public ZKClient getZKClient() {
    return new ForwardingZKClient(zkClient) {
    };
  }

  @Override
  protected void doStart() {
    callbackExecutor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("message-callback"));

    // Connect to zookeeper.
    Futures.addCallback(zkClient.start(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        // Create nodes for states and messaging
        StateNode stateNode = new StateNode(WeaveController.State.STARTING, null);
        createMessagesNode();
        final OperationFuture<String> stateFuture = zkClient.create(getZKPath("state"),
                                                                    encodeStateNode(stateNode),
                                                                    CreateMode.PERSISTENT);
        stateFuture.addListener(new Runnable() {
          @Override
          public void run() {
            try {
              stateFuture.get();
              // Starts the decorated service
              decoratedService.addListener(createListener(), SAME_THREAD_EXECUTOR);
              decoratedService.start();
            } catch (Exception e) {
              notifyFailed(e);
            }
          }
        }, SAME_THREAD_EXECUTOR);
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    });
  }

  @Override
  protected void doStop() {
    // Stops the decorated service
    decoratedService.stop();
  }

  private Watcher createConnectionWatcher() {
    final AtomicReference<Watcher.Event.KeeperState> keeperState = new AtomicReference<Watcher.Event.KeeperState>();

    return new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // When connected (either first time or reconnected from expiration), creates a ephemeral node
        Event.KeeperState current = event.getState();
        Event.KeeperState previous = keeperState.getAndSet(current);

        LOG.info("Connection state changed " + previous + " => " + current);

        if (current == Event.KeeperState.SyncConnected && (previous == null || previous == Event.KeeperState.Expired)) {
          String liveNode = "/instances/" + id;
          LOG.info("Create live node " + liveNode);

          JsonObject content = new JsonObject();
          content.add("data", liveNodeData.get());
          listenFailure(zkClient.create(liveNode, encodeJson(content), CreateMode.EPHEMERAL));
        }
      }
    };
  }

  private void createMessagesNode() {
    final OperationFuture<String> future = zkClient.create(getZKPath("messages"), null, CreateMode.PERSISTENT);
    future.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          future.get();
          watchMessages();
        } catch (Exception e) {
          // TODO: what could be done besides just logging?
          LOG.error("Failed to create node " + future.getRequestPath(), e);
        }
      }
    }, SAME_THREAD_EXECUTOR);
  }

  private void watchMessages() {
    final String messagesPath = getZKPath("messages");
    Futures.addCallback(zkClient.getChildren(messagesPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // TODO: Do we need to deal with other type of events?
        if (event.getType() == Event.EventType.NodeChildrenChanged && decoratedService.isRunning()) {
          watchMessages();
        }
      }
    }), new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        // Sort by the name, which is the messageId. Assumption is that message ids is ordered by time.
        List<String> messages = Lists.newArrayList(result.getChildren());
        Collections.sort(messages);
        for (String messageId : messages) {
          processMessage(messagesPath + "/" + messageId, messageId);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // TODO: what could be done besides just logging?
        LOG.error("Failed to watch messages.", t);
      }
    });
  }

  private void processMessage(final String path, final String messageId) {
    Futures.addCallback(zkClient.getData(path), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(final NodeData result) {
        Message message = MessageCodec.decode(result.getData());
        if (message == null) {
          LOG.error("Failed to decode message for " + messageId + " in " + path);
          listenFailure(zkClient.delete(path, result.getStat().getVersion()));
          return;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Message received from " + path + ": " + new String(MessageCodec.encode(message), Charsets.UTF_8));
        }
        if (message.getType() == Message.Type.SYSTEM && SystemMessages.STOP.equals(message)) {
            decoratedService.stop().addListener(new Runnable() {

              @Override
              public void run() {
                stopServiceOnComplete(zkClient.delete(path, result.getStat().getVersion()), ZKServiceDecorator.this);
              }
            }, MoreExecutors.sameThreadExecutor());
          return;
        }
        messageCallback.onReceived(callbackExecutor, path, result.getStat().getVersion(), messageId, message);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to fetch message content.", t);
      }
    });
  }

  private Listener createListener() {
    return new DecoratedServiceListener();
  }

  private <V> byte[] encode(V data, Class<? extends V> clz) {
    return new GsonBuilder().registerTypeAdapter(StateNode.class, new StateNode.StateNodeCodec())
                            .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
                            .create()
      .toJson(data, clz).getBytes(Charsets.UTF_8);
  }

  private byte[] encodeStateNode(StateNode stateNode) {
    return encode(stateNode, StateNode.class);
  }

  private <V extends JsonElement> byte[] encodeJson(V json) {
    return encode(json, json.getClass());
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", id, path);
  }

  private static <V> OperationFuture<V> listenFailure(final OperationFuture<V> operationFuture) {
    operationFuture.addListener(new Runnable() {

      @Override
      public void run() {
        try {
          if (!operationFuture.isCancelled()) {
            operationFuture.get();
          }
        } catch (Exception e) {
          // TODO: what could be done besides just logging?
          LOG.error("Operation execution failed for " + operationFuture.getRequestPath(), e);
        }
      }
    }, SAME_THREAD_EXECUTOR);
    return operationFuture;
  }

  private static final class MessageCallbackCaller {
    private final MessageCallback callback;
    private final ZKClient zkClient;

    private MessageCallbackCaller(ZKClient zkClient) {
      this(null, zkClient);
    }

    private MessageCallbackCaller(MessageCallback callback, ZKClient zkClient) {
      this.callback = callback;
      this.zkClient = zkClient;
    }

    public void onReceived(Executor executor, final String path,
                           final int version, final String id, final Message message) {
      if (callback == null) {
        // Simply delete the message
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignoring incoming message from " + path + ": " + message);
        }
        listenFailure(zkClient.delete(path, version));
        return;
      }

      executor.execute(new Runnable() {

        @Override
        public void run() {
          Futures.addCallback(callback.onReceived(id, message), new FutureCallback<String>() {
            @Override
            public void onSuccess(String result) {
              // Delete the message node when processing is completed successfully.
              listenFailure(zkClient.delete(path, version));
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Failed to process message for " + id + " in " + path, t);
            }
          });
        }
      });
    }
  }

  private class DecoratedServiceListener implements Listener {
    private volatile boolean zkFailure = false;

    @Override
    public void starting() {
      LOG.info("Starting: " + id);
      saveState(WeaveController.State.STARTING);
    }

    @Override
    public void running() {
      LOG.info("Running: " + id);
      notifyStarted();
      saveState(WeaveController.State.RUNNING);
    }

    @Override
    public void stopping(State from) {
      LOG.info("Stopping: " + id);
      saveState(WeaveController.State.STOPPING);
    }

    @Override
    public void terminated(State from) {
      LOG.info("Terminated: " + from + " " + id);
      if (zkFailure) {
        return;
      }
      StateNode stateNode = new StateNode(WeaveController.State.TERMINATED, null);
      Futures.addCallback(stopServiceOnComplete(zkClient.setData(getZKPath("state"),
                                                                 encodeStateNode(stateNode)),
                                                zkClient),
        new FutureCallback<State>() {
          @Override
          public void onSuccess(State result) {
            notifyStopped();
          }

          @Override
          public void onFailure(Throwable t) {
            notifyFailed(t);
          }
        });
    }

    @Override
    public void failed(State from, final Throwable failure) {
      LOG.info("Failed: " + from + " " + id);
      if (zkFailure) {
        return;
      }

      StateNode stateNode = new StateNode(WeaveController.State.FAILED, failure.getStackTrace());
      stopServiceOnComplete(zkClient.setData(getZKPath("state"),
                                             encodeStateNode(stateNode)),
                            zkClient).addListener(new Runnable() {
        @Override
        public void run() {
          notifyFailed(failure);
        }
      }, SAME_THREAD_EXECUTOR);
    }

    private void saveState(WeaveController.State state) {
      if (zkFailure) {
        return;
      }
      StateNode stateNode = new StateNode(state, null);
      stopOnFailure(zkClient.setData(getZKPath("state"), encodeStateNode(stateNode)));
    }

    private <V> void stopOnFailure(final OperationFuture<V> future) {
      future.addListener(new Runnable() {
        @Override
        public void run() {
          try {
            future.get();
          } catch (final Exception e) {
            LOG.error("ZK operation failed", e);
            zkFailure = true;
            stopServiceOnComplete(decoratedService.stop(), zkClient).addListener(new Runnable() {
              @Override
              public void run() {
                notifyFailed(e);
              }
            }, SAME_THREAD_EXECUTOR);
          }
        }
      }, SAME_THREAD_EXECUTOR);
    }
  }

  private <V> ListenableFuture<State> stopServiceOnComplete(ListenableFuture <V> future, final Service service) {
    return Futures.transform(future, new AsyncFunction<V, State>() {
      @Override
      public ListenableFuture<State> apply(V input) throws Exception {
        return service.stop();
      }
    });
  }
}
