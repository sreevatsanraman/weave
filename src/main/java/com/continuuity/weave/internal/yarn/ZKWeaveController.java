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
package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.internal.StackTraceElementCodec;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.MessageCodec;
import com.continuuity.weave.internal.state.Messages;
import com.continuuity.weave.internal.state.StateNode;
import com.continuuity.weave.internal.state.SystemMessages;
import com.continuuity.zookeeper.NodeData;
import com.continuuity.zookeeper.RetryStrategies;
import com.continuuity.zookeeper.ZKClientService;
import com.continuuity.zookeeper.ZKClientServices;
import com.continuuity.zookeeper.ZKOperations;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.GsonBuilder;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
class ZKWeaveController implements WeaveController {

  private static final Logger LOG = LoggerFactory.getLogger(ZKWeaveController.class);

  private final ZKClientService zkClient;
  private final RunId runId;
  private final Queue<LogHandler> logHandlers;
  private final ListenerExecutors listenerExecutors;
  private final AtomicReference<State> state;

  ZKWeaveController(String zkConnect, int zkTimeout, RunId runId) {
    this.runId = runId;
    // Creates a retry on failure zk client
    this.zkClient = ZKClientServices.reWatchOnExpire(
      ZKClientServices.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                        .setSessionTimeout(zkTimeout)
                                        .build(),
                                      RetryStrategies.exponentialDelay(100, 2000,
                                                                       TimeUnit.MILLISECONDS)));
    logHandlers = new ConcurrentLinkedQueue<LogHandler>();
    listenerExecutors = new ListenerExecutors();
    state = new AtomicReference<State>();
  }

  void start() {
    Futures.getUnchecked(zkClient.start());

    // Watch for state changes
    ZKOperations.watchData(zkClient, getZKPath("state"), new ZKOperations.DataCallback() {
      @Override
      public void updated(NodeData nodeData) {
        StateNode stateNode = decode(nodeData);
        if (state.getAndSet(stateNode.getState()) != stateNode.getState()) {
          fireStateChange(stateNode);
        }
      }
    });
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public void addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
  }

  @Override
  public ListenableFuture<Command> sendCommand(Command command) {
    return sendMessage(Messages.createForAll(command), command);
  }

  @Override
  public ListenableFuture<Command> sendCommand(String runnableName, Command command) {
    return sendMessage(Messages.createForRunnable(runnableName, command), command);
  }

  @Override
  public State getState() {
    return state.get();
  }

  @Override
  public ListenableFuture<State> stop() {
    return Futures.transform(sendMessage(SystemMessages.STOP, State.TERMINATED), new AsyncFunction<State, State>() {
      @Override
      public ListenableFuture<State> apply(State input) throws Exception {
        // Wait for the instance ephemeral node goes away
        return Futures.transform(ZKOperations.watchDeleted(zkClient, "/instances/" + runId),
                                 new Function<String, State>() {
                                   @Override
                                   public State apply(String input) {
                                     if (LOG.isDebugEnabled()) {
                                       LOG.debug("Remote service stopped: " + runId);
                                     }
                                     state.set(State.TERMINATED);
                                     fireStateChange(new StateNode(State.TERMINATED, null));
                                     return State.TERMINATED;
                                   }
                                 });
      }
    });
  }

  @Override
  public void stopAndWait() {
    Futures.getUnchecked(stop());
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    listenerExecutors.addListener(listener, executor);
  }

  private StateNode decode(NodeData nodeData) {
    // Node data and data inside shouldn't be null. If it does, the service must not be running anymore.
    if (nodeData == null) {
      return new StateNode(WeaveController.State.TERMINATED, null);
    }
    byte[] data = nodeData.getData();
    if (data == null) {
      return new StateNode(WeaveController.State.TERMINATED, null);
    }
    return new GsonBuilder().registerTypeAdapter(StateNode.class, new StateNode.StateNodeCodec())
      .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
      .create()
      .fromJson(new String(data, Charsets.UTF_8), StateNode.class);
  }

  /**
   * Sends a message to remote service by creating the message node. It returns a {@link ListenableFuture} that
   * will be completed when the message is processed, which signaled by removal of the newly created node.
   *
   * @param message Message to sent
   * @param completionResult Result to set into the returning {@link ListenableFuture} when the message is processed.
   * @param <V> Type of the completion result
   * @return A {@link ListenableFuture} that will be completed when message is processed.
   */
  private <V> ListenableFuture<V> sendMessage(final Message message, final V completionResult) {
    return Futures.transform(zkClient.create(getZKPath("messages/msg"), MessageCodec.encode(message),
                                             CreateMode.PERSISTENT_SEQUENTIAL), new AsyncFunction<String, V>() {
      @Override
      public ListenableFuture<V> apply(String path) throws Exception {
        return Futures.transform(ZKOperations.watchDeleted(zkClient, path), new Function<String, V>() {
          @Override
          public V apply(String path) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Message processed in " + path + ": " + message);
            }
            return completionResult;
          }
        });
      }
    });
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", runId, path);
  }

  private void fireStateChange(StateNode state) {
    switch (state.getState()) {
      case STARTING:
        listenerExecutors.starting();
        break;
      case RUNNING:
        listenerExecutors.running();
        break;
      case STOPPING:
        listenerExecutors.stopping();
        break;
      case TERMINATED:
        listenerExecutors.terminated();
        break;
      case FAILED:
        listenerExecutors.failed(state.getStackTraces());
        break;
    }
  }

  private static final class ListenerExecutors implements Listener {
    private final Queue<ListenerExecutor> listeners = new ConcurrentLinkedQueue<ListenerExecutor>();
    private final ConcurrentMap<State, Boolean> callStates = Maps.newConcurrentMap();

    void addListener(Listener listener, Executor executor) {
      listeners.add(new ListenerExecutor(listener, executor));
    }

    @Override
    public void starting() {
      if (hasCalled(State.STARTING)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.starting();
      }
    }

    @Override
    public void running() {
      if (hasCalled(State.RUNNING)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.running();
      }
    }

    @Override
    public void stopping() {
      if (hasCalled(State.STOPPING)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.stopping();
      }
    }

    @Override
    public void terminated() {
      if (hasCalled(State.TERMINATED) || hasCalled(State.FAILED)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.terminated();
      }
    }

    @Override
    public void failed(StackTraceElement[] stackTraces) {
      if (hasCalled(State.FAILED) || hasCalled(State.TERMINATED)) {
        return;
      }
      for (ListenerExecutor listener : listeners) {
        listener.failed(stackTraces);
      }
    }

    private boolean hasCalled(State state) {
      return callStates.putIfAbsent(state, true) != null;
    }
  }

  private static final class ListenerExecutor implements Listener {

    private final Listener delegate;
    private final Executor executor;

    private ListenerExecutor(Listener delegate, Executor executor) {
      this.delegate = delegate;
      this.executor = executor;
    }

    @Override
    public void starting() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.starting();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void running() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.running();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void stopping() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.stopping();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void terminated() {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.terminated();
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }

    @Override
    public void failed(final StackTraceElement[] stackTraces) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            delegate.failed(stackTraces);
          } catch (Throwable t) {
            LOG.warn("Exception thrown from listener", t);
          }
        }
      });
    }
  }
}
