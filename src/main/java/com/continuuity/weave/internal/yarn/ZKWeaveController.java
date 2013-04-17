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
import com.continuuity.weave.api.RunInfo;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.Messages;
import com.continuuity.zk.NodeData;
import com.continuuity.zk.ZKClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
class ZKWeaveController implements WeaveController {

  private static final Logger LOG = LoggerFactory.getLogger(ZKWeaveController.class);

  private final ZKClient zkClient;
  private final RunId runId;
  private final AtomicReference<RunInfo> runInfo;

  ZKWeaveController(ZKClient zkClient, RunId runId) {
    this.zkClient = zkClient;
    this.runId = runId;
    this.runInfo = new AtomicReference<RunInfo>();
    updateRunInfo(RunInfo.State.NEW);
    watchState();
  }

  @Override
  public RunInfo getRunInfo() {
    return runInfo.get();
  }

  @Override
  public void addLogHandler(LogHandler handler) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public ListenableFuture<?> stop() {
    // TODO: Not so good to create message and encode it in here
    // TODO: Also need to unify with WeaveContainerLauncher
    byte[] data = Messages.encode(new Message() {
      @Override
      public Type getType() {
        return Type.SYSTEM;
      }

      @Override
      public Scope getScope() {
        return Scope.APPLICATION;
      }

      @Override
      public String getRunnableName() {
        return null;
      }

      @Override
      public Command getCommand() {
        return new Command() {
          @Override
          public String getCommand() {
            return "stop";
          }

          @Override
          public Map<String, String> getOptions() {
            return ImmutableMap.of();
          }
        };
      }
    });

    final SettableFuture<String> deleteFuture = SettableFuture.create();
    try {
      Futures.addCallback(zkClient.create("/" + runId + "/messages/msg", data, CreateMode.PERSISTENT_SEQUENTIAL),
                          new FutureCallback<String>() {
                            @Override
                            public void onSuccess(String result) {
                              // TODO: Should look for instance node goes away as well.
                              watchDelete(result, deleteFuture);
                            }

                            @Override
                            public void onFailure(Throwable t) {
                              deleteFuture.setException(t);
                            }
                          });
    } catch (Exception e) {
      deleteFuture.setException(e);
    }

    return deleteFuture;
  }

  @Override
  public ListenableFuture<?> sendCommand(Command command) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean waitFor(long timeout, TimeUnit timeoutUnit) throws InterruptedException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  private void watchState() {
    Futures.addCallback(zkClient.exists("/" + runId + "/state", new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeCreated) {
          getState();
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null) {
          // Starts monitoring the state
          getState();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to watch for state node.", t);
      }
    });
  }

  private void getState() {
    Futures.addCallback(zkClient.getData("/" + runId + "/state", new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged) {
          getState();
        }
      }
    }), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        byte[] data = result.getData();
        if (data == null) {
          LOG.warn("Unknown state");
          return;
        }
        String content = new String(data, Charsets.UTF_8);
        JsonElement json = new Gson().fromJson(content, JsonElement.class);
        if (json == null || !json.isJsonObject()) {
          LOG.warn("Incorrect state format: " + content);
          return;
        }
        JsonObject jsonObj = json.getAsJsonObject();
        if (!jsonObj.has("state")) {
          LOG.warn("No state information: " + content);
          return;
        }
        RunInfo.State state = RunInfo.State.valueOf(jsonObj.get("state").getAsString());
        if (state == RunInfo.State.FAILED) {
          LOG.warn("Run failed: " + jsonObj.get("stackTrace").getAsString());
        }
        updateRunInfo(state);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to fetch state node information.", t);
      }
    });
  }

  private void updateRunInfo(final RunInfo.State state) {

    runInfo.set(new RunInfo() {
      @Override
      public RunId getId() {
        return runId;
      }

      @Override
      public State getState() {
        return state;
      }
    });
  }

  private void watchDelete(final String path, final SettableFuture<String> completion) {
    Futures.addCallback(zkClient.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDeleted) {
          completion.set(path);
        } else {
          watchDelete(path, completion);
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
}
