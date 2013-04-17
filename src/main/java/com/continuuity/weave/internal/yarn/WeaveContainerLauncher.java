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
package com.continuuity.weave.internal.yarn;

import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.Messages;
import com.continuuity.weave.internal.utils.YarnUtils;
import com.continuuity.zk.ZKClient;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 *
 */
public final class WeaveContainerLauncher extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(WeaveContainerLauncher.class);

  private final WeaveSpecification weaveSpec;
  private final File weaveSpecFile;
  private final String runnableName;
  private final RunId runId;
  private final ProcessLauncher processLauncher;
  private final ZKClient zkClient;
  private final String zkConnectStr;
  private ProcessLauncher.ProcessController controller;

  public WeaveContainerLauncher(WeaveSpecification weaveSpec,
                                File weaveSpecFile,
                                String runnableName,
                                RunId runId,
                                ProcessLauncher processLauncher,
                                ZKClient zkClient,
                                String zkConnectStr) {
    this.weaveSpec = weaveSpec;
    this.weaveSpecFile = weaveSpecFile;
    this.runnableName = runnableName;
    this.runId = runId;
    this.processLauncher = processLauncher;
    // TODO: This is hacky to pass around a ZKClient like this
    this.zkClient = zkClient;
    this.zkConnectStr = zkConnectStr;
  }

  @Override
  protected void startUp() throws Exception {
    RuntimeSpecification runtimeSpec = weaveSpec.getRunnables().get(runnableName);

    ProcessLauncher.PrepareLaunchContext.AfterUser afterUser = processLauncher.prepareLaunch()
      .setUser(System.getProperty("user.name"));

    ProcessLauncher.PrepareLaunchContext.MoreResources moreResources =
      afterUser.withResources().add("weave.spec", YarnUtils.createLocalResource(LocalResourceType.FILE, weaveSpecFile));

    for (LocalFile localFile : runtimeSpec.getLocalFiles()) {
      File file = new File(runnableName + "." + localFile.getName());
      LocalResource localRsc = setLocalResourceType(localFile,
                                                    YarnUtils.createLocalResource(LocalResourceType.FILE, file));
      moreResources = moreResources.add(localFile.getName(), localRsc);
    }

    controller = moreResources.withCommands()
      .add("java",
           WeaveContainerMain.class.getName(),
           zkConnectStr,
           "weave.spec",
           runnableName,
           runId.getId())
      .redirectOutput("/tmp/container." + runnableName + ".out")
      .redirectError("/tmp/container." + runnableName + ".err")
      .launch();
  }

  @Override
  protected void shutDown() throws Exception {
    // TODO: Not so good to create message and encode it in here
    // TODO: Also need to unify with WeaveController
    byte[] data = Messages.encode(new Message() {
      @Override
      public Type getType() {
        return Type.SYSTEM;
      }

      @Override
      public Scope getScope() {
        return Scope.RUNNABLE;
      }

      @Override
      public String getRunnableName() {
        return runnableName;
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
    // TODO: Should be wait for instance node to go away as well.
    Futures.addCallback(zkClient.create("/" + runId + "/messages/msg", data, CreateMode.PERSISTENT_SEQUENTIAL),
                        new FutureCallback<String>() {
                          @Override
                          public void onSuccess(String result) {
                            watchDelete(result, deleteFuture);
                          }

                          @Override
                          public void onFailure(Throwable t) {
                            deleteFuture.setException(t);
                          }
                        });

    deleteFuture.get();

//    controller.stop();
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

  private LocalResource setLocalResourceType(LocalFile localFile, LocalResource localResource) {
    if (localFile.isArchive()) {
      if (localFile.getPattern() == null) {
        localResource.setType(LocalResourceType.ARCHIVE);
      } else {
        localResource.setType(LocalResourceType.PATTERN);
        localResource.setPattern(localFile.getPattern());
      }
    } else {
      localResource.setType(LocalResourceType.FILE);
    }
    return localResource;
  }
}
