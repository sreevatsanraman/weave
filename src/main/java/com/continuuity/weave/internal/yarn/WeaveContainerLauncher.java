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

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.internal.state.MessageCodec;
import com.continuuity.weave.internal.state.SystemMessages;
import com.continuuity.weave.internal.utils.YarnUtils;
import com.continuuity.zookeeper.ZKClient;
import com.continuuity.zookeeper.ZKOperations;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.zookeeper.CreateMode;

import java.io.File;

/**
 *
 */
final class WeaveContainerLauncher extends AbstractIdleService {

  private final RuntimeSpecification runtimeSpec;
  private final RunId runId;
  private final ProcessLauncher processLauncher;
  private final ZKClient zkClient;
  private final Iterable<String> args;
  private final int instanceId;
  private ProcessLauncher.ProcessController controller;

  WeaveContainerLauncher(RuntimeSpecification runtimeSpec, RunId runId, ProcessLauncher processLauncher,
                         ZKClient zkClient, Iterable<String> args, int instanceId) {
    this.runtimeSpec = runtimeSpec;
    this.runId = runId;
    this.processLauncher = processLauncher;
    // TODO: This is hacky to pass around a ZKClient like this
    this.zkClient = zkClient;
    this.args = args;
    this.instanceId = instanceId;
  }

  @Override
  protected void startUp() throws Exception {
    ProcessLauncher.PrepareLaunchContext.AfterUser afterUser = processLauncher.prepareLaunch()
      .setUser(System.getProperty("user.name"));

    ProcessLauncher.PrepareLaunchContext.AfterResources afterResources = null;
    if (runtimeSpec.getLocalFiles().isEmpty()) {
      afterResources = afterUser.noResources();
    }

    ProcessLauncher.PrepareLaunchContext.ResourcesAdder resourcesAdder = afterUser.withResources();

    for (LocalFile localFile : runtimeSpec.getLocalFiles()) {
      File file = new File(runtimeSpec.getName() + "." + localFile.getName());
      LocalResource localRsc = setLocalResourceType(localFile,
                                                    YarnUtils.createLocalResource(LocalResourceType.FILE, file));
      afterResources = resourcesAdder.add(localFile.getName(), localRsc);
    }

    controller = afterResources
      .withEnvironment()
        .add(EnvKeys.WEAVE_RUN_ID, runId.getId())
        .add(EnvKeys.WEAVE_RUNNABLE_NAME, runtimeSpec.getName())
        .add(EnvKeys.WEAVE_INSTANCE_ID, Integer.toString(instanceId))
      .withCommands()
        .add("java",
             ImmutableList.<String>builder()
               .add("-cp").add(System.getenv(EnvKeys.WEAVE_CONTAINER_JAR_PATH))
               .add(WeaveContainerMain.class.getName())
               .addAll(args).build().toArray(new String[0]))
      .noOutput().noError()
      .launch();
  }

  @Override
  protected void shutDown() throws Exception {
    // TODO: Need to unify with WeaveController
    byte[] data = MessageCodec.encode(SystemMessages.stopRunnable(runtimeSpec.getName()));
    final SettableFuture<String> deleteFuture = SettableFuture.create();
    // TODO: Should wait for instance node to go away as well.
    Futures.addCallback(zkClient.create("/" + runId + "/messages/msg", data, CreateMode.PERSISTENT_SEQUENTIAL),
                        new FutureCallback<String>() {
                          @Override
                          public void onSuccess(String result) {
                            ZKOperations.watchDeleted(zkClient, result, deleteFuture);
                          }

                          @Override
                          public void onFailure(Throwable t) {
                            deleteFuture.setException(t);
                          }
                        });

    deleteFuture.get();

//    controller.stop();
  }

  RunId getRunId() {
    return runId;
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
