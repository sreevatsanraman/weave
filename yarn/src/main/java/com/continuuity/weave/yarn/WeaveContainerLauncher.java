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
package com.continuuity.weave.yarn;

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.ServiceController;
import com.continuuity.weave.yarn.utils.YarnUtils;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;

import java.io.File;

/**
 *
 */
final class WeaveContainerLauncher {

  private final RuntimeSpecification runtimeSpec;
  private final RunId runId;
  private final ProcessLauncher processLauncher;
  private final ZKClient zkClient;
  private final Iterable<String> args;
  private final int instanceId;
  private ProcessLauncher.ProcessController processController;

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

  ServiceController start() {
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

    processController = afterResources
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

    return new AbstractServiceController(zkClient, runId){};
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
