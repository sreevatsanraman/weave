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

import com.continuuity.internal.discovery.ZKDiscoveryService;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.RunIds;
import com.continuuity.weave.internal.ServiceMain;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.continuuity.weave.internal.utils.Services;
import com.continuuity.zookeeper.DiscoveryService;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.concurrent.Executor;

/**
 *
 */
public final class WeaveContainerMain extends ServiceMain {

  /**
   * Main method for launching a {@link WeaveContainerService} which runs
   * a {@link com.continuuity.weave.api.WeaveRunnable}.
   */
  public static void main(final String[] args) throws Exception {
    String zkConnectStr = System.getenv(EnvKeys.WEAVE_ZK_CONNECT);
    File weaveSpecFile = new File(System.getenv(EnvKeys.WEAVE_SPEC_PATH));
    RunId runId = RunIds.fromString(System.getenv(EnvKeys.WEAVE_RUN_ID));
    String runnableName = System.getenv(EnvKeys.WEAVE_RUNNABLE_NAME);
    int instanceId = Integer.parseInt(System.getenv(EnvKeys.WEAVE_INSTANCE_ID));

    System.out.println("ZK: " + zkConnectStr);
    System.out.println("Container ZK: " + getContainerZKConnect(zkConnectStr, runnableName));

    DiscoveryService discoveryService = new ZKDiscoveryService(zkConnectStr);

    WeaveSpecification weaveSpec = loadWeaveSpec(weaveSpecFile);
    WeaveRunnableSpecification runnableSpec = weaveSpec.getRunnables().get(runnableName).getRunnableSpecification();
    ContainerInfo containerInfo = new ContainerInfo();
    WeaveContext context = new BasicWeaveContext(containerInfo.getHost(), args,
                                                  decodeArgs(System.getenv(EnvKeys.WEAVE_APPLICATION_ARGS)),
                                                  runnableSpec, instanceId, discoveryService);
    new WeaveContainerMain().doMain(wrapService(new WeaveContainerService(context, containerInfo,
                                                              getContainerZKConnect(zkConnectStr, runnableName),
                                                              runId, runnableSpec, ClassLoader.getSystemClassLoader()),
                                                discoveryService));
  }

  private static String getContainerZKConnect(String zkConnectStr, String runnableName) {
    return String.format("%s/runnables/%s", zkConnectStr, runnableName);
  }

  private static WeaveSpecification loadWeaveSpec(File specFile) throws IOException {
    Reader reader = Files.newReader(specFile, Charsets.UTF_8);
    try {
      return WeaveSpecificationAdapter.create().fromJson(reader);
    } finally {
      reader.close();
    }
  }

  private static String[] decodeArgs(String args) {
    return new Gson().fromJson(args, String[].class);
  }

  private static Service wrapService(final Service service, final DiscoveryService discoverService) {
    return new Service() {

      @Override
      public ListenableFuture<State> start() {
        return Futures.transform(Services.chainStart(discoverService, service),
                                 new AsyncFunction<List<ListenableFuture<State>>, State>() {
                                   @Override
                                   public ListenableFuture<State> apply(List<ListenableFuture<State>> input) throws Exception {
                                     return input.get(input.size() - 1);
                                   }
                                 });
      }

      @Override
      public State startAndWait() {
        return Futures.getUnchecked(start());
      }

      @Override
      public boolean isRunning() {
        return service.isRunning();
      }

      @Override
      public State state() {
        return service.state();
      }

      @Override
      public ListenableFuture<State> stop() {
        return Futures.transform(Services.chainStop(service, discoverService),
                                 new AsyncFunction<List<ListenableFuture<State>>, State>() {
                                   @Override
                                   public ListenableFuture<State> apply(List<ListenableFuture<State>> input) throws Exception {
                                     return input.get(0);
                                   }
                                 });
      }

      @Override
      public State stopAndWait() {
        return Futures.getUnchecked(stop());
      }

      @Override
      public void addListener(Listener listener, Executor executor) {
        service.addListener(listener, executor);
      }
    };
  }

  @Override
  protected String getHostname() {
    return System.getenv(EnvKeys.YARN_CONTAINER_HOST);
  }

  @Override
  protected String getKafkaZKConnect() {
    return System.getenv(EnvKeys.WEAVE_LOG_KAFKA_ZK);
  }

  @Override
  protected File getLogBackTemplate() {
    return new File(System.getenv(EnvKeys.WEAVE_LOGBACK_PATH));
  }
}
