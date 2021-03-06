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
package com.continuuity.weave.internal;

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.discovery.DiscoveryService;
import com.continuuity.weave.discovery.ZKDiscoveryService;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class WeaveContainerMain extends ServiceMain {

  /**
   * Main method for launching a {@link com.continuuity.weave.internal.WeaveContainerService} which runs
   * a {@link com.continuuity.weave.api.WeaveRunnable}.
   */
  public static void main(final String[] args) throws Exception {
    String zkConnectStr = System.getenv(EnvKeys.WEAVE_ZK_CONNECT);
    File weaveSpecFile = new File("weaveSpec.json");
    RunId appRunId = RunIds.fromString(System.getenv(EnvKeys.WEAVE_APP_RUN_ID));
    RunId runId = RunIds.fromString(System.getenv(EnvKeys.WEAVE_RUN_ID));
    String runnableName = System.getenv(EnvKeys.WEAVE_RUNNABLE_NAME);
    int instanceId = Integer.parseInt(System.getenv(EnvKeys.WEAVE_INSTANCE_ID));

    ZKClientService zkClientService = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(ZKClientService.Builder.of(zkConnectStr).build(),
                                 RetryStrategies.fixDelay(1, TimeUnit.SECONDS))));

    DiscoveryService discoveryService = new ZKDiscoveryService(zkClientService);

    WeaveSpecification weaveSpec = loadWeaveSpec(weaveSpecFile);
    WeaveRunnableSpecification runnableSpec = weaveSpec.getRunnables().get(runnableName).getRunnableSpecification();
    ContainerInfo containerInfo = new ContainerInfo();
    WeaveContext context = new BasicWeaveContext(containerInfo.getHost(), args,
                                                  decodeArgs(System.getenv(EnvKeys.WEAVE_APPLICATION_ARGS)),
                                                  runnableSpec, instanceId, discoveryService);

    Service service = new WeaveContainerService(context, containerInfo,
                                                getContainerZKClient(zkClientService, appRunId, runnableName),
                                                runId, runnableSpec, ClassLoader.getSystemClassLoader());
    service.addListener(createServiceListener(zkClientService), Threads.SAME_THREAD_EXECUTOR);

    CountDownLatch stopLatch = new CountDownLatch(1);
    zkClientService.addListener(createZKClientServiceListener(stopLatch), Threads.SAME_THREAD_EXECUTOR);

    new WeaveContainerMain().doMain(wrapService(zkClientService, service));
    stopLatch.await();
  }

  private static ZKClient getContainerZKClient(ZKClient zkClient, RunId appRunId, String runnableName) {
    return ZKClients.namespace(zkClient, String.format("/%s/runnables/%s", appRunId, runnableName));
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

  private static Service wrapService(final ZKClientService zkClientService,
                                     final Service containerService) {
    return new Service() {

      @Override
      public ListenableFuture<State> start() {
        return Futures.transform(Services.chainStart(zkClientService, containerService),
                                 new AsyncFunction<List<ListenableFuture<State>>, State>() {
          @Override
          public ListenableFuture<State> apply(List<ListenableFuture<State>> input) throws Exception {
            return input.get(1);
          }
        });
      }

      @Override
      public State startAndWait() {
        return Futures.getUnchecked(start());
      }

      @Override
      public boolean isRunning() {
        return containerService.isRunning();
      }

      @Override
      public State state() {
        return containerService.state();
      }

      @Override
      public ListenableFuture<State> stop() {
        return Futures.transform(Services.chainStop(containerService, zkClientService),
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
        containerService.addListener(listener, executor);
      }
    };
  }

  private static Service.Listener createServiceListener(final ZKClientService zkClientService) {
    return new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        zkClientService.stop();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        zkClientService.stop();
      }
    };
  }

  private static Service.Listener createZKClientServiceListener(final CountDownLatch stopLatch) {
    return new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        stopLatch.countDown();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        stopLatch.countDown();
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
    return new File("logback-template.xml");
  }
}
