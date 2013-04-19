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

import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.internal.api.RunIds;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.continuuity.weave.internal.state.Message;
import com.continuuity.weave.internal.state.MessageCallback;
import com.continuuity.weave.internal.state.ZKServiceDecorator;
import com.continuuity.zookeeper.ZKClients;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.AMRMClient;
import org.apache.hadoop.yarn.client.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class ApplicationMasterService implements Service {

  private static final int ZK_TIMEOUT = 10000;    // 10 seconds
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterService.class);

  private final RunId runId;
  private final String zkConnectStr;
  private final WeaveSpecification weaveSpec;
  private final File weaveSpecFile;
  private final YarnConfiguration yarnConf;
  private final String masterContainerId;
  private final AMRMClient amrmClient;
  private final Queue<WeaveContainerLauncher> launchers;
  private final ZKServiceDecorator serviceDelegate;
  private YarnRPC yarnRPC;
  private Resource maxCapability;
  private Resource minCapability;


  public ApplicationMasterService(RunId runId, String zkConnectStr, File weaveSpecFile) throws IOException {
    this.runId = runId;
    this.zkConnectStr = zkConnectStr;
    this.weaveSpecFile = weaveSpecFile;
    this.weaveSpec = WeaveSpecificationAdapter.create().fromJson(weaveSpecFile);

    this.yarnConf = new YarnConfiguration();
    this.launchers = new ConcurrentLinkedQueue<WeaveContainerLauncher>();

    this.serviceDelegate = new ZKServiceDecorator(zkConnectStr, ZK_TIMEOUT, runId,
                                                  createLiveNodeDataSupplier(), new ServiceDelegate());

    // Get the container ID and convert it to ApplicationAttemptId
    masterContainerId = System.getenv().get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    Preconditions.checkArgument(masterContainerId != null,
                                "Missing %s from environment", ApplicationConstants.AM_CONTAINER_ID_ENV);
    amrmClient = new AMRMClientImpl(ConverterUtils.toContainerId(masterContainerId).getApplicationAttemptId());
  }

  private Supplier<? extends JsonElement> createLiveNodeDataSupplier() {
    return new Supplier<JsonElement>() {
      @Override
      public JsonElement get() {
        JsonObject jsonObj = new JsonObject();
        jsonObj.addProperty("containerId", masterContainerId);
        return jsonObj;
      }
    };
  }

  private void doStart() throws Exception {
    LOG.info("Start application master with spec: " + WeaveSpecificationAdapter.create().toJson(weaveSpec));

    yarnRPC = YarnRPC.create(yarnConf);

    amrmClient.init(yarnConf);
    amrmClient.start();
    // TODO: Have RPC host and port
    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster("", 0, null);
    maxCapability = response.getMaximumResourceCapability();
    minCapability = response.getMinimumResourceCapability();

    LOG.info("Maximum resource capability: " + maxCapability);
    LOG.info("Minimum resource capability: " + minCapability);

    serviceDelegate.getZKClient().create("/" + runId + "/runnables", null, CreateMode.PERSISTENT).get();
  }

  private void doStop() throws Exception {
    // TODO: Order the stop sequence in reverse of the start sequence
    for (WeaveContainerLauncher launcher : launchers) {
      launcher.stopAndWait();
    }

    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
    amrmClient.stop();
  }

  private void doRun() throws Exception {

    // Orderly stores container requests.
    Queue<RunnableContainerRequest> runnableRequests = Lists.newLinkedList();
    for (WeaveSpecification.Order order : weaveSpec.getOrders()) {

      ImmutableMultimap.Builder<Resource, RuntimeSpecification> builder = ImmutableMultimap.builder();
      for (String runnableName : order.getNames()) {
        RuntimeSpecification runtimeSpec = weaveSpec.getRunnables().get(runnableName);
        Resource capability = createCapability(runtimeSpec.getResourceSpecification());
        builder.put(capability, runtimeSpec);
      }
      runnableRequests.add(new RunnableContainerRequest(order.getType(), builder.build()));
    }

    // The main loop
    Map.Entry<Resource, ? extends Collection<RuntimeSpecification>> currentRequest = null;
    Queue<ProvisionRequest> provisioning = Lists.newLinkedList();
    while (isRunning()) {
      // If nothing is in provisioning, and no pending request, move to next one
      while (provisioning.isEmpty() && currentRequest == null && !runnableRequests.isEmpty()) {
        currentRequest = runnableRequests.peek().takeRequest();
        if (currentRequest == null) {
          // All different types of resource request from current order is done, move to next one
          // TODO: Need to handle order type as well
          runnableRequests.poll();
        }
      }
      // Nothing in provision, makes the next batch of provision request
      if (provisioning.isEmpty() && currentRequest != null) {
        provisioning.addAll(addContainerRequests(currentRequest.getKey(), currentRequest.getValue()));
        currentRequest = null;
      }

      // Now call allocate
      AllocateResponse allocateResponse = amrmClient.allocate(0.0f);
      allocateResponse.getAMResponse().getResponseId();
      AMResponse amResponse = allocateResponse.getAMResponse();

      // Assign runnable to container
      launchRunnable(amResponse.getAllocatedContainers(), provisioning);
      TimeUnit.SECONDS.sleep(1);
    }
  }

  /**
   * Adds {@link AMRMClient.ContainerRequest}s with the given resource capability for each runtime.
   * @param capability
   * @param runtimeSpecs
   */
  private Queue<ProvisionRequest> addContainerRequests(Resource capability,
                                                       Collection<RuntimeSpecification> runtimeSpecs) {
    Queue<ProvisionRequest> requests = Lists.newLinkedList();
    for (RuntimeSpecification runtimeSpec : runtimeSpecs) {
      // TODO: Allow user to set priority?
      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

      int containerCount = runtimeSpec.getResourceSpecification().getInstances();
      AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null,
                                                                            priority, containerCount);
      LOG.info("Request for container: " + request);
      requests.add(new ProvisionRequest(request, runtimeSpec));
      amrmClient.addContainerRequest(request);
    }
    return requests;
  }

  /**
   * Launches runnables in the provisioned containers.
   * @param containers
   * @param provisioning
   */
  private void launchRunnable(List<Container> containers, Queue<ProvisionRequest> provisioning) {
    for (Container container : containers) {
      ProvisionRequest provisionRequest = provisioning.poll();
      if (provisionRequest == null) {
        LOG.info("Nothing to run in container, releasing it: " + container);
        amrmClient.releaseAssignedContainer(container.getId());
        continue;
      }

      String runnableName = provisionRequest.getRuntimeSpec().getName();
      LOG.info("Starting runnable " + runnableName + " in container " + container);
      DefaultProcessLauncher processLauncher = new DefaultProcessLauncher(container, yarnRPC, yarnConf);
      WeaveContainerLauncher launcher = new WeaveContainerLauncher(weaveSpec, weaveSpecFile, runnableName,
                                                                   RunIds.generate(), processLauncher,
                                                                   ZKClients.namespace(serviceDelegate.getZKClient(),
                                                                                       getZKNamespace(runnableName)),
                                                                   getRunnableZKConnectStr(runnableName));
      launcher.start();
      launchers.add(launcher);
      // Needs to remove it from AMRMClient, otherwise later container requests will get accumulated with completed one
      // if it has the same priority.
      // TODO: Need to verify if the mentioned behavior is bug in AMRMClient or is intended usage.
      amrmClient.removeContainerRequest(provisionRequest.getRequest());
    }
  }

  private String getRunnableZKConnectStr(String runnableName) {
    return String.format("%s%s", zkConnectStr, getZKNamespace(runnableName));
  }

  private String getZKNamespace(String runnableName) {
    return String.format("/%s/runnables/%s", runId, runnableName);
  }

  private ListenableFuture<String> processMessage(String messageId, Message message) {
    SettableFuture<String> result = SettableFuture.create();
    // TODO:
    return result;
  }

  private Resource createCapability(ResourceSpecification resourceSpec) {
    Resource capability = Records.newRecord(Resource.class);

    int cores = Math.max(Math.min(resourceSpec.getCores(), maxCapability.getVirtualCores()),
                         minCapability.getVirtualCores());
    capability.setVirtualCores(cores);

    int memory = Math.max(Math.min(resourceSpec.getMemorySize(), maxCapability.getMemory()),
                          minCapability.getMemory());
    capability.setMemory(memory);

    return capability;
  }

  @Override
  public ListenableFuture<State> start() {
    return serviceDelegate.start();
  }

  @Override
  public State startAndWait() {
    return Futures.getUnchecked(start());
  }

  @Override
  public boolean isRunning() {
    return serviceDelegate.isRunning();
  }

  @Override
  public State state() {
    return serviceDelegate.state();
  }

  @Override
  public ListenableFuture<State> stop() {
    return serviceDelegate.stop();
  }

  @Override
  public State stopAndWait() {
    return Futures.getUnchecked(stop());
  }

  @Override
  public void addListener(Listener listener, Executor executor) {
    serviceDelegate.addListener(listener, executor);
  }

  /**
   * A private class for service lifecycle. It's done this way so that we can have {@link ZKServiceDecorator} to
   * wrap around this to reflect status in ZK.
   */
  private final class ServiceDelegate extends AbstractExecutionThreadService implements MessageCallback {

    private volatile Thread runThread;

    @Override
    protected void run() throws Exception {
      runThread = Thread.currentThread();
      try {
        doRun();
      } catch (InterruptedException e) {
        // It's ok to get interrupted exception, as it's a signal to stop
        Thread.currentThread().interrupt();
      }
    }

    @Override
    protected void startUp() throws Exception {
      doStart();
    }

    @Override
    protected void shutDown() throws Exception {
      doStop();
    }

    @Override
    protected void triggerShutdown() {
      Thread runThread = this.runThread;
      if (runThread != null) {
        runThread.interrupt();
      }
    }

    @Override
    public ListenableFuture<String> onReceived(String messageId, Message message) {
      return processMessage(messageId, message);
    }
  }

  private static final class RunnableContainerRequest {
    private final WeaveSpecification.Order.Type orderType;
    private final Iterator<Map.Entry<Resource, Collection<RuntimeSpecification>>> requests;

    private RunnableContainerRequest(WeaveSpecification.Order.Type orderType,
                                     Multimap<Resource, RuntimeSpecification> requests) {
      this.orderType = orderType;
      this.requests = requests.asMap().entrySet().iterator();
    }

    private WeaveSpecification.Order.Type getOrderType() {
      return orderType;
    }

    /**
     * Remove a resource request and returns it.
     * @return The {@link Resource} and {@link Collection} of {@link RuntimeSpecification} or
     *         {@code null} if there is no more request.
     */
    private Map.Entry<Resource, ? extends Collection<RuntimeSpecification>> takeRequest() {
      Map.Entry<Resource, Collection<RuntimeSpecification>> next = Iterators.getNext(requests, null);
      return next == null ? null : Maps.immutableEntry(next.getKey(), ImmutableList.copyOf(next.getValue()));
    }
  }

  private static final class ProvisionRequest {
    private final AMRMClient.ContainerRequest request;
    private final RuntimeSpecification runtimeSpec;

    private ProvisionRequest(AMRMClient.ContainerRequest request, RuntimeSpecification runtimeSpec) {
      this.request = request;
      this.runtimeSpec = runtimeSpec;
    }

    private AMRMClient.ContainerRequest getRequest() {
      return request;
    }

    private RuntimeSpecification getRuntimeSpec() {
      return runtimeSpec;
    }
  }
}
