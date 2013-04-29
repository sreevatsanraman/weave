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

import com.continuuity.weave.api.LocalFile;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.RuntimeSpecification;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.internal.api.RunIds;
import com.continuuity.weave.internal.json.WeaveSpecificationAdapter;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
final class YarnWeavePreparer implements WeavePreparer {

  private final WeaveSpecification weaveSpec;
  private final YarnClient yarnClient;
  private final String zkConnectStr;

  private final List<Closeable> resourceCleaner = Lists.newArrayList();
  private final List<LogHandler> logHandlers = Lists.newArrayList();
  private final List<String> arguments = Lists.newArrayList();
  private final ListMultimap<String, String> runnableArgs = ArrayListMultimap.create();


  YarnWeavePreparer(WeaveSpecification weaveSpec, YarnClient yarnClient, String zkConnectStr) {
    this.weaveSpec = weaveSpec;
    this.yarnClient = yarnClient;
    this.zkConnectStr = zkConnectStr;
  }

  @Override
  public WeavePreparer addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
    return this;
  }

  @Override
  public WeavePreparer withApplicationArguments(String... args) {
    return withApplicationArguments(Arrays.asList(args));
  }

  @Override
  public WeavePreparer withApplicationArguments(Iterable<String> args) {
    Iterables.addAll(arguments, args);
    return this;
  }

  @Override
  public WeavePreparer withArguments(String runnableName, String... args) {
    return withArguments(runnableName, Arrays.asList(args));
  }

  @Override
  public WeavePreparer withArguments(String runnableName, Iterable<String> args) {
    runnableArgs.putAll(runnableName, args);
    return this;
  }

  @Override
  public WeaveController start() {
    // TODO: Unify this with {@link ProcessLauncher}
    try {
      GetNewApplicationResponse response = yarnClient.getNewApplication();

      ApplicationSubmissionContext appSubmissionContext = Records.newRecord(ApplicationSubmissionContext.class);
      appSubmissionContext.setApplicationId(response.getApplicationId());
      appSubmissionContext.setApplicationName(weaveSpec.getName());

      Map<String, LocalResource> localResources = Maps.newHashMap();

      resourceCleaner.add(saveWeaveSpec(weaveSpec, localResources));
      resourceCleaner.add(populateRunnableResources(weaveSpec, localResources));

      ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
      containerLaunchContext.setLocalResources(localResources);

      RunId runId = RunIds.generate();
      containerLaunchContext.setCommands(ImmutableList.of(
        "java",
        ApplicationMasterMain.class.getName()));

      // TODO: Should figure out jar dependencies
      containerLaunchContext.setEnvironment(ImmutableMap.<String, String>builder()
        .put("CLASSPATH", System.getProperty("java.class.path"))
        .put(EnvKeys.WEAVE_CONTAINER_ZK, zkConnectStr)
        .put(EnvKeys.WEAVE_SPEC_PATH, "weaveSpec.json")
        .put(EnvKeys.WEAVE_APPLICATION_ARGS, encodeArguments(arguments))
        .put(EnvKeys.WEAVE_RUNNABLE_ARGS, encodeRunnableArguments(runnableArgs))
        .put(EnvKeys.WEAVE_RUN_ID, runId.getId())
        .build()
      );
      Resource capability = Records.newRecord(Resource.class);
      capability.setMemory(256);
      containerLaunchContext.setResource(capability);

      appSubmissionContext.setAMContainerSpec(containerLaunchContext);

      yarnClient.submitApplication(appSubmissionContext);

      return createController(runId, logHandlers);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private WeaveController createController(RunId runId, Iterable<LogHandler> logHandlers) {
    ZKWeaveController controller = new ZKWeaveController(zkConnectStr, 10000, runId, logHandlers);
    controller.start();
    return controller;
  }

  private String encodeArguments(List<String> args) {
    return new Gson().toJson(args);
  }

  private String encodeRunnableArguments(Multimap<String, String> args) {
    return new Gson().toJson(args.asMap());
  }

  private Closeable saveWeaveSpec(WeaveSpecification spec,
                                  Map<String, LocalResource> localResources) throws IOException {
    // Serialize into a local temp file.
    final File file = File.createTempFile("weaveSpec", ".json");
    WeaveSpecificationAdapter.create().toJson(spec, file);

    LocalResource resource = Records.newRecord(LocalResource.class);

    // Creates Yarn LocalResource
    resource.setResource(ConverterUtils.getYarnUrlFromURI(file.toURI()));
    resource.setSize(file.length());
    resource.setTimestamp(file.lastModified());
    resource.setType(LocalResourceType.FILE);
    resource.setVisibility(LocalResourceVisibility.APPLICATION);

    localResources.put("weaveSpec.json", resource);

    // Delete the file when the closeable is invoked.
    return new Closeable() {
      @Override
      public void close() throws IOException {
        file.delete();
      }
    };
  }

  /**
   * Based on the given {@link WeaveSpecification}, setup the local resource map.
   * @param weaveSpec
   * @param localResources
   * @return
   * @throws IOException
   */
  private Closeable populateRunnableResources(WeaveSpecification weaveSpec,
                                              Map<String, LocalResource> localResources) throws IOException {
    final List<File> tmpFiles = Lists.newArrayList();

    for (Map.Entry<String, RuntimeSpecification> entry: weaveSpec.getRunnables().entrySet()) {
      String name = entry.getKey();
      for (LocalFile localFile : entry.getValue().getLocalFiles()) {
        LocalResource localResource = Records.newRecord(LocalResource.class);
        localResource.setVisibility(LocalResourceVisibility.APPLICATION);
        localResource.setType(LocalResourceType.FILE);

        File tmpFile = copyFromURI(localFile.getURI(), File.createTempFile(localFile.getName(), ".tmp"));
        tmpFiles.add(tmpFile);

        localResource.setResource(ConverterUtils.getYarnUrlFromURI(tmpFile.toURI()));
        localResource.setSize(tmpFile.length());
        localResource.setTimestamp(tmpFile.lastModified());

        localResources.put(name + "." + localFile.getName(), localResource);
      }
    }

    return new Closeable() {
      @Override
      public void close() throws IOException {
        for (File file : tmpFiles) {
          file.delete();
        }
      }
    };
  }

  private File copyFromURI(URI uri, File target) throws IOException {
    InputStream is = uri.toURL().openStream();
    try {
      ByteStreams.copy(is, Files.newOutputStreamSupplier(target));
      if ("file".equals(uri.getScheme())) {
        target.setLastModified(new File(uri).lastModified());
      }
      return target;
    } finally {
      is.close();
    }
  }
}
