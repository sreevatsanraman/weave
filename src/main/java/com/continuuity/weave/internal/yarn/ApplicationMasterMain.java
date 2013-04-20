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

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.internal.ServiceMain;
import com.continuuity.weave.internal.api.RunIds;
import com.google.common.base.Preconditions;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

/**
 * Main class for launching {@link ApplicationMasterService}.
 */
public final class ApplicationMasterMain extends ServiceMain {

  private final String kafkaZKConnect;

  private ApplicationMasterMain(String kafkaZKConnect) {
    this.kafkaZKConnect = kafkaZKConnect;
  }

  /**
   * Starts the application master.
   * @param args args[0] - ZooKeeper connection string.
   *             args[1] - local resource name for spec file.
   *             args[2] - RunId.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length >= 3, "Incorrect argument size.");
    String zkConnect = args[0];
    RunId runId = RunIds.fromString(args[2]);
    new ApplicationMasterMain(String.format("%s/%s/kafka", zkConnect, runId))
      .doMain(new ApplicationMasterService(runId, zkConnect, new File(args[1])));
  }

  @Override
  protected String getHostname() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      return "unknown";
    }
  }

  @Override
  protected String getKafkaZKConnect() {
    return kafkaZKConnect;
  }
}
