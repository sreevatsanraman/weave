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

import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.zookeeper.RetryStrategies;
import com.continuuity.zookeeper.ZKClientService;
import com.continuuity.zookeeper.ZKClientServices;
import com.google.common.util.concurrent.Futures;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
final class ZKWeaveController extends AbstractServiceController implements WeaveController {

  private final ZKClientService zkClient;
  private final Queue<LogHandler> logHandlers;

  ZKWeaveController(String zkConnect, int zkTimeout, RunId runId) {
    super(runId);
    // Creates a retry on failure zk client
    this.zkClient = ZKClientServices.reWatchOnExpire(
      ZKClientServices.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                        .setSessionTimeout(zkTimeout)
                                        .build(),
                                      RetryStrategies.exponentialDelay(100, 2000, TimeUnit.MILLISECONDS)));
    logHandlers = new ConcurrentLinkedQueue<LogHandler>();
  }

  void start() {
    Futures.getUnchecked(zkClient.start());
    super.doStart(zkClient);
  }

  @Override
  public void addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
  }
}
