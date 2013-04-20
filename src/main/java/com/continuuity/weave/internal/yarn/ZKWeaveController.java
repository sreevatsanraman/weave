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

import com.continuuity.internal.kafka.client.SimpleKafkaClient;
import com.continuuity.kafka.client.FetchedMessage;
import com.continuuity.kafka.client.KafkaClient;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.logging.LogEntry;
import com.continuuity.weave.api.logging.LogHandler;
import com.continuuity.weave.internal.logging.LogEntryDecoder;
import com.continuuity.weave.internal.utils.Services;
import com.continuuity.zookeeper.RetryStrategies;
import com.continuuity.zookeeper.ZKClientService;
import com.continuuity.zookeeper.ZKClientServices;
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
final class ZKWeaveController extends AbstractServiceController implements WeaveController {

  private static final Logger LOG = LoggerFactory.getLogger(ZKWeaveController.class);
  private static final String LOG_TOPIC = "log";

  private final ZKClientService zkClient;
  private final Queue<LogHandler> logHandlers;
  private final KafkaClient kafkaClient;
  private final Thread logPoller;

  ZKWeaveController(String zkConnect, int zkTimeout, RunId runId, Collection<LogHandler> logHandlers) {
    super(runId);
    // Creates a retry on failure zk client
    this.zkClient = ZKClientServices.reWatchOnExpire(
      ZKClientServices.retryOnFailure(ZKClientService.Builder.of(zkConnect)
                                        .setSessionTimeout(zkTimeout)
                                        .build(),
                                      RetryStrategies.exponentialDelay(100, 2000, TimeUnit.MILLISECONDS)));
    this.logHandlers = new ConcurrentLinkedQueue<LogHandler>(logHandlers);
    this.kafkaClient = new SimpleKafkaClient(String.format("%s/%s/kafka", zkConnect, runId));
    this.logPoller = createLogPoller();
  }

  void start() {
    Futures.getUnchecked(zkClient.start());
    Futures.addCallback(kafkaClient.start(), new FutureCallback<Service.State>() {
      @Override
      public void onSuccess(Service.State result) {
        logPoller.start();
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to start kafka client.", t);
      }
    });
    super.doStart(zkClient);
  }

  @Override
  public ListenableFuture<State> stop() {
    final SettableFuture<State> result = SettableFuture.create();
    final ListenableFuture<State> future = super.stop();
    future.addListener(new Runnable() {
      @Override
      public void run() {
        logPoller.interrupt();
        try {
          logPoller.join();
        } catch (InterruptedException e) {
          LOG.warn("Joining of log poller thread interrupted.", e);
        }
        final ListenableFuture<List<ListenableFuture<Service.State>>> stopFuture = Services.chainStop(kafkaClient,
                                                                                                      zkClient);
        stopFuture.addListener(new Runnable() {
          @Override
          public void run() {
            try {
              Futures.allAsList(stopFuture.get()).get();
              future.get();
              result.set(State.TERMINATED);
            } catch (Exception e) {
              LOG.error("Failed when stopping local services", e);
              result.setException(e);
            }
          }
        }, MoreExecutors.sameThreadExecutor());
      }
    }, MoreExecutors.sameThreadExecutor());
    return result;
  }

  @Override
  public void addLogHandler(LogHandler handler) {
    logHandlers.add(handler);
  }

  private Thread createLogPoller() {
    Thread poller = new Thread("weave-log-poller") {
      @Override
      public void run() {
        LOG.info("Weave log poller thread started.");
        Gson gson = new GsonBuilder().registerTypeAdapter(LogEntry.class, new LogEntryDecoder()).create();
        Iterator<FetchedMessage> messageIterator = kafkaClient.consume(LOG_TOPIC, 0, 0, 1048576);
        while (messageIterator.hasNext()) {
          String json = Charsets.UTF_8.decode(messageIterator.next().getBuffer()).toString();
          LogEntry entry = gson.fromJson(json, LogEntry.class);
          if (entry != null) {
            invokeHandlers(entry);
          }
        }
      }

      private void invokeHandlers(LogEntry entry) {
        for (LogHandler handler : logHandlers) {
          handler.onLog(entry);
        }
      }
    };
    poller.setDaemon(true);
    return poller;
  }
}
