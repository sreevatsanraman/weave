package com.continuuity.weave;

import com.continuuity.weave.api.ListenerAdapter;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.internal.utils.Threads;
import com.continuuity.weave.internal.yarn.YarnWeaveRunnerService;
import com.continuuity.weave.zk.InMemoryZKServer;
import com.continuuity.discovery.Discoverable;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.LineReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class EchoServerTest {

  private static final Logger LOG = LoggerFactory.getLogger(EchoServerTest.class);

  @Test
  public void testEchoServer() throws InterruptedException, ExecutionException, IOException {
    WeaveController controller = runnerService.prepare(new EchoServer(),
                                                     ResourceSpecification.Builder.with()
                                                       .setCores(1)
                                                       .setMemory(1, ResourceSpecification.SizeUnit.GIGA)
                                                       .setInstances(2)
                                                       .build())
                                            .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
                                            .start();

    final CountDownLatch running = new CountDownLatch(1);
    controller.addListener(new ListenerAdapter() {
      @Override
      public void running() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(30, TimeUnit.SECONDS));

    Iterable<Discoverable> echoServices = controller.discoverService("echo");
    int trial = 0;
    while (Iterables.size(echoServices) != 2 && trial < 60) {
      TimeUnit.SECONDS.sleep(1);
      trial++;
    }
    Assert.assertTrue(trial < 60);

    for (Discoverable discoverable : echoServices) {
      String msg = "Hello: " + discoverable.getSocketAddress();

      Socket socket = new Socket(discoverable.getSocketAddress().getAddress(),
                                 discoverable.getSocketAddress().getPort());
      try {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
        LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));

        writer.println(msg);
        Assert.assertEquals(msg, reader.readLine());
      } finally {
        socket.close();
      }
    }

    TimeUnit.SECONDS.sleep(2);

    controller.stop().get();
  }

  @Before
  public void init() {
    // Starts Zookeeper
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    // Start YARN mini cluster
    YarnConfiguration config = new YarnConfiguration(new Configuration());

    // TODO: Hack
    config.set("yarn.resourcemanager.scheduler.class", "org.apache.hadoop.yarn.server.resourcemanager.scheduler" +
      ".fifo.FifoScheduler");
    config.set("yarn.minicluster.fixed.ports", "true");

    cluster = new MiniYARNCluster("test-cluster", 1, 1, 1);
    cluster.init(config);
    cluster.start();

    runnerService = new YarnWeaveRunnerService(new YarnConfiguration(), zkServer.getConnectionStr() + "/weave");
    runnerService.startAndWait();
  }

  @After
  public void finish() {
    runnerService.stopAndWait();
    cluster.stop();
    zkServer.stopAndWait();
  }

  private InMemoryZKServer zkServer;
  private MiniYARNCluster cluster;
  private WeaveRunnerService runnerService;
}
