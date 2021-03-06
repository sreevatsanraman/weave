package com.continuuity.weave.yarn;

import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.internal.zookeeper.InMemoryZKServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.PrintWriter;

/**
 *
 */
public class DistributeShellTest {

  private InMemoryZKServer zkServer;
  private MiniYARNCluster cluster;
  private WeaveRunnerService runnerService;


  @Ignore
  @Test
  public void testDistributedShell() throws InterruptedException {
    WeaveRunnerService weaveRunner = new YarnWeaveRunnerService(new YarnConfiguration(), zkServer.getConnectionStr());
    weaveRunner.startAndWait();

    WeaveController controller = weaveRunner.prepare(new DistributedShell("pwd", "ls -al"))
                                            .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
                                            .start();
//    controller.waitFor(30, TimeUnit.SECONDS);
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

    runnerService = new YarnWeaveRunnerService(new YarnConfiguration(), zkServer.getConnectionStr());
    runnerService.startAndWait();
  }

  @After
  public void finish() {
    runnerService.stopAndWait();
    cluster.stop();
    zkServer.stopAndWait();
  }
}
