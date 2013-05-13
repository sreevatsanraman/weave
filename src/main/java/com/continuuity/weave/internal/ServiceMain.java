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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.continuuity.weave.internal.utils.Threads;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;

/**
 * Class for main method that starts a service.
 */
public abstract class ServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceMain.class);

  protected final void doMain(final Service service) throws ExecutionException, InterruptedException {
    configureLogger();

    final String serviceName = service.getClass().getName();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Shutdown hook triggered. Shutting down service " + serviceName);
        service.stopAndWait();
        LOG.info("Service shutdown " + serviceName);
      }
    });

    // Listener for state changes of the service
    final SettableFuture<Service.State> completion = SettableFuture.create();
    service.addListener(new Service.Listener() {
      @Override
      public void starting() {
        LOG.info("Starting service " + serviceName);
      }

      @Override
      public void running() {
        LOG.info("Service running " + serviceName);
      }

      @Override
      public void stopping(Service.State from) {
        LOG.info("Stopping service " + serviceName + " from " + from);
      }

      @Override
      public void terminated(Service.State from) {
        LOG.info("Service terminated " + serviceName + " from " + from);
        completion.set(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.info("Service failure " + serviceName, failure);
        completion.setException(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Starts the service
    service.start();

    try {
      // If container failed with exception, the future.get() will throws exception
      completion.get();
    } finally {
//      ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
//      if (loggerFactory instanceof LoggerContext) {
//        ((LoggerContext) loggerFactory).stop();
//      }
    }
  }

  protected abstract String getHostname();

  protected abstract String getKafkaZKConnect();

  protected abstract File getLogBackTemplate();

  private void configureLogger() {
    // Check if SLF4J is bound to logback in the current environment
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      return;
    }

    LoggerContext context = (LoggerContext) loggerFactory;

    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    context.reset();
    doConfigure(configurator);
  }

  private void doConfigure(JoranConfigurator configurator) {
    try {
      Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(getLogBackTemplate());

      NodeList appenders = document.getElementsByTagName("appender");
      for (int i = 0; i < appenders.getLength(); i++) {
        Node node = appenders.item(i);
        if ("KAFKA".equals(node.getAttributes().getNamedItem("name").getNodeValue())) {
          Element hostname = document.createElement("hostname");
          hostname.appendChild(document.createTextNode(getHostname()));
          node.appendChild(hostname);

          Element zookeeper = document.createElement("zookeeper");
          zookeeper.appendChild(document.createTextNode(getKafkaZKConnect()));
          node.appendChild(zookeeper);
        }
      }

      StringWriter result = new StringWriter();
      try {
        TransformerFactory.newInstance().newTransformer().transform(new DOMSource(document), new StreamResult(result));
      } finally {
        result.close();
      }
      configurator.doConfigure(new InputSource(new StringReader(result.toString())));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
