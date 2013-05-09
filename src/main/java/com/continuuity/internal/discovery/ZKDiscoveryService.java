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
package com.continuuity.internal.discovery;

import com.continuuity.zookeeper.Cancellable;
import com.continuuity.zookeeper.Discoverable;
import com.continuuity.zookeeper.DiscoveryService;
import com.continuuity.zookeeper.DiscoveryServiceClient;
import com.continuuity.zookeeper.NodeChildren;
import com.continuuity.zookeeper.NodeData;
import com.continuuity.zookeeper.OperationFuture;
import com.continuuity.zookeeper.RetryStrategies;
import com.continuuity.zookeeper.ZKClientService;
import com.continuuity.zookeeper.ZKClientServices;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Zookeeper implementation of {@link DiscoveryService} and {@link DiscoveryServiceClient}.
 * <p>
 *   Discoverable services are registered within Zookeeper under the namespace 'discoverable' by default.
 *   If you would like to change the namespace under which the services are registered then you can pass
 *   in the namespace during construction of {@link ZKDiscoveryService}.
 * </p>
 *
 * <p>
 *   Following is a simple example of how {@link ZKDiscoveryService} can be used for registering services
 *   and also for discovering the registered services.
 *   <blockquote>
 *    <pre>
 *      {@code
 *
 *      DiscoveryService service = new ZKDiscoveryService(zkQuorum);
 *      service.startAndWait();
 *      service.register(new Discoverable() {
 *        @Override
 *        public String getName() {
 *          return 'service-name';
 *        }
 *
 *        @Override
 *        public InetSocketAddress getSocketAddress() {
 *          return new InetSocketAddress(hostname, port);
 *        }
 *      });
 *      ...
 *      ...
 *      Iterable<Discoverable> services = service.discovery("service-name");
 *      ...
 *      }
 *    </pre>
 *   </blockquote>
 * </p>
 */
public class ZKDiscoveryService extends AbstractService implements DiscoveryService, DiscoveryServiceClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZKDiscoveryService.class);
  private static final String NAMESPACE = "/discoverable";
  private static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();

  private final AtomicReference<Multimap<String, Discoverable>> services;
  private final ConcurrentMap<String, Boolean> serviceWatched;
  private final ZKClientService client;

  /**
   * Constructs ZKDiscoveryService using the provided zookeeper quorum for storing service registry.
   * @param zkConnectionString of zookeeper quorum
   */
  public ZKDiscoveryService(String zkConnectionString) {
    this(zkConnectionString, NAMESPACE);
  }

  /**
   * Constructs ZKDiscoveryService using the provided zookeeper quorum for storing service registry under namepsace.
   * @param zkConnectionString of zookeeper quorum
   * @param namespace under which the service registered would be stored in zookeeper.
   */
  public ZKDiscoveryService(String zkConnectionString, String namespace) {
    client = ZKClientServices.reWatchOnExpire(
      ZKClientServices.retryOnFailure(ZKClientService.Builder.of(zkConnectionString + namespace).build(),
                                      RetryStrategies.fixDelay(2, TimeUnit.SECONDS)));
    services = new AtomicReference<Multimap<String, Discoverable>>();
    serviceWatched = Maps.newConcurrentMap();
  }

  @Override
  protected void doStart() {
    services.set(HashMultimap.<String, Discoverable>create());
    Futures.addCallback(client.start(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        notifyStarted();
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    });
  }

  @Override
  protected void doStop() {
    Futures.addCallback(client.stop(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        notifyStopped();
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    });
  }

  /**
   * Registers a {@link Discoverable} in zookeeper.
   * <p>
   *   Registering a {@link Discoverable} will create a node <base>/<service-name>
   *   in zookeeper as a ephemeral node. If the node already exists (timeout associated with emphemeral, then a runtime
   *   exception is thrown to make sure that a service with an intent to register is not started without registering.
   *   When a runtime is thrown, expectation is that the process being started with fail and would be started again
   *   by the monitoring service.
   * </p>
   * @param discoverable Information of the service provider that could be discovered.
   * @return An instance of {@link Cancellable}
   */
  @Override
  public Cancellable register(final Discoverable discoverable) {
    Preconditions.checkState(isRunning(), "Service is not running");

    final Discoverable wrapper = new DiscoverableWrapper(discoverable);
    byte[] discoverableBytes = encode(wrapper);

    // Path /<service-name>/service-sequential
    final String sb = "/" + wrapper.getName() + "/service-";
    final String path = Futures.getUnchecked(client.create(sb, discoverableBytes,
                                                           CreateMode.EPHEMERAL_SEQUENTIAL, true));
    return new Cancellable() {
      @Override
      public void cancel() {
        Futures.getUnchecked(client.delete(path));
      }
    };
  }

  /**
   * Gets the list of {@link Discoverable} for a given service.
   * <p>
   *   Once, the list is retrieved a watcher is set on the service to track
   *   any changes that would require us to reload the service information
   *   information from zookeeper.
   * </p>
   * @param service for which we are requested to retrieve the list of {@link Discoverable}
   */
  private void getChildren(final String service) {
    final String sb = "/" + service;

    if(!client.isRunning()) {
      return;
    }

    Futures.addCallback(client.exists(sb, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeCreated) {
          getChildren(service);
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result == null) {
          // Node not yet exists, the Watcher will get triggered when the node is created.
          return;
        }
        Futures.addCallback(client.getChildren(sb, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeChildrenChanged) {
              getChildren(service);
            }
          }
        }), new FutureCallback<NodeChildren>() {
          @Override
          public void onSuccess(NodeChildren result) {
            updateService(result, service);
          }

          @Override
          public void onFailure(Throwable t) {
            LOG.error("Failed to fetch children node for service " + service + ": " + t, t);
          }
        });
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to access discovery service node " + service + ": " + t, t);
      }
    });
  }

  private void updateService(NodeChildren children, final String service) {
    final String sb = "/" + service;
    final Multimap<String, Discoverable> newServices = HashMultimap.create(services.get());
    newServices.removeAll(service);

    // Fetch data of all children nodes in parallel.
    List<OperationFuture<NodeData>> dataFutures = Lists.newArrayListWithCapacity(children.getChildren().size());
    for(String child : children.getChildren()) {
      String path = sb + "/" + child;
      dataFutures.add(client.getData(path));
    }

    // Update the service map when all fetching are done.
    final ListenableFuture<List<NodeData>> fetchFuture = Futures.successfulAsList(dataFutures);
    fetchFuture.addListener(new Runnable() {
      @Override
      public void run() {
        for (NodeData nodeData : Futures.getUnchecked(fetchFuture)) {
          // For successful fetch, decode the content.
          if (nodeData != null) {
            Discoverable discoverable = decode(nodeData.getData());
            if (discoverable != null) {
              newServices.put(service, discoverable);
            }
          }
        }
        // Replace the local service register with changes.
        services.set(newServices);
      }
    }, SAME_THREAD_EXECUTOR);
  }

  /**
   * Discovers a <code>service</code> available
   *
   * @param service name of the service to be discovered.
   * @return Live {@link Iterable} of {@link Discoverable} <code>service</code>
   */
  @Override
  public Iterable<Discoverable> discover(final String service) {
    Preconditions.checkState(isRunning(), "Service is not running");
    if (serviceWatched.putIfAbsent(service, true) == null) {
      getChildren(service);
    }
    return new Iterable<Discoverable>() {
      @Override
      public Iterator<Discoverable> iterator() {
        Preconditions.checkState(isRunning(), "Service is not running");
        return ImmutableList.copyOf(services.get().get(service)).iterator();
      }
    };
  }

  /**
   * Static helper function for decoding array of bytes into a {@link DiscoverableWrapper} object.
   * @param bytes representing serialized {@link DiscoverableWrapper}
   * @return null if bytes are null; else an instance of {@link DiscoverableWrapper}
   */
  private static Discoverable decode(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    String content = new String(bytes, Charsets.UTF_8);
    return new GsonBuilder().registerTypeAdapter(Discoverable.class, new DiscoverableCodec())
      .create()
      .fromJson(content, Discoverable.class);
  }

  /**
   * Static helper function for encoding an instance of {@link DiscoverableWrapper} into array of bytes.
   * @param discoverable An instance of {@link DiscoverableWrapper}
   * @return array of bytes representing an instance of <code>discoverable</code>
   */
  private static byte[] encode(Discoverable discoverable) {
    return new GsonBuilder().registerTypeAdapter(DiscoverableWrapper.class, new DiscoverableCodec())
      .create()
      .toJson(discoverable, DiscoverableWrapper.class)
      .getBytes(Charsets.UTF_8);
  }

  /**
   * SerDe for converting a {@link DiscoverableWrapper} into a JSON object
   * or from a JSON object into {@link DiscoverableWrapper}.
   */
  private static final class DiscoverableCodec
    implements JsonSerializer<Discoverable>, JsonDeserializer<Discoverable> {

    @Override
    public Discoverable deserialize(JsonElement json, Type typeOfT,
                                    JsonDeserializationContext context) throws JsonParseException {
      JsonObject jsonObj = json.getAsJsonObject();
      final String service = jsonObj.get("service").getAsString();
      String hostname = jsonObj.get("hostname").getAsString();
      int port = jsonObj.get("port").getAsInt();
      final InetSocketAddress address = new InetSocketAddress(hostname, port);
      return new Discoverable() {
        @Override
        public String getName() {
          return service;
        }

        @Override
        public InetSocketAddress getSocketAddress() {
          return address;
        }
      };
    }

    @Override
    public JsonElement serialize(Discoverable src, Type typeOfSrc, JsonSerializationContext context) {
      JsonObject jsonObj = new JsonObject();
      jsonObj.addProperty("service", src.getName());
      jsonObj.addProperty("hostname", src.getSocketAddress().getHostName());
      jsonObj.addProperty("port", src.getSocketAddress().getPort());
      return jsonObj;
    }
  }
}

