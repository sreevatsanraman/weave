package com.continuuity.discovery;

import com.continuuity.zookeeper.Cancellable;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractIdleService;

import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple in memory implementation of {@link DiscoveryService} and {@link DiscoveryServiceClient}.
 */
public class InMemoryDiscoveryService implements DiscoveryService, DiscoveryServiceClient {

  private final Multimap<String, Discoverable> services = HashMultimap.create();
  private final Lock lock = new ReentrantLock();

  @Override
  public Cancellable register(final Discoverable discoverable) {
    lock.lock();
    try {
      final Discoverable wrapper = new DiscoverableWrapper(discoverable);
      services.put(wrapper.getName(), wrapper);
      return new Cancellable() {
        @Override
        public void cancel() {
          lock.lock();
          try {
            services.remove(wrapper.getName(), wrapper);
          } finally {
            lock.unlock();
          }
        }
      };
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Iterable<Discoverable> discover(final String name) {
    return new Iterable<Discoverable>() {
      @Override
      public Iterator<Discoverable> iterator() {
        lock.lock();
        try {
          return ImmutableList.copyOf(services.get(name)).iterator();
        } finally {
          lock.unlock();
        }
      }
    };
  }
}
