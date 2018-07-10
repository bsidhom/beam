/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.fn.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.sdk.resource.CloseableResource;
import org.apache.beam.sdk.resource.Closer;
import org.apache.beam.sdk.resource.Loader;

/**
 * A manually reference-counted cache.
 */
public class RefCountedCache<K, I, V> {

  private final Object lock = new Object();
  @GuardedBy("lock")
  private final Map<K, Payload<V>> cache;
  private final Loader<I, V> loader;
  private final Closer<V> closer;
  private final Function<I, K> keyExtractor;

  private RefCountedCache(Loader<I, V> loader, Closer<V> closer, Function<I, K> keyExtractor) {
    this.cache = new HashMap<>();
    this.loader = loader;
    this.closer = closer;
    this.keyExtractor = keyExtractor;
  }

  public CloseableResource<V> get(I input) {
    synchronized (lock) {
      K key = keyExtractor.apply(input);
      Payload<V> result = cache.get(key);
      if (result == null) {
        //result = new Payload<>();
        cache.put(key, result);
      }
      return null;
    }
  }

  private static class Payload<T> {

    private final T item;

    private int refCount = 1;

    Payload(T item) {
      this.item = item;
    }

    void close() {
    }

  }

}
