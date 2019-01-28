package org.gbif.pipelines.parsers.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Simple implementation of Memoizer/Cache.
 * It uses an internal cache to stored pre-computed values.
 *
 * @param <K> lookup key value
 * @param <V> type of stored elements
 */
public class Memoizer<K,V> {

    private final LoadingCache<K,V> cache;

    /**
     *  Creates a instance using a supplier function, a expected cache size and expiration time.
     * @param supplier provider function
     * @param maximumSize max number of elements in the cache
     * @param expiredAfterInMillis time after the an element must be evicted
     */
    public Memoizer(Function<K,V> supplier, int maximumSize, long expiredAfterInMillis) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(expiredAfterInMillis, TimeUnit.MILLISECONDS)
                .build(
                        new CacheLoader<K, V>() {
                            @Override
                            public V load(K key) {
                                return supplier.apply(key);
                            }
                        });
    }

    /**
     * Retrieves an element form the cache.
     * @param key identifier of stored elements
     * @return cached element or null
     */
    public V get(K key) {
        try {
            return cache.get(key);
        } catch(ExecutionException ex) {
            throw Throwables.propagate(ex);
        }
    }
}
