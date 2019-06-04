package org.gbif.pipelines.ingest.utils;

import org.gbif.pipelines.ingest.options.SharedLockOptions;
import org.gbif.wrangler.lock.Mutex;
import org.gbif.wrangler.lock.zookeeper.ZookeeperSharedReadWriteMutex;

import java.util.Objects;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Utility class to create instances of ReadWrite locks using Curator Framework, Zookeeper Servers and GBIF Wrangler.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SharedLockUtils {

  /**
   * Creates an non-started instance of {@link CuratorFramework}.
   */
  private static CuratorFramework curator(SharedLockOptions options) {
    return CuratorFrameworkFactory.builder().namespace(options.getLockNamespace())
      .retryPolicy(new ExponentialBackoffRetry(options.getLockConnectionSleepTimeMs(), options.getLockConnectionMaxRetries()))
      .connectString(options.getLockZkConnectionString())
      .build();
  }

  /**
   * Performs a action in the context of write/exclusive lock.
   * @param sharedLockOptions lock configuration options
   * @param action to be performed
   */
  public static void doInWriteLock(SharedLockOptions sharedLockOptions, Mutex.Action action) {
    if (Objects.isNull(sharedLockOptions.getLockName())) {
      action.execute();
    } else {
      try (CuratorFramework curator = curator(sharedLockOptions)) {
        curator.start();
        ZookeeperSharedReadWriteMutex sharedReadWriteMutex =
          new ZookeeperSharedReadWriteMutex(curator, sharedLockOptions.getLockingPath());
        sharedReadWriteMutex.createWriteMutex(sharedLockOptions.getLockName()).doInLock(action);
      }
    }
  }

}
