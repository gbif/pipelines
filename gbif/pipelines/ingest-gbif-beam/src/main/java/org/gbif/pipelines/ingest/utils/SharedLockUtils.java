package org.gbif.pipelines.ingest.utils;

import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.gbif.pipelines.core.config.model.LockConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;
import org.gbif.wrangler.lock.Mutex;
import org.gbif.wrangler.lock.zookeeper.ZookeeperSharedReadWriteMutex;

/**
 * Utility class to create instances of ReadWrite locks using Curator Framework, Zookeeper Servers
 * and GBIF Wrangler.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SharedLockUtils {

  /** Creates an non-started instance of {@link CuratorFramework}. */
  private static CuratorFramework curator(LockConfig config) {
    return CuratorFrameworkFactory.builder()
        .namespace(config.getNamespace())
        .retryPolicy(new ExponentialBackoffRetry(config.getSleepTimeMs(), config.getMaxRetries()))
        .connectString(config.getZkConnectionString())
        .build();
  }

  @SneakyThrows
  public static void doInBarrier(LockConfig config, Mutex.Action action) {
    try (CuratorFramework curator = curator(config)) {
      curator.start();
      String lockPath = config.getLockingPath() + config.getLockName();
      DistributedBarrier barrier = new DistributedBarrier(curator, lockPath);
      log.info("Acquiring barrier {}", lockPath);
      barrier.waitOnBarrier();
      log.info("Setting barrier {}", lockPath);
      barrier.setBarrier();
      action.execute();
      log.info("Removing barrier {}", lockPath);
      barrier.removeBarrier();
    }
  }

  /**
   * @param config lock configuration
   * @param action action to be executed
   */
  private static void doInCurator(
      LockConfig config,
      Mutex.Action action,
      Function<ZookeeperSharedReadWriteMutex, Mutex> mutexCreate) {
    if (config.getLockName() == null) {
      action.execute();
    } else {
      try (CuratorFramework curator = curator(config)) {
        curator.start();
        ZookeeperSharedReadWriteMutex sharedReadWriteMutex =
            new ZookeeperSharedReadWriteMutex(curator, config.getLockingPath());
        mutexCreate.apply(sharedReadWriteMutex).doInLock(action);
      }
    }
  }

  /**
   * Performs a action in the context of write/exclusive lock.
   *
   * @param config lock configuration options
   * @param action to be performed
   */
  public static void doInWriteLock(LockConfig config, Mutex.Action action) {
    doInCurator(
        config,
        action,
        sharedReadWriteMutex -> sharedReadWriteMutex.createWriteMutex(config.getLockName()));
  }

  /**
   * Performs a action in the context of read/exclusive lock.
   *
   * @param config lock configuration options
   * @param action to be performed
   */
  public static void doInReadLock(LockConfig config, Mutex.Action action) {
    doInCurator(
        config,
        action,
        sharedReadWriteMutex -> sharedReadWriteMutex.createReadMutex(config.getLockName()));
  }

  /** A write lock is acquired to avoid concurrent modifications while this operation is running */
  public static void doHdfsPrefixLock(InterpretationPipelineOptions options, Mutex.Action action) {
    PipelinesConfig config =
        FsUtils.readConfigFile(
            options.getHdfsSiteConfig(),
            options.getCoreSiteConfig(),
            options.getProperties(),
            PipelinesConfig.class);

    String zk = config.getHdfsLock().getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? config.getZkConnectionString() : zk;
    config.getHdfsLock().setZkConnectionString(zk);

    SharedLockUtils.doInBarrier(config.getHdfsLock(), action);
  }
}
