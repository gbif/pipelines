package org.gbif.pipelines.common.utils;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.CreateMode;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

import static org.gbif.crawler.constants.PipelinesNodePaths.SIZE;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

/**
 * Utils help to work with Zookeeper
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ZookeeperUtils {

  /**
   * Check exists a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  public static boolean checkExists(CuratorFramework curator, String crawlId) {
    try {
      return curator.checkExists().forPath(crawlId) != null;
    } catch (Exception ex) {
      log.error("Exception while calling ZooKeeper", ex);
    }
    return false;
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  public static void checkMonitoringById(CuratorFramework curator, int size, String crawlId) {
    try {
      String path = getPipelinesInfoPath(crawlId);
      if (checkExists(curator, path)) {
        InterProcessMutex mutex = new InterProcessMutex(curator, path);
        mutex.acquire();
        int counter = getAsInteger(curator, crawlId, SIZE).orElse(0) + 1;
        if (counter >= size) {
          log.info("Delete zookeeper node, crawlId - {}", crawlId);
          curator.delete().deletingChildrenIfNeeded().forPath(path);
        } else {
          updateMonitoring(curator, crawlId, SIZE, Integer.toString(counter));
        }
        mutex.release();
      }
    } catch (Exception ex) {
      log.error("Exception while updating ZooKeeper", ex);
    }
  }


  /**
   * Read value from Zookeeper as a {@link String}
   */
  public static Optional<Integer> getAsInteger(CuratorFramework curator, String crawlId, String path) throws Exception {
    String infoPath = getPipelinesInfoPath(crawlId, path);
    if (checkExists(curator, infoPath)) {
      byte[] responseData = curator.getData().forPath(infoPath);
      if (responseData != null && responseData.length > 0) {
        return Optional.of(Integer.valueOf(new String(responseData, StandardCharsets.UTF_8)));
      }
    }
    return Optional.empty();
  }

  /**
   * Creates or updates a String value for a Zookeeper monitoring node
   *
   * @param crawlId root node path
   * @param path child node path
   * @param value some String value
   */
  public static void updateMonitoring(CuratorFramework curator, String crawlId, String path, String value) {
    try {
      String fullPath = getPipelinesInfoPath(crawlId, path);
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      if (checkExists(curator, fullPath)) {
        curator.setData().forPath(fullPath, bytes);
      } else {
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(fullPath, bytes);
      }
    } catch (Exception ex) {
      log.error("Exception while updating ZooKeeper", ex);
    }
  }

  /**
   * Creates or updates current LocalDateTime value for a Zookeeper monitoring node
   *
   * @param crawlId root node path
   * @param path child node path
   */
  public static void updateMonitoringDate(CuratorFramework curator, String crawlId, String path) {
    String value = LocalDateTime.now(ZoneOffset.UTC).format(ISO_LOCAL_DATE_TIME);
    updateMonitoring(curator, crawlId, path, value);
  }
}
