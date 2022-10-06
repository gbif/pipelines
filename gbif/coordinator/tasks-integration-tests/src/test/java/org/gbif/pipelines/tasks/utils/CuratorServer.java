package org.gbif.pipelines.tasks.utils;

import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

import java.io.IOException;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.junit.rules.ExternalResource;

@Slf4j
public class CuratorServer extends ExternalResource {

  @Getter private CuratorFramework curator;
  private TestingServer server;

  @Override
  protected void before() throws Throwable {
    server = new TestingServer();
    curator =
        CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .namespace("crawler")
            .retryPolicy(new RetryOneTime(1))
            .build();
    curator.start();
  }

  @Override
  protected void after() {
    try {
      curator.close();
      server.close();
    } catch (IOException ex) {
      log.error("Could not close curator for testing", ex);
    }
  }

  public boolean checkExists(String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }

  @SneakyThrows
  public void deletePath(String id, String path) {

    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(id, path));
  }

  @SneakyThrows
  public void deletePath(String crawlId) {
    if (checkExists(crawlId, null)) {
      deletePath(crawlId, null);
    }
  }
}
