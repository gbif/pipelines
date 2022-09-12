package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import javax.validation.constraints.Min;
import lombok.ToString;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * A configuration class which can be used to get all the details needed to create a connection to
 * ZooKeeper needed by the Curator Framework. It provides a convenience method ({@link
 * #getCuratorFramework()} ()} to actually get a {@link CuratorFramework} object when populated
 * fully.
 */
@ToString
public class ZooKeeperConfiguration {

  @Parameter(
      names = "--zk-connection-string",
      description = "The connection string to connect to ZooKeeper")
  public String connectionString;

  @Parameter(
      names = "--zk-namespace",
      description = "The namespace in ZooKeeper under which all data lives")
  public String namespace;

  @Parameter(
      names = "--zk-sleep-time",
      description = "Initial amount of time to wait between retries in ms")
  @Min(1_000)
  public int baseSleepTime = 3_000;

  @Parameter(names = "--zk-max-retries", description = "Max number of times to retry")
  @Min(5)
  public int maxRetries = 20;

  @Parameter(names = "--zk-session-timeout", description = "Curator session timeout")
  @Min(60_000)
  public int sessionTimeout = 120_000;

  @Parameter(names = "--zk-connection-timeout", description = "Curator connection timeout")
  @Min(15_000)
  public int connectionTimeout = 30_000;

  /**
   * This method returns a connection object to ZooKeeper with the provided settings and creates and
   * starts a {@link CuratorFramework}. These settings are not validated in this method so only call
   * it when the object has been validated.
   *
   * @return started CuratorFramework
   */
  @JsonIgnore
  public CuratorFramework getCuratorFramework() {
    CuratorFramework curator =
        CuratorFrameworkFactory.builder()
            .namespace(namespace)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
            .connectString(connectionString)
            .sessionTimeoutMs(sessionTimeout)
            .connectionTimeoutMs(connectionTimeout)
            .build();

    curator.start();
    return curator;
  }
}
