package org.gbif.pipelines.common.configs;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * A configuration class which can be used to get all the details needed to create a connection to ZooKeeper needed by
 * the Curator Framework.
 * It provides a convenience method ({@link #getCuratorFramework()} ()} to actually get a {@link CuratorFramework}
 * object when populated fully.
 */
@SuppressWarnings("PublicField")
public class ZooKeeperConfiguration {

  @Parameter(
    names = "--zk-connection-string",
    description = "The connection string to connect to ZooKeeper")
  @NotNull
  public String connectionString;

  @Parameter(
    names = "--zk-namespace",
    description = "The namespace in ZooKeeper under which all data lives")
  @NotNull
  public String namespace;

  @Parameter(
    names = "--zk-sleep-time",
    description = "Initial amount of time to wait between retries in ms")
  @Min(1)
  public int baseSleepTime = 1000;

  @Parameter(
    names = "--zk-max-retries",
    description = "Max number of times to retry")
  @Min(1)
  public int maxRetries = 10;

  /**
   * This method returns a connection object to ZooKeeper with the provided settings and creates and starts a {@link
   * CuratorFramework}. These settings are not validated in this method so only call it when the object has been
   * validated.
   *
   * @return started CuratorFramework
   *
   * @throws IOException if connection fails
   */
  @JsonIgnore
  public CuratorFramework getCuratorFramework() throws IOException {
    CuratorFramework curator = CuratorFrameworkFactory.builder().namespace(namespace)
      .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries)).connectString(connectionString).build();
    curator.start();
    return curator;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("connectionString", connectionString).add("namespace", namespace)
      .add("baseSleepTime", baseSleepTime).add("maxRetries", maxRetries).toString();

  }
}
