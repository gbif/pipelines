package org.gbif.pipelines.parsers.config.factory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.gbif.pipelines.parsers.config.model.LockConfig;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LockConfigFactory {

  private static final String PREFIX = "lock.";

  private static final String ZK_CONNECTION_STRING = PREFIX + "zkConnectionString";
  private static final String NAMESPACE = PREFIX + "namespace";
  private static final String LOCK_PATH = PREFIX + "path";
  private static final String LOCK_NAME = PREFIX + "name";
  private static final String CONNECTION_SLEEP_TIME_MS = PREFIX + "connection.sleepTimeMs";
  private static final String CONNECTION_MAX_RETRIES = PREFIX + "connection.maxRetries";

  // property defaults
  private static final String DEFAULT_CONNECTION_SLEEP_TIME_MS = "100";
  private static final String DEFAULT_LOCK_CONNECTION_MAX_RETRIES = "5";

  public static LockConfig create(@NonNull Path propertiesPath, String prefix) {
    // load properties or throw exception if cannot be loaded
    Properties props = ConfigFactory.loadProperties(propertiesPath);

    return create(props, prefix);
  }

  public static LockConfig create(@NonNull String propertiesPath, String prefix) {
    return create(Paths.get(propertiesPath), prefix);
  }

  public static LockConfig create(@NonNull Properties props, @NonNull String prefix) {
    String zKConnectionsString = props.getProperty(prefix + ZK_CONNECTION_STRING);
    String namespace = props.getProperty(prefix + NAMESPACE);
    String lockPath = props.getProperty(prefix + LOCK_PATH);
    String lockName = props.getProperty(prefix + LOCK_NAME);
    int sleepTimeMs = Integer.parseInt(props.getProperty(prefix + CONNECTION_SLEEP_TIME_MS, DEFAULT_CONNECTION_SLEEP_TIME_MS));
    int maxRetries = Integer.parseInt(props.getProperty(prefix + CONNECTION_MAX_RETRIES, DEFAULT_LOCK_CONNECTION_MAX_RETRIES));

    return LockConfig.create(zKConnectionsString, namespace, lockPath, lockName, sleepTimeMs, maxRetries);
  }

}
