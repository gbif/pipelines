package org.gbif.pipelines.tasks.resources;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.junit.rules.ExternalResource;

/**
 * ZK server for testing purposes.
 *
 * <p>This class is intended to be used as a {@link org.junit.ClassRule}.
 */
@Slf4j
@Getter
public class ZkServer extends ExternalResource {

  // Disable ZK Admin Server
  static {
    System.setProperty("zookeeper.admin.enableServer", "false");
    System.setProperty("zookeeper.admin.serverPort", "0");
  }

  private static final Object MUTEX = new Object();
  private static volatile ZkServer instance;
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  private TestingServer zkServer;

  public static final String LOCK_PROPERTIES_PATH = "lock.yaml";

  public static ZkServer getInstance() {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ZkServer();
        }
      }
    }
    return instance;
  }

  @Override
  protected void before() throws Throwable {
    if (COUNTER.get() == 0) {
      zkServer = new TestingServer();
      zkServer.start();
      updateLockProperties();
    }
    COUNTER.addAndGet(1);
  }

  @Override
  protected void after() {
    if (COUNTER.addAndGet(-1) == 0) {
      try {
        zkServer.stop();
        zkServer.close();
      } catch (IOException e) {
        log.error("Could not close zk server for testing", e);
      }
    }
  }

  private void updateLockProperties() throws IOException, URISyntaxException {
    // create props
    PipelinesConfig config;
    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER));
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    mapper.findAndRegisterModules();
    try (InputStream in =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(LOCK_PROPERTIES_PATH)) {
      config = mapper.readValue(in, PipelinesConfig.class);
      // update properties file with connection string
      config.getIndexLock().setZkConnectionString(zkServer.getConnectString());
      config.getHdfsLock().setZkConnectionString(zkServer.getConnectString());
    }

    // write properties to the file
    URL urlFile = Thread.currentThread().getContextClassLoader().getResource(LOCK_PROPERTIES_PATH);
    try (FileOutputStream out = new FileOutputStream(Paths.get(urlFile.toURI()).toFile())) {
      mapper.writeValue(out, config);
    }
  }
}
