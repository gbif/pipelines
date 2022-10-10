package org.gbif.pipelines.ingest.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
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

  private static final Object MUTEX = new Object();
  private static volatile ZkServer instance;
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  private TestingServer zkServer;

  public static final String PROPERTIES_PATH = "data7/ingest/pipelines.yaml";

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
      zkServer = new TestingServer(true);
      updateZkProperties();
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

  private void updateZkProperties() throws IOException, URISyntaxException {
    // create props
    PipelinesConfig config;
    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER));
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    mapper.findAndRegisterModules();

    File resource =
        Paths.get(
                Thread.currentThread().getContextClassLoader().getResource(PROPERTIES_PATH).toURI())
            .toFile();
    try (InputStream in =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(PROPERTIES_PATH)) {
      config = mapper.readValue(in, PipelinesConfig.class);
      config.setZkConnectionString(zkServer.getConnectString());
      config.getVocabularyConfig().setVocabulariesPath(resource.getParent());
    }

    // write properties to the file
    try (FileOutputStream out = new FileOutputStream(resource)) {
      mapper.writeValue(out, config);
    }
  }
}
