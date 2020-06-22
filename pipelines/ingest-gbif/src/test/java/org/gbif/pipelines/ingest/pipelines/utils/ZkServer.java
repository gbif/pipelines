package org.gbif.pipelines.ingest.pipelines.utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Properties;

import org.gbif.pipelines.parsers.config.model.PipelinesConfig;

import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * ZK server for testing purposes.
 *
 * <p>This class is intended to be used as a {@link org.junit.ClassRule}.
 */
@Slf4j
@Getter
public class ZkServer extends ExternalResource {

  private TestingServer zkServer;

  public static final String LOCK_PROPERTIES_PATH = "lock.yaml";

  @Override
  protected void before() throws Throwable {
    zkServer = new TestingServer(true);
    updateLockProperties();
  }

  @Override
  protected void after() {
    try {
      zkServer.stop();
      zkServer.close();
    } catch (IOException e) {
      log.error("Could not close zk server for testing", e);
    }
  }

  private void updateLockProperties() throws IOException, URISyntaxException {
    // create props
    PipelinesConfig config;
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER));
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    mapper.findAndRegisterModules();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(LOCK_PROPERTIES_PATH)) {
      config = mapper.readValue(in, PipelinesConfig.class);
      // update properties file with connection string
      config.getIndexLock().setZkConnectionString(zkServer.getConnectString());
    }

    // write properties to the file
    URL urlFile = Thread.currentThread().getContextClassLoader().getResource(LOCK_PROPERTIES_PATH);
    try (FileOutputStream out = new FileOutputStream(Paths.get(urlFile.toURI()).toFile())) {
      mapper.writeValue(out, config);
    }
  }
}
