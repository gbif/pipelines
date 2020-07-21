package au.org.ala.util;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import java.io.File;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestUtils {

  public static ALAPipelinesConfig getConfig() {
    String absolutePath = new File(getPipelinesConfigFile()).getAbsolutePath();
    return ALAPipelinesConfigFactory.getInstance(null, null, absolutePath).get();
  }

  public static String getPipelinesConfigFile() {
    return System.getProperty("pipelinesTestYamlConfigFile", "target/test-classes/pipelines.yaml");
  }
}
