package au.org.ala.util;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import java.io.File;

public class TestUtils {

  public static ALAPipelinesConfig getConfig() {
    String absolutePath = new File("target/test-classes/pipelines.yaml").getAbsolutePath();
    return ALAPipelinesConfigFactory.getInstance(null, null, absolutePath).get();
  }
}
