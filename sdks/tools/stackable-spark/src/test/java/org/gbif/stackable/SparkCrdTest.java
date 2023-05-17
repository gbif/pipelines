package org.gbif.stackable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.InputStream;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test class basically compiles the examples provided by Stackable. Most of them found at this
 * <a href="https://docs.stackable.tech/home/stable/spark-k8s/usage-guide/examples.html">page</a>.
 */
public class SparkCrdTest {

  private static final String TEST_FILE = "spark-cdrs.yaml";

  @Test
  @SneakyThrows
  public void externalResourcesSerDeSerTest() {
    readAllSparkCrds(TEST_FILE)
        .forEach(
            sparkCrd -> {
              String sparkCdrYaml = sparkCrd.toYamlString();

              SparkCrd sparkCrd2 = SparkCrd.fromYaml(sparkCdrYaml);

              Assert.assertEquals(sparkCrd, sparkCrd2);
            });
  }

  @SneakyThrows
  public List<SparkCrd> readAllSparkCrds(String testFile) {
    try (InputStream testFileInputStream =
        SparkCrdTest.class.getClassLoader().getResourceAsStream(testFile)) {
      YAMLFactory yamlFactory = new YAMLFactory();
      ObjectMapper mapper = new ObjectMapper(yamlFactory);
      return mapper
          .readValues(
              yamlFactory.createParser(testFileInputStream), new TypeReference<SparkCrd>() {})
          .readAll();
    }
  }
}
