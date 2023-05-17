package org.gbif.stackable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.util.KubeConfig;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.SneakyThrows;

public class ConfigUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

  @SneakyThrows
  public static V1ConfigMap loadConfigMap(String configMapFile) {
    try (InputStream configMapInputFile = Files.newInputStream(Paths.get(configMapFile))) {
      return MAPPER.readValue(configMapInputFile, V1ConfigMap.class);
    }
  }

  @SneakyThrows
  public static SparkCrd loadSparkCdr(String sparkApplicationConfigFile) {
    try (InputStream sparkApplicationConfigInputFile =
        Files.newInputStream(Paths.get(sparkApplicationConfigFile))) {
      return SparkCrd.fromYaml(sparkApplicationConfigInputFile);
    }
  }

  @SneakyThrows
  public static KubeConfig loadKubeConfig(String kubeConfigFile) {
    try (FileReader kubeConfigReader = new FileReader(Paths.get(kubeConfigFile).toFile())) {
      return KubeConfig.loadKubeConfig(kubeConfigReader);
    }
  }
}
