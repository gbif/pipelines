package org.gbif.pipelines.spark;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.VocabularyConfig;

public class TestConfigUtil {

  public static void createConfigYaml(
      String inputPath,
      String testResourcePath,
      String zkConnectionString,
      int registryPort,
      int namematchingPort,
      int geocodePort,
      int grscicollPort,
      String outConfigFilePath)
      throws IOException {

    // create props
    PipelinesConfig config;
    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    mapper.findAndRegisterModules();

    try (InputStream in =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("pipelines_template.yaml")) {
      config = mapper.readValue(in, PipelinesConfig.class);

      config.setInputPath(inputPath);
      config.setOutputPath(inputPath);
      config
          .getNameUsageMatchingService()
          .getWs()
          .getApi()
          .setWsUrl("http://localhost:" + namematchingPort);
      config.getGrscicollLookup().getApi().setWsUrl("http://localhost:" + grscicollPort + "/v1/");
      config.getGeocode().getApi().setWsUrl("http://localhost:" + geocodePort + "/v1/");
      config.getGbifApi().setWsUrl("http://localhost:" + registryPort + "/v1/");
      config.getKeygen().setZkConnectionString(zkConnectionString);

      VocabularyConfig vocabularyConfig = new VocabularyConfig();
      vocabularyConfig.setVocabulariesPath(testResourcePath + "vocabs");
      config.setVocabularyConfig(vocabularyConfig);
    }
    try (FileOutputStream out = new FileOutputStream(testResourcePath + "/" + outConfigFilePath)) {
      mapper.writeValue(out, config);
    }
  }
}
