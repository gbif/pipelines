package org.gbif.pipelines.spark;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.gbif.pipelines.core.config.model.DnaConfig;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.VocabularyConfig;
import org.gbif.pipelines.core.config.model.WsConfig;

public class TestConfigUtil {

  public static void createConfigYaml(
      String inputPath,
      String testResourcePath,
      String zkConnectionString,
      int registryPort,
      int namematchingPort,
      int geocodePort,
      int grscicollPort,
      int sequenceClassifierPort,
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

      DnaConfig dnaConfig = new DnaConfig();
      WsConfig dnaWsConfig = new WsConfig();
      dnaWsConfig.setWsUrl("http://localhost:" + sequenceClassifierPort);
      dnaConfig.setSequenceClassifierWsConfig(dnaWsConfig);
      config.setDnaConfig(dnaConfig);

      VocabularyConfig vocabularyConfig = new VocabularyConfig();
      vocabularyConfig.setVocabulariesPath(testResourcePath + "vocab");
      vocabularyConfig.setVocabulariesNames(
          Map.of(
              "http://rs.tdwg.org/dwc/terms/degreeOfEstablishment", "DegreeOfEstablishment",
              "http://rs.tdwg.org/dwc/terms/lifeStage", "LifeStage",
              "http://rs.tdwg.org/dwc/terms/establishmentMeans", "EstablishmentMeans",
              "http://rs.tdwg.org/dwc/terms/pathway", "Pathway",
              "http://rs.tdwg.org/dwc/terms/eventType", "EventType",
              "http://rs.tdwg.org/dwc/terms/occurrenceStatus", "OccurrenceStatus",
              "https://w3id.org/mixs/0000044", "target_gene"
              // #    http://rs.tdwg.org/dwc/terms/typeStatus: TypeStatus.json
              // #    http://rs.tdwg.org/dwc/terms/sex: Sex.json
              ));
      config.setVocabularyConfig(vocabularyConfig);
    }
    try (FileOutputStream out = new FileOutputStream(testResourcePath + "/" + outConfigFilePath)) {
      mapper.writeValue(out, config);
    }
  }
}
