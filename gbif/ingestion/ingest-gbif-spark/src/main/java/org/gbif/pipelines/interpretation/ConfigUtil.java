package org.gbif.pipelines.interpretation;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

public class ConfigUtil {

  public static PipelinesConfig loadConfig(String configPath) {
    try (BufferedReader br = new BufferedReader(new FileReader(configPath, UTF_8))) {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);

      SimpleModule keyTermDeserializer = new SimpleModule();
      keyTermDeserializer.addKeyDeserializer(
          Term.class,
          new KeyDeserializer() {
            @Override
            public Term deserializeKey(String value, DeserializationContext dc) {
              return TermFactory.instance().findTerm(value);
            }
          });
      mapper.registerModule(keyTermDeserializer);

      mapper.findAndRegisterModules();
      return mapper.readValue(br, PipelinesConfig.class);
    } catch (IOException e) {
      System.err.println("Error reading config file: " + e.getMessage());
      throw new RuntimeException("Failed to load configuration", e);
    }
  }
}
