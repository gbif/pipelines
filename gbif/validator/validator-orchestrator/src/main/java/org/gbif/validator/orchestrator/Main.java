package org.gbif.validator.orchestrator;

import org.gbif.validator.orchestrator.config.DagProperties;
import org.gbif.validator.orchestrator.config.ListenMessagingProperties;
import org.gbif.validator.orchestrator.config.OrchestratorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/** Bootstrap for the standalone validation orchestrator. */
@SpringBootApplication
@EnableConfigurationProperties({
  OrchestratorProperties.class,
  DagProperties.class,
  ListenMessagingProperties.class
})
public class Main {
  public static void main(String[] args) {
    SpringApplication.run(Main.class, args);
  }
}
