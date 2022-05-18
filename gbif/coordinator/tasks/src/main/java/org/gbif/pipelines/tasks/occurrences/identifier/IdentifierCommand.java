package org.gbif.pipelines.tasks.occurrences.identifier;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * CLI for GBIF IDs lookups and validations, cli runs GBIF id lookup without generating with GBIF
 * IDs and applies some validation rules
 */
@MetaInfServices(Command.class)
public class IdentifierCommand extends ServiceCommand {

  private final IdentifierConfiguration config = new IdentifierConfiguration();

  public IdentifierCommand() {
    super("pipelines-identifier");
  }

  @Override
  protected Service getService() {
    return new IdentifierService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
