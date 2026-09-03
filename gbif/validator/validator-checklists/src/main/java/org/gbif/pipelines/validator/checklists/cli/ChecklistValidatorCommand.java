package org.gbif.pipelines.validator.checklists.cli;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.pipelines.validator.checklists.cli.config.ChecklistValidatorConfiguration;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to validate downloaded archive. This command starts
 * a service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage } and perform conversion
 */
@MetaInfServices(Command.class)
public class ChecklistValidatorCommand extends ServiceCommand {

  private final ChecklistValidatorConfiguration config = new ChecklistValidatorConfiguration();

  public ChecklistValidatorCommand() {
    super("pipelines-validator-checklist-validator");
  }

  @Override
  protected Service getService() {
    return new ChecklistValidatorService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
