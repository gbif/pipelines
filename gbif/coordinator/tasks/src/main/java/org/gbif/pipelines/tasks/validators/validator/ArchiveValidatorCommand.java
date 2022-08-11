package org.gbif.pipelines.tasks.validators.validator;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to validate downloaded archive. This command starts
 * a service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage } and perform conversion
 */
@MetaInfServices(Command.class)
public class ArchiveValidatorCommand extends ServiceCommand {

  private final ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();

  public ArchiveValidatorCommand() {
    super("pipelines-validator-archive-validator");
  }

  @Override
  protected Service getService() {
    return new ArchiveValidatorService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
