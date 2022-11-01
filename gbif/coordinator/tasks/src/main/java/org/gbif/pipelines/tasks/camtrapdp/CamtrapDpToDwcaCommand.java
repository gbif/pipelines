package org.gbif.pipelines.tasks.camtrapdp;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to convert downloaded CamtrapDP archive to DwC-A.
 * This command starts a service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesCamtrapDpMessage } and performs the conversion.
 */
@MetaInfServices(Command.class)
public class CamtrapDpToDwcaCommand extends ServiceCommand {

  private final CamtrapToDwcaConfiguration config = new CamtrapToDwcaConfiguration();

  public CamtrapDpToDwcaCommand() {
    super("pipelines-camtrapdp-to-dwca");
  }

  @Override
  protected Service getService() {
    return new CamtrapDpToDwcaService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
