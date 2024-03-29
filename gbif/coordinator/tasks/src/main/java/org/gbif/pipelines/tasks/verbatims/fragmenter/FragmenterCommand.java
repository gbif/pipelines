package org.gbif.pipelines.tasks.verbatims.fragmenter;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to process raw fragments from dwca/xml archives
 * This command starts a service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage}
 */
@MetaInfServices(Command.class)
public class FragmenterCommand extends ServiceCommand {

  private final FragmenterConfiguration config = new FragmenterConfiguration();

  public FragmenterCommand() {
    super("pipelines-verbatim-fragmenter");
  }

  @Override
  protected Service getService() {
    return new FragmenterService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
