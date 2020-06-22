package org.gbif.pipelines.crawler.fragmenter;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

/**
 * Entry class for cli command, to start service to process raw fragments from dwca/xml archives
 * This command starts a service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage}
 */
@MetaInfServices(Command.class)
public class FragmenterCommand extends ServiceCommand {

  private final FragmenterConfiguration config = new FragmenterConfiguration();

  public FragmenterCommand() {
    super("pipelines-fragmenter");
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
