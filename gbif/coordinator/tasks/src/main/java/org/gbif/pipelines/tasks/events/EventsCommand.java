package org.gbif.pipelines.tasks.events;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to index interpreted dataset This command starts a
 * service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage } and perform conversion
 */
@MetaInfServices(Command.class)
public class EventsCommand extends ServiceCommand {

  private final EventsConfiguration config = new EventsConfiguration();

  public EventsCommand() {
    super("pipelines-events");
  }

  @Override
  protected Service getService() {
    return new EventsService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
