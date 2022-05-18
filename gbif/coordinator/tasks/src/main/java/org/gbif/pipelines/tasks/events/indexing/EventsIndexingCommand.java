package org.gbif.pipelines.tasks.events.indexing;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to index dataset events. This command starts a
 * service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage }.
 */
@MetaInfServices(Command.class)
public class EventsIndexingCommand extends ServiceCommand {

  private final EventsIndexingConfiguration config = new EventsIndexingConfiguration();

  public EventsIndexingCommand() {
    super("pipelines-event-indexing");
  }

  @Override
  protected Service getService() {
    return new EventsIndexingService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
