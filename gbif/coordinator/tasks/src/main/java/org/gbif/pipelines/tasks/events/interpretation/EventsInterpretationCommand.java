package org.gbif.pipelines.tasks.events.interpretation;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to interpret dataset events. This command starts a
 * service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesEventsMessage
 * }.
 */
@MetaInfServices(Command.class)
public class EventsInterpretationCommand extends ServiceCommand {

  private final EventsInterpretationConfiguration config = new EventsInterpretationConfiguration();

  public EventsInterpretationCommand() {
    super("pipelines-events-interpretation");
  }

  @Override
  protected Service getService() {
    return new EventsInterpretationService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
