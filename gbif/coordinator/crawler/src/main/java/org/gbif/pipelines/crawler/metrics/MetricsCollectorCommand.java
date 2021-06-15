package org.gbif.pipelines.crawler.metrics;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to collect metrics using ES index. This command
 * starts a service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesIndexedMessage } and perform conversion
 */
@MetaInfServices(Command.class)
public class MetricsCollectorCommand extends ServiceCommand {

  private final MetricsCollectorConfiguration config = new MetricsCollectorConfiguration();

  public MetricsCollectorCommand() {
    super("pipelines-metrics-collector");
  }

  @Override
  protected Service getService() {
    return new MetricsCollectorService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
