package org.gbif.pipelines.crawler.balancer;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to populate and resend messages This command starts
 * a service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesBalancerMessage}
 */
@MetaInfServices(Command.class)
public class BalancerCommand extends ServiceCommand {

  private final BalancerConfiguration config = new BalancerConfiguration();

  public BalancerCommand() {
    super("pipelines-balancer");
  }

  @Override
  protected Service getService() {
    return new BalancerService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
