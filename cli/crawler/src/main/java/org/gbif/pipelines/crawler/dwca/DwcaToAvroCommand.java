package org.gbif.pipelines.crawler.dwca;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

/**
 * Entry class for cli command, to start service to convert downloaded DwCA Archive to Avro.
 * This command starts a service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage } and perform conversion
 */
@MetaInfServices(Command.class)
public class DwcaToAvroCommand extends ServiceCommand {

  private final DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();

  public DwcaToAvroCommand() {
    super("pipelines-to-avro-from-dwca");
  }

  @Override
  protected Service getService() {
    return new DwcaToAvroService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
