package org.gbif.pipelines.crawler.xml;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;

import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

/**
 * CLI {@link Command} to convert XML files (ABCD archives) to Avro.
 */
@MetaInfServices(Command.class)
public class XmlToAvroCommand extends ServiceCommand {

  private final XmlToAvroConfiguration configuration = new XmlToAvroConfiguration();

  public XmlToAvroCommand() {
    super("pipelines-to-avro-from-xml");
  }

  @Override
  protected Service getService() {
    return new XmlToAvroService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
