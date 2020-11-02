package org.gbif.pipelines.crawler.hdfs;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to process Hdfs View This command starts a service
 * which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
 */
@MetaInfServices(Command.class)
public class HdfsViewCommand extends ServiceCommand {

  private final HdfsViewConfiguration config = new HdfsViewConfiguration();

  public HdfsViewCommand() {
    super("pipelines-hdfs-view");
  }

  @Override
  protected Service getService() {
    return new HdfsViewService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
