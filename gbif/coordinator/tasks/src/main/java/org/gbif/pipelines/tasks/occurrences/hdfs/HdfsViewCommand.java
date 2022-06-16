package org.gbif.pipelines.tasks.occurrences.hdfs;

import com.google.common.util.concurrent.Service;
import java.util.concurrent.ExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.common.hdfs.HdfsViewService;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to process Hdfs View This command starts a service
 * which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
 */
@MetaInfServices(Command.class)
public class HdfsViewCommand extends ServiceCommand {

  private final HdfsViewConfiguration config = new HdfsViewConfiguration();

  public HdfsViewCommand() {
    super("pipelines-occurrence-hdfs-view");
  }

  @Override
  protected Service getService() {
    return new HdfsViewService<>(config, HdfsViewCommand::createCallBack);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  public static OccurrenceHdfsViewCallback createCallBack(
      HdfsViewConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryClient historyClient,
      ExecutorService executor) {
    return new OccurrenceHdfsViewCallback(config, publisher, curator, historyClient, executor);
  }
}
