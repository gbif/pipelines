package org.gbif.pipelines.tasks.occurrences.warehouse;

import com.google.common.util.concurrent.Service;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.pipelines.tasks.common.hdfs.HdfsViewConfiguration;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to process Hdfs View This command starts a service
 * which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
 */
@MetaInfServices(Command.class)
public class DataWarehouseCommand extends ServiceCommand {

  private final HdfsViewConfiguration config = new HdfsViewConfiguration();

  public DataWarehouseCommand() {
    super("pipelines-warehouse");
    config.stepType = StepType.DATA_WAREHOUSE;
  }

  @Override
  protected Service getService() {
    return new DataWarehouseService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
