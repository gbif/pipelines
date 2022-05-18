package org.gbif.pipelines.tasks.validators.cleaner;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * CLI {@link Command} to delete elasticsearch index/records, file system files and hdfs file by
 * validaton uuid
 */
@MetaInfServices(Command.class)
public class CleanerCommand extends ServiceCommand {

  private final CleanerConfiguration configuration = new CleanerConfiguration();

  public CleanerCommand() {
    super("pipelines-cleaner");
  }

  @Override
  protected Service getService() {
    return new CleanerService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
