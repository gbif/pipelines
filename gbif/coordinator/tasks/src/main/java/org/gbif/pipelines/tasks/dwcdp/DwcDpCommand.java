package org.gbif.pipelines.tasks.dwcdp;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to handle {@link
 * org.gbif.common.messaging.api.messages.DwcDpDownloadFinishedMessage } and call the Airflow DAG to
 * process it.
 */
@MetaInfServices(Command.class)
public class DwcDpCommand extends ServiceCommand {

  private final DwcDpConfiguration config = new DwcDpConfiguration();

  public DwcDpCommand() {
    super("pipelines-dwc-dp");
  }

  @Override
  protected Service getService() {
    return new DwcDpService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
