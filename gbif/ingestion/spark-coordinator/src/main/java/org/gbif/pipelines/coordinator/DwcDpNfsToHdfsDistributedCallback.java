package org.gbif.pipelines.coordinator;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpNfsToHdfsMessage;
import org.gbif.pipelines.airflow.*;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.util.DistributedUtil;

@Slf4j
public class DwcDpNfsToHdfsDistributedCallback extends DwcDpNfsToHdfsCallback {

  public DwcDpNfsToHdfsDistributedCallback(
      PipelinesConfig config, MessagePublisher publisher, String master) {
    super(config, publisher, master);
  }

  @Override
  protected void runPipeline(DwcDpNfsToHdfsMessage message) throws Exception {
    String jobname = "nfs-to-hdfs";
    String dagName = getDagName(pipelinesConfig);
    DistributedUtil.runPipeline(
        pipelinesConfig, message, jobname, fileSystem, dagName, StepType.NFS_TO_HDFS, List.of());
  }

  private static String getDagName(PipelinesConfig config) {
    return config.getAirflowConfig().getDwcDpNfsToHdfsDag();
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
