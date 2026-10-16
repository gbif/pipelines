package org.gbif.pipelines.coordinator;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.util.DistributedUtil;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

/**
 * Distributed callback for the DWCDP_TO_VERBATIM step. Triggers an Airflow DAG via {@link
 * DistributedUtil} rather than running Spark in-process.
 */
@Slf4j
public class ValidatorDwcDpToVerbatimDistributedCallback extends ValidatorDwcDpToVerbatimCallback {

  public ValidatorDwcDpToVerbatimDistributedCallback(
      PipelinesConfig config, MessagePublisher publisher, String master) {
    super(config, publisher, master);
  }

  @Override
  protected void runPipeline(DwcDpToVerbatimMessage message) throws Exception {
    String jobname = "validator-dwcdp-to-verbatim";
    String dagName = getDagName(pipelinesConfig);
    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        jobname,
        fileSystem,
        dagName,
        StepType.DWCDP_TO_VERBATIM,
        List.of(
            "--containsEvents=" + message.isContainsEvents(),
            "--containsOccurrences=" + message.isContainsOccurrences()));
  }

  private static String getDagName(PipelinesConfig config) {
    return config.getAirflowConfig().getDwcDpToVerbatimDag();
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
