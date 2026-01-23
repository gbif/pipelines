package org.gbif.pipelines.interpretation.standalone;

import feign.Headers;
import feign.Param;
import feign.RequestLine;
import java.util.List;
import java.util.UUID;
import org.gbif.api.model.pipelines.PipelineExecution;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.ws.PipelineProcessParameters;

/**
 * Feign client for Pipelines History Service. Taken from registry-ws-client module to avoid
 * dependency issues (spring-boot).
 */
public interface PipelinesHistoryClient {

  @RequestLine("POST /pipelines/history/execution/{executionKey}/finished")
  void markPipelineExecutionIfFinished(@Param("executionKey") long executionKey);

  @RequestLine("GET /pipelines/history/execution/running/{datasetKey}")
  Long getRunningExecutionKey(@Param("datasetKey") UUID datasetKey);

  @RequestLine("GET /pipelines/history/execution/{executionKey}/step")
  List<PipelineStep> getPipelineStepsByExecutionKey(@Param("executionKey") long executionKey);

  @RequestLine("PUT /pipelines/history/step/{stepKey}")
  @Headers("Content-Type: application/json")
  long updatePipelineStep(@Param("stepKey") long stepKey, PipelineStep pipelineStep);

  @RequestLine("PUT /pipelines/history/step/{stepKey}/submittedToQueued")
  @Headers("Content-Type: application/json")
  void setSubmittedPipelineStepToQueued(@Param("stepKey") long stepKey);

  @RequestLine("POST /pipelines/history/process")
  @Headers("Content-Type: application/json")
  long createPipelineProcess(PipelineProcessParameters params);

  @RequestLine("POST /pipelines/history/process/{processKey}")
  @Headers("Content-Type: application/json")
  long addPipelineExecution(
      @Param("processKey") long processKey, PipelineExecution pipelineExecution);

  @RequestLine("GET /pipelines/history/step/{stepKey}")
  @Headers("Accept: application/json")
  PipelineStep getPipelineStep(@Param("stepKey") long stepKey);

  @RequestLine("POST /pipelines/history/execution/{executionKey}/abort")
  void markPipelineStatusAsAborted(@Param("executionKey") long executionKey);

  @RequestLine("POST /pipelines/history/identifier/{datasetKey}/{attempt}/{executionKey}/notify")
  @Headers("Content-Type: text/plain")
  void notifyAbsentIdentifiers(
      @Param("datasetKey") UUID datasetKey,
      @Param("attempt") int attempt,
      @Param("executionKey") long executionKey,
      String message);
}
