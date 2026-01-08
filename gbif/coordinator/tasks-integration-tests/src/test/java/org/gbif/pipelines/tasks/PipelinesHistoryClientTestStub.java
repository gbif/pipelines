package org.gbif.pipelines.tasks;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.pipelines.*;
import org.gbif.api.model.pipelines.PipelineStep.Status;
import org.gbif.api.model.pipelines.ws.PipelineProcessParameters;
import org.gbif.api.model.pipelines.ws.RunAllParams;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

@Getter
@NoArgsConstructor(staticName = "create")
public class PipelinesHistoryClientTestStub implements PipelinesHistoryClient {

  private final Map<String, Long> pipelineProcessMap = new HashMap<>(1);
  private final Map<Long, PipelineProcessParameters> revertPipelineProcessMap = new HashMap<>(1);
  private final Map<Long, PipelineExecution> pipelineExecutionMap = new HashMap<>(1);
  private final Map<Long, PipelineStep> pipelineStepMap = new HashMap<>(10);
  private final Map<UUID, Long> runningExecutionMap = new HashMap<>(1);

  @Override
  public PagingResponse<PipelineProcess> history(Pageable pageable) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public PagingResponse<PipelineProcess> history(UUID uuid, Pageable pageable) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public PipelineProcess getPipelineProcess(UUID uuid, int i) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public long createPipelineProcess(PipelineProcessParameters pp) {
    String stringKey = pp.getDatasetKey().toString() + pp.getAttempt();
    if (!pipelineProcessMap.containsKey(stringKey)) {
      long l = ThreadLocalRandom.current().nextLong(10L, 1_000L);
      pipelineProcessMap.put(stringKey, l);
      revertPipelineProcessMap.put(l, pp);
      return l;
    }
    return pipelineProcessMap.get(stringKey);
  }

  @Override
  public long addPipelineExecution(long l, PipelineExecution pipelineExecution) {
    if (revertPipelineProcessMap.containsKey(l)) {
      Set<PipelineStep> stepSet = new HashSet<>(pipelineExecution.getStepsToRun().size());
      long ek = ThreadLocalRandom.current().nextLong(2_000L, 3_000L);
      for (StepType st : pipelineExecution.getStepsToRun()) {
        PipelineStep s =
            new PipelineStep()
                .setStarted(OffsetDateTime.now())
                .setState(Status.SUBMITTED)
                .setType(st);
        s.setKey(++ek);

        stepSet.add(s);
        pipelineStepMap.put(ek, s);
      }

      long k = ThreadLocalRandom.current().nextLong(10L, 1_000L);
      pipelineExecution.setKey(k);
      pipelineExecution.setSteps(stepSet);
      pipelineExecutionMap.put(k, pipelineExecution);

      PipelineProcessParameters processParameters = revertPipelineProcessMap.get(l);
      runningExecutionMap.put(processParameters.getDatasetKey(), k);

      return k;
    }
    throw new UnsupportedOperationException("Can't find the pipeline process key - " + l);
  }

  @Override
  public Long getRunningExecutionKey(@NotNull UUID uuid) {
    return runningExecutionMap.get(uuid);
  }

  @Override
  public List<PipelineStep> getPipelineStepsByExecutionKey(long l) {
    return new ArrayList<>(pipelineExecutionMap.get(l).getSteps());
  }

  @Override
  public PagingResponse<PipelineProcess> getRunningPipelineProcess(
      @Nullable StepType stepType, @Nullable StepRunner stepRunner, Pageable pageable) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public void markAllPipelineExecutionAsFinished() {}

  @Override
  public void markPipelineExecutionIfFinished(long l) {}

  @Override
  public void markPipelineStatusAsAborted(long l) {}

  @Override
  public long updatePipelineStep(PipelineStep pipelineStep) {
    return updatePipelineStep(pipelineStep.getKey(), pipelineStep);
  }

  @Override
  public long updatePipelineStep(long key, PipelineStep pipelineStep) {
    pipelineStepMap.put(key, pipelineStep);
    return pipelineStep.getKey();
  }

  @Override
  public PipelineStep getPipelineStep(long l) {
    return pipelineStepMap.get(l);
  }

  @Override
  public RunPipelineResponse runAll(
      String s, String s1, boolean b, boolean b1, RunAllParams runAllParams, Set<String> set) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public RunPipelineResponse runPipelineAttempt(
      UUID uuid, String s, String s1, boolean b, boolean b1, Set<String> set) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public RunPipelineResponse runPipelineAttempt(
      UUID uuid, int i, String s, String s1, boolean b, Set<String> set) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public void sendAbsentIndentifiersEmail(UUID uuid, int i, String s) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public void allowAbsentIndentifiers(UUID uuid, int i) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public void allowAbsentIndentifiers(UUID uuid) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public void notifyAbsentIdentifiers(UUID uuid, int i, long l, String s) {
    throw new UnsupportedOperationException("The method is not implemented!");
  }

  @Override
  public void setSubmittedPipelineStepToQueued(long key) {
    pipelineStepMap.get(key).setState(Status.QUEUED);
  }

  public Map<StepType, PipelineStep> getStepMap() {
    return pipelineStepMap.values().stream()
        .collect(Collectors.toMap(PipelineStep::getType, Function.identity()));
  }
}
