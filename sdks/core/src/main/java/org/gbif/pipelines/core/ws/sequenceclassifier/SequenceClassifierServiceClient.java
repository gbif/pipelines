package org.gbif.pipelines.core.ws.sequenceclassifier;

import io.github.resilience4j.retry.Retry;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.xml.ws.WebServiceException;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.factory.RetryFactory;
import org.gbif.pipelines.core.ws.sequenceclassifier.request.ClassifyOccurrenceRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import retrofit2.Call;
import retrofit2.HttpException;
import retrofit2.Response;

public class SequenceClassifierServiceClient {

  private final SequenceClassifierServiceFactory rest;

  private final Retry retry;

  private SequenceClassifierServiceClient(WsConfig wsConfig) {
    rest = SequenceClassifierServiceFactory.getInstance(wsConfig);
    retry = RetryFactory.create(wsConfig.getRetryConfig(), "SequenceClassifierServiceCall");
  }

  public static SequenceClassifierServiceClient create(WsConfig wsConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    Objects.requireNonNull(wsConfig.getWsUrl(), "WS URL is required");
    return new SequenceClassifierServiceClient(wsConfig);
  }

  /**
   * Classifies a batch of occurrences using their nucleotide sequences.
   *
   * @return one {@link ClassifyOccurrenceResponse} per input occurrence, in input order. The
   *     classification of a result is null when the occurrence has no (valid) sequences or nothing
   *     matched.
   */
  public List<ClassifyOccurrenceResponse> classifyBatch(
      List<ClassifyOccurrenceRequest> occurrences) {
    Objects.requireNonNull(occurrences);
    if (occurrences.isEmpty()) {
      return Collections.emptyList();
    }

    return Retry.decorateFunction(
            retry,
            (List<ClassifyOccurrenceRequest> occs) -> {
              Call<List<ClassifyOccurrenceResponse>> call = rest.getService().classifyBatch(occs);
              try {
                Response<List<ClassifyOccurrenceResponse>> execute = call.execute();
                if (execute.isSuccessful()) {
                  return execute.body();
                } else {
                  throw new HttpException(execute);
                }
              } catch (IOException e) {
                throw new WebServiceException("Error making request " + call.request(), e);
              }
            })
        .apply(occurrences);
  }
}
