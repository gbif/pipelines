package org.gbif.pipelines.core.ws.sequenceclassifier;

import io.github.resilience4j.retry.Retry;
import io.vavr.CheckedFunction1;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.xml.ws.WebServiceException;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.factory.RetryFactory;
import org.gbif.pipelines.core.ws.sequenceclassifier.request.ClassifyOccurrenceRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import retrofit2.Call;
import retrofit2.HttpException;
import retrofit2.Response;

public class SequenceClassifierServiceClient {

  /**
   * Transient errors worth retrying: rate limited, bad gateway, unavailable and gateway timeout.
   * Other errors, like a 500 caused by the content of the batch, would fail again.
   */
  private static final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(429, 502, 503, 504);

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
   * Classifies a batch of occurrences using their nucleotide sequences. Connection errors and the
   * {@link #RETRYABLE_STATUS_CODES} are retried according to the retry configuration.
   *
   * @return the {@link ClassifyOccurrenceResponse} of each classified occurrence, identified by its
   *     gbifID; empty if the ws returned nothing. The classification of a result is null when the
   *     occurrence has no (valid) sequences or nothing matched.
   * @throws WebServiceException if the request fails after the retries
   */
  public List<ClassifyOccurrenceResponse> classifyBatch(
      List<ClassifyOccurrenceRequest> occurrences) {
    Objects.requireNonNull(occurrences);
    if (occurrences.isEmpty()) {
      return Collections.emptyList();
    }

    CheckedFunction1<List<ClassifyOccurrenceRequest>, List<ClassifyOccurrenceResponse>> classify =
        Retry.decorateCheckedFunction(retry, this::callClassifyBatch);
    List<ClassifyOccurrenceResponse> responses;
    try {
      responses = classify.apply(occurrences);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new WebServiceException("Error classifying occurrences", e);
    }
    // basic checks only: the occurrences without a result are handled by the caller
    if (responses == null) {
      return Collections.emptyList();
    }
    return responses.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * The IOExceptions are thrown to the retry, as well as the {@link #RETRYABLE_STATUS_CODES} as
   * IOExceptions. The other errors are not retried.
   */
  private List<ClassifyOccurrenceResponse> callClassifyBatch(
      List<ClassifyOccurrenceRequest> occurrences) throws IOException {
    Call<List<ClassifyOccurrenceResponse>> call = rest.getService().classifyBatch(occurrences);
    Response<List<ClassifyOccurrenceResponse>> response = call.execute();
    if (response.isSuccessful()) {
      return response.body();
    }
    if (RETRYABLE_STATUS_CODES.contains(response.code())) {
      throw new IOException(
          "Retryable error " + response.code() + " making request " + call.request());
    }
    throw new HttpException(response);
  }
}
