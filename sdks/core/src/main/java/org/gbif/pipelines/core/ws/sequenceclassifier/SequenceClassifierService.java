package org.gbif.pipelines.core.ws.sequenceclassifier;

import java.util.List;
import org.gbif.pipelines.core.ws.sequenceclassifier.request.ClassifyOccurrenceRequest;
import org.gbif.pipelines.core.ws.sequenceclassifier.response.ClassifyOccurrenceResponse;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

/** Retrofit definition of the sequence-classifier-ws endpoints used by the pipelines. */
public interface SequenceClassifierService {

  /** Classifies a batch of occurrences. */
  @POST("/occurrence/classify/batch")
  Call<List<ClassifyOccurrenceResponse>> classifyBatch(
      @Body List<ClassifyOccurrenceRequest> occurrences);
}
