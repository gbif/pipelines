package org.gbif.pipelines.core.ws.blast;

import io.github.resilience4j.retry.Retry;
import java.io.IOException;
import java.util.Objects;
import javax.xml.ws.WebServiceException;
import org.gbif.pipelines.core.config.factory.RetryFactory;
import org.gbif.pipelines.core.config.model.WsConfig;
import org.gbif.pipelines.core.ws.blast.request.Sequence;
import org.gbif.pipelines.core.ws.blast.response.Blast;
import retrofit2.Call;
import retrofit2.HttpException;
import retrofit2.Response;

public class BlastServiceClient {

  private final BlastServiceFactory rest;

  private final Retry retry;

  private BlastServiceClient(WsConfig wsConfig) {
    rest = BlastServiceFactory.getInstance(wsConfig);
    retry = RetryFactory.create(wsConfig.getRetryConfig(), "BlastServiceCall");
  }

  public static BlastServiceClient create(WsConfig wsConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    return new BlastServiceClient(wsConfig);
  }

  public Blast getBlast(Sequence sequence) {
    Objects.requireNonNull(sequence);
    return Retry.decorateFunction(
            retry,
            (Sequence seq) -> {
              Call<Blast> call = rest.getService().getBlast(seq);
              try {
                Response<Blast> execute = call.execute();
                if (execute.isSuccessful()) {
                  return execute.body();
                } else {
                  throw new HttpException(execute);
                }
              } catch (IOException e) {
                throw new WebServiceException("Error making request " + call.request(), e);
              }
            })
        .apply(sequence);
  }
}
