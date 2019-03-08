package org.gbif.pipelines.parsers.ws.client.blast;

import java.io.IOException;
import java.util.Objects;

import org.gbif.pipelines.parsers.config.WsConfig;
import org.gbif.pipelines.parsers.ws.client.blast.request.Sequence;
import org.gbif.pipelines.parsers.ws.client.blast.response.Blast;

import javax.xml.ws.WebServiceException;
import retrofit2.Call;
import retrofit2.HttpException;
import retrofit2.Response;

public class BlastServiceClient {

  private final BlastServiceRest rest;

  private BlastServiceClient(WsConfig wsConfig) {
    rest = BlastServiceRest.getInstance(wsConfig);
  }

  public static BlastServiceClient create(WsConfig wsConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    return new BlastServiceClient(wsConfig);
  }

  public Blast getBlast(Sequence sequence) {
    Objects.requireNonNull(sequence);
    Call<Blast> call = rest.getService().getBlast(sequence);
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
  }

}
