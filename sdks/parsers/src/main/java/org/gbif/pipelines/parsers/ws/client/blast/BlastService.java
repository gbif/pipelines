package org.gbif.pipelines.parsers.ws.client.blast;

import org.gbif.pipelines.parsers.ws.client.blast.request.Sequence;
import org.gbif.pipelines.parsers.ws.client.blast.response.Blast;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.Headers;
import retrofit2.http.POST;

public interface BlastService {

  @Headers("Cache-Control: no-cache")
  @POST("/blast")
  Call<Blast> getBlast(@Body Sequence sequence);

}
