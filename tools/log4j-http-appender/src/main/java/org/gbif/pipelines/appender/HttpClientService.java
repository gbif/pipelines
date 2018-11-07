package org.gbif.pipelines.appender;

import okhttp3.RequestBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.POST;

interface HttpClientService {

  @POST(" ")
  Call<Void> append(@Body RequestBody body);
}
