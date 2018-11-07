package org.gbif.pipelines.appender;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class HttpAppender extends AppenderSkeleton {

  private String url;
  private String contentType = "application/json";
  private int connectTimeout = 60;
  private int readTimeout = 60;

  @Override
  public void close() {
    // NOP
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }

  @Override
  protected void append(LoggingEvent event) {
    if (url == null || url.isEmpty()) {
      return;
    }

    HttpClientServiceRest.getInstance(url, connectTimeout, readTimeout)
        .getService()
        .append(RequestBody.create(MediaType.parse(contentType), this.layout.format(event)))
        .enqueue(
            new Callback<Void>() {
              @Override
              public void onResponse(Call<Void> c, Response<Void> r) {
                // NOP
              }

              @Override
              public void onFailure(Call<Void> c, Throwable t) {
                // NOP
              }
            });
  }

  public void setUrl(String url) {
    if (url != null) {
      this.url = url.endsWith("/") ? url : url + "/";
    }
  }

  public void setConnectTimeout(int connectTimeout) {
    if (connectTimeout > 0) {
      this.connectTimeout = connectTimeout;
    }
  }

  public void setReadTimeout(int readTimeout) {
    if (readTimeout > 0) {
      this.readTimeout = readTimeout;
    }
  }

  public void setContentType(String contentType) {
    if (contentType != null && !contentType.isEmpty()) {
      this.contentType = contentType;
    }
  }
}
