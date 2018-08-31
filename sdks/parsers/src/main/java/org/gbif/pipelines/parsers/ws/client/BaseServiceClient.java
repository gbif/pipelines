package org.gbif.pipelines.parsers.ws.client;

import org.gbif.pipelines.parsers.ws.HttpResponse;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

/**
 * Base class for a web service client.
 *
 * @param <T> parameter of the {@link Call}
 * @param <R> type of the response to return
 */
public abstract class BaseServiceClient<T, R> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseServiceClient.class);

  protected HttpResponse<R> performCall(Map<String, String> params) {

    Call<T> call = getCall(params);

    try {
      Response<T> response = call.execute();

      if (!response.isSuccessful()) {
        String errorMessage = getErrorMessage() + " - " + response.message();
        LOG.error(errorMessage);
        return HttpResponse.fail(response.code(), errorMessage, HttpResponse.ErrorCode.CALL_FAILED);
      }

      return HttpResponse.success(parseResponse(response.body()));
    } catch (IOException e) {
      LOG.error(getErrorMessage(), e);
      return HttpResponse.fail(getErrorMessage(), HttpResponse.ErrorCode.UNEXPECTED_ERROR);
    }
  }

  /**
   * Return the {@link Call} to be executed.
   *
   * @param params parameters to pass to the {@link Call}.
   * @return {@link Call} of T type.
   */
  protected abstract Call<T> getCall(Map<String, String> params);

  /** Custom error message to be used when performing the call. */
  protected abstract String getErrorMessage();

  /**
   * Parses the response T returned from the {@link Call} and returns a R response.
   *
   * @param body body response of the {@link Call} executed.
   * @return R response.
   */
  protected abstract R parseResponse(T body);
}
