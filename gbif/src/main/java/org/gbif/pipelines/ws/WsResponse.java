package org.gbif.pipelines.ws;

import java.util.function.Predicate;

/**
 * Wraps the response from a WS.
 *
 * @param <T> type of the body of the response.
 */
public class WsResponse<T> {

  private final T body;
  private final Integer httpResponseCode;
  private final boolean error;
  private final WsErrorCode errorCode;
  private final String errorMessage;

  private WsResponse(Builder<T> builder) {
    this.body = builder.body;
    this.httpResponseCode = builder.httpResponseCode;
    this.error = builder.error;
    this.errorMessage = builder.errorMessage;
    this.errorCode = builder.errorCode;
  }

  /**
   * Creates a {@link WsResponse} for a successful call.
   */
  public static <S> WsResponse<S> success(S body) {
    return Builder.<S>newBuilder().body(body).build();
  }

  /**
   * Creates a {@link WsResponse} for a failed call.
   */
  public static <S> WsResponse<S> fail(S body, int responseCode, String errorMessage, WsErrorCode errorCode) {
    return Builder.<S>newBuilder().body(body)
      .httpResponseCode(responseCode)
      .error(true)
      .errorCode(errorCode)
      .errorMessage(errorMessage)
      .build();
  }

  /**
   * Creates a {@link WsResponse} for a failed call.
   */
  public static <S> WsResponse<S> fail(S body, String errorMessage, WsErrorCode errorCode) {
    return Builder.<S>newBuilder().body(body).error(true).errorCode(errorCode).errorMessage(errorMessage).build();
  }

  /**
   * Checks if the response body is empty.
   *
   * @param emptyValidator {@link Predicate} that checks if the response is empty
   *
   * @return true if it is empty, false otherwise.
   */
  public boolean isResponsyEmpty(Predicate<T> emptyValidator) {
    return emptyValidator.test(body);
  }

  public T getBody() {
    return body;
  }

  public Integer getHttpResponseCode() {
    return httpResponseCode;
  }

  public boolean isError() {
    return error;
  }

  public WsErrorCode getErrorCode() {
    return errorCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  private static class Builder<T> {

    private T body;
    private int httpResponseCode;
    private boolean error;
    private WsErrorCode errorCode;
    private String errorMessage;

    static Builder newBuilder() {
      return new Builder();
    }

    Builder body(T body) {
      this.body = body;
      return this;
    }

    Builder httpResponseCode(int code) {
      httpResponseCode = code;
      return this;
    }

    Builder error(boolean error) {
      this.error = error;
      return this;
    }

    Builder errorCode(WsErrorCode errorCode) {
      this.errorCode = errorCode;
      return this;
    }

    Builder errorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    WsResponse<T> build() {
      return new WsResponse<>(this);
    }

  }

  /**
   * Enum with the possible errors.
   */
  public enum WsErrorCode {
    CALL_FAILED, UNEXPECTED_ERROR;
  }

}
