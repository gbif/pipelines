package org.gbif.pipelines.parsers.ws;

/**
 * Wraps the response from a WS.
 *
 * @param <T> type of the body of the response.
 */
public class HttpResponse<T> {

  private final T body;
  private final Integer httpResponseCode;
  private final boolean error;
  private final ErrorCode errorCode;
  private final String errorMessage;

  private HttpResponse(Builder<T> builder) {
    this.body = builder.body;
    this.httpResponseCode = builder.httpResponseCode;
    this.error = builder.error;
    this.errorMessage = builder.errorMessage;
    this.errorCode = builder.errorCode;
  }

  /** Creates a {@link HttpResponse} for a successful call. */
  public static <S> HttpResponse<S> success(S body) {
    return Builder.<S>newBuilder().body(body).build();
  }

  /** Creates a {@link HttpResponse} for a failed call. */
  public static <S> HttpResponse<S> fail(
      int responseCode, String errorMessage, ErrorCode errorCode) {
    return Builder.<S>newBuilder()
        .httpResponseCode(responseCode)
        .fail()
        .errorCode(errorCode)
        .errorMessage(errorMessage)
        .build();
  }

  /** Creates a {@link HttpResponse} for a failed call. */
  public static <S> HttpResponse<S> fail(String errorMessage, ErrorCode errorCode) {
    return Builder.<S>newBuilder().fail().errorCode(errorCode).errorMessage(errorMessage).build();
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

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  private static class Builder<T> {

    private T body;
    private int httpResponseCode;
    private boolean error;
    private ErrorCode errorCode;
    private String errorMessage;

    static <T> Builder<T> newBuilder() {
      return new Builder<>();
    }

    Builder<T> body(T body) {
      this.body = body;
      return this;
    }

    Builder<T> httpResponseCode(int code) {
      httpResponseCode = code;
      return this;
    }

    Builder<T> fail() {
      this.error = true;
      return this;
    }

    Builder<T> errorCode(ErrorCode errorCode) {
      this.errorCode = errorCode;
      return this;
    }

    Builder<T> errorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    HttpResponse<T> build() {
      return new HttpResponse<>(this);
    }
  }

  /** Enum with the possible errors. */
  public enum ErrorCode {
    CALL_FAILED,
    UNEXPECTED_ERROR,
    ABORTED
  }
}
