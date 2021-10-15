package org.gbif.pipelines.tasks.interpret;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;
import lombok.AllArgsConstructor;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;

@AllArgsConstructor
public class CloseableHttpClientStub extends CloseableHttpClient {

  private final int status;
  private final String jsonResponse;

  @Override
  protected CloseableHttpResponse doExecute(
      HttpHost httpHost, HttpRequest httpRequest, HttpContext httpContext) {
    return null;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public HttpParams getParams() {
    return null;
  }

  @Override
  public ClientConnectionManager getConnectionManager() {
    return null;
  }

  @Override
  public CloseableHttpResponse execute(HttpUriRequest request) {
    return new CloseableHttpResponse() {
      @Override
      public void close() {}

      @Override
      public StatusLine getStatusLine() {
        return new StatusLine() {
          @Override
          public ProtocolVersion getProtocolVersion() {
            return null;
          }

          @Override
          public int getStatusCode() {
            return status;
          }

          @Override
          public String getReasonPhrase() {
            return null;
          }
        };
      }

      @Override
      public void setStatusLine(StatusLine statusLine) {}

      @Override
      public void setStatusLine(ProtocolVersion protocolVersion, int i) {}

      @Override
      public void setStatusLine(ProtocolVersion protocolVersion, int i, String s) {}

      @Override
      public void setStatusCode(int i) throws IllegalStateException {}

      @Override
      public void setReasonPhrase(String s) throws IllegalStateException {}

      @Override
      public HttpEntity getEntity() {
        return new HttpEntity() {
          @Override
          public boolean isRepeatable() {
            return false;
          }

          @Override
          public boolean isChunked() {
            return false;
          }

          @Override
          public long getContentLength() {
            return 0;
          }

          @Override
          public Header getContentType() {
            return null;
          }

          @Override
          public Header getContentEncoding() {
            return null;
          }

          @Override
          public InputStream getContent() throws UnsupportedOperationException {
            return new ByteArrayInputStream(jsonResponse.getBytes(UTF_8));
          }

          @Override
          public void writeTo(OutputStream outputStream) {}

          @Override
          public boolean isStreaming() {
            return false;
          }

          @Override
          public void consumeContent() {}
        };
      }

      @Override
      public void setEntity(HttpEntity httpEntity) {}

      @Override
      public Locale getLocale() {
        return null;
      }

      @Override
      public void setLocale(Locale locale) {}

      @Override
      public ProtocolVersion getProtocolVersion() {
        return null;
      }

      @Override
      public boolean containsHeader(String s) {
        return false;
      }

      @Override
      public Header[] getHeaders(String s) {
        return new Header[0];
      }

      @Override
      public Header getFirstHeader(String s) {
        return null;
      }

      @Override
      public Header getLastHeader(String s) {
        return null;
      }

      @Override
      public Header[] getAllHeaders() {
        return new Header[0];
      }

      @Override
      public void addHeader(Header header) {}

      @Override
      public void addHeader(String s, String s1) {}

      @Override
      public void setHeader(Header header) {}

      @Override
      public void setHeader(String s, String s1) {}

      @Override
      public void setHeaders(Header[] headers) {}

      @Override
      public void removeHeader(Header header) {}

      @Override
      public void removeHeaders(String s) {}

      @Override
      public HeaderIterator headerIterator() {
        return null;
      }

      @Override
      public HeaderIterator headerIterator(String s) {
        return null;
      }

      @Override
      public HttpParams getParams() {
        return null;
      }

      @Override
      public void setParams(HttpParams httpParams) {}
    };
  }
}
