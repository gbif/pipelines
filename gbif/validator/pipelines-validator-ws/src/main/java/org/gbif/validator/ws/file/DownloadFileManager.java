package org.gbif.validator.ws.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * Downloads files from user supplied URLs. Only http(s) URLs of hosts with public addresses are
 * fetched, and redirects are followed here so every hop is checked, to prevent requests to internal
 * services (SSRF).
 */
@Slf4j
@Service
public class DownloadFileManager {

  private static final Set<String> SUPPORTED_CONTENT_TYPES =
      Set.of("application/zip", "application/xml", "text/csv");

  private static final Set<String> SUPPORTED_SCHEMES = Set.of("http", "https");

  private static final int MAX_REDIRECTS = 5;

  private static final int CONNECT_TIMEOUT_MS = 10_000;

  private static final int READ_TIMEOUT_MS = 60_000;

  // Hosts allowed even if they resolve to a non-public address, e.g. for tests
  private final Set<String> allowedPrivateHosts;

  public DownloadFileManager(
      @Value("${validation.url.allowedPrivateHosts:}") String[] allowedPrivateHosts) {
    this.allowedPrivateHosts =
        Arrays.stream(allowedPrivateHosts)
            .map(String::trim)
            .filter(h -> !h.isEmpty())
            .map(h -> h.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
  }

  public boolean isAvailable(String url) {
    HttpURLConnection con = null;
    try {
      con = open(url, "HEAD");
      String content = con.getHeaderField("Content-Type");

      return con.getResponseCode() == HttpURLConnection.HTTP_OK
          && content != null
          && SUPPORTED_CONTENT_TYPES.stream().anyMatch(content::contains);
    } catch (Exception e) {
      log.warn("Error getting file information", e);
      return false;
    } finally {
      if (con != null) {
        con.disconnect();
      }
    }
  }

  /** Gets the URL a HEAD request ends at, after following redirects. */
  public String getFinalUrl(String url) throws IOException {
    HttpURLConnection con = open(url, "HEAD");
    try {
      return con.getURL().toString();
    } finally {
      con.disconnect();
    }
  }

  /** Gets a response header of a HEAD request, null if not present. */
  public String getHeader(String url, String header) throws IOException {
    HttpURLConnection con = open(url, "HEAD");
    try {
      return con.getHeaderField(header);
    } finally {
      con.disconnect();
    }
  }

  @SneakyThrows
  public File download(String url, Path targetFilePath) {
    Files.createDirectories(targetFilePath.getParent());
    File targetFile = targetFilePath.toFile();
    HttpURLConnection con = open(url, "GET");
    try (InputStream body = con.getInputStream();
        ReadableByteChannel in = Channels.newChannel(body);
        FileOutputStream fileOutputStream = new FileOutputStream(targetFile)) {
      fileOutputStream.getChannel().transferFrom(in, 0, Long.MAX_VALUE);
      return targetFile;
    } finally {
      con.disconnect();
    }
  }

  /** Downloads a file asynchronously. */
  public CompletableFuture<File> downloadAsync(String url, Path targetFilePath) {
    return CompletableFuture.supplyAsync(() -> download(url, targetFilePath));
  }

  @Async
  @SneakyThrows
  public CompletableFuture<File> downloadAsync(
      String url,
      Path targetFilePath,
      Consumer<File> successCallback,
      Consumer<Throwable> errorCallback) {
    return downloadAsync(url, targetFilePath)
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                log.error("Error downloading file from url " + url, error);
                errorCallback.accept(error);
              } else {
                successCallback.accept(result);
              }
            });
  }

  /**
   * Sends a request, following redirects and checking every URL, and returns the connection of the
   * final response.
   */
  private HttpURLConnection open(String url, String method) throws IOException {
    URL current = new URL(url);
    for (int redirects = 0; redirects <= MAX_REDIRECTS; redirects++) {
      checkUrl(current);
      HttpURLConnection con = (HttpURLConnection) current.openConnection();
      con.setInstanceFollowRedirects(false);
      con.setConnectTimeout(CONNECT_TIMEOUT_MS);
      con.setReadTimeout(READ_TIMEOUT_MS);
      con.setRequestMethod(method);

      int status = con.getResponseCode();
      String location = con.getHeaderField("Location");
      if (status < 300 || status >= 400 || location == null) {
        return con;
      }
      con.disconnect();
      current = new URL(current, location);
    }
    throw new IOException("Too many redirects for " + url);
  }

  /** Asserts the URL is http(s) and its host has only public addresses. */
  void checkUrl(URL url) throws IOException {
    if (!SUPPORTED_SCHEMES.contains(url.getProtocol().toLowerCase(Locale.ROOT))) {
      throw new UnsafeUrlException("Only http and https URLs are supported: " + url);
    }
    String host = url.getHost();
    if (host == null || host.isEmpty()) {
      throw new UnsafeUrlException("URL without host: " + url);
    }
    if (allowedPrivateHosts.contains(host.toLowerCase(Locale.ROOT))) {
      return;
    }
    for (InetAddress address : InetAddress.getAllByName(host)) {
      if (!isPublic(address)) {
        throw new UnsafeUrlException("URL host is not a public address: " + url);
      }
    }
  }

  /** True if the address is publicly routable. */
  static boolean isPublic(InetAddress address) {
    if (address.isAnyLocalAddress()
        || address.isLoopbackAddress()
        || address.isLinkLocalAddress()
        || address.isSiteLocalAddress()
        || address.isMulticastAddress()) {
      return false;
    }
    byte[] b = address.getAddress();
    if (address instanceof Inet4Address) {
      int b0 = b[0] & 0xff;
      int b1 = b[1] & 0xff;
      int b2 = b[2] & 0xff;
      return b0 != 0 // "this" network
          && !(b0 == 100 && (b1 & 0xc0) == 64) // 100.64.0.0/10, carrier-grade NAT
          && !(b0 == 192 && b1 == 0 && b2 == 0) // 192.0.0.0/24, IETF protocol assignments
          && !(b0 == 198 && (b1 & 0xfe) == 18) // 198.18.0.0/15, benchmarking
          && b0 < 240; // 240.0.0.0/4, reserved and broadcast
    }
    // IPv4-compatible (::a.b.c.d) and NAT64 (64:ff9b::a.b.c.d) addresses embed an IPv4 address
    boolean ipv4Compatible = Arrays.equals(Arrays.copyOf(b, 12), new byte[12]);
    boolean nat64 =
        Arrays.equals(
            Arrays.copyOf(b, 12),
            new byte[] {0, 0x64, (byte) 0xff, (byte) 0x9b, 0, 0, 0, 0, 0, 0, 0, 0});
    if (ipv4Compatible || nat64) {
      try {
        return isPublic(InetAddress.getByAddress(Arrays.copyOfRange(b, 12, 16)));
      } catch (IOException e) {
        return false;
      }
    }
    // fc00::/7, unique local addresses
    return (b[0] & 0xfe) != 0xfc;
  }

  /** A URL the validator doesn't fetch. */
  public static class UnsafeUrlException extends IOException {
    UnsafeUrlException(String message) {
      super(message);
    }
  }
}
