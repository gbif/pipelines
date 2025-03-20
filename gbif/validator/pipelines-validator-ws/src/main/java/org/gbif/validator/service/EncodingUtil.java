package org.gbif.validator.service;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/** Url encoding class. */
@UtilityClass
@Slf4j
public class EncodingUtil {

  /** Encodes an URL, specially URLs with blank spaces can be problematics. */
  static String encode(String rawUrl) {
    try {
      String decodedURL = URLDecoder.decode(rawUrl, StandardCharsets.UTF_8);
      URL url = new URL(decodedURL);
      URI uri =
          new URI(
              url.getProtocol(),
              url.getUserInfo(),
              url.getHost(),
              url.getPort(),
              url.getPath(),
              url.getQuery(),
              url.getRef());
      return uri.toURL().toString();
    } catch (Exception ex) {
      log.error("URL encoding error", ex);
      throw new IllegalArgumentException(ex);
    }
  }

  static Optional<String> getRedirectedUrl(String url) {
    try {
      HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
      con.setRequestMethod("HEAD");
      String redirect = con.getHeaderField("Location");
      if (redirect != null && !redirect.isEmpty()) {
        return Optional.of(redirect);
      }
      return Optional.empty();
    } catch (Exception ex) {
      log.error("URL redirection error", ex);
      throw new IllegalArgumentException(ex);
    }
  }
}
