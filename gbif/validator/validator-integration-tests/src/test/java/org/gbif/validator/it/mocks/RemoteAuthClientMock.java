package org.gbif.validator.it.mocks;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.gbif.ws.remoteauth.LoggedUser;
import org.gbif.ws.remoteauth.RemoteAuthClient;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;

@Data
@Builder
public class RemoteAuthClientMock implements RemoteAuthClient {

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private final LoggedUser testUser;

  /** Authenticated instead of testUser when its user name is sent in a Basic header. */
  private final LoggedUser otherUser;

  @SneakyThrows
  @Override
  public ResponseEntity<String> remoteAuth(String s, HttpHeaders httpHeaders) {
    LoggedUser user =
        otherUser != null && otherUser.getUserName().equals(basicUserName(httpHeaders))
            ? otherUser
            : testUser;
    return ResponseEntity.ok(OBJECT_MAPPER.writeValueAsString(user));
  }

  private static String basicUserName(HttpHeaders httpHeaders) {
    String authorization = httpHeaders.getFirst(HttpHeaders.AUTHORIZATION);
    if (authorization == null || !authorization.startsWith("Basic ")) {
      return null;
    }
    String credentials =
        new String(
            Base64.getDecoder().decode(authorization.substring("Basic ".length())),
            StandardCharsets.UTF_8);
    return credentials.split(":", 2)[0];
  }
}
