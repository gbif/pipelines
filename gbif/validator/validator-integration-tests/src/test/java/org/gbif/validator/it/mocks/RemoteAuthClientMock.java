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

  /** Authenticated when its user name is sent in a Basic header. */
  private final LoggedUser adminUser;

  /** Authenticated when its user name is sent in a Basic header. */
  private final LoggedUser otherAdminUser;

  @SneakyThrows
  @Override
  public ResponseEntity<String> remoteAuth(String s, HttpHeaders httpHeaders) {
    String userName = basicUserName(httpHeaders);
    LoggedUser user = testUser;
    if (otherUser != null && otherUser.getUserName().equals(userName)) {
      user = otherUser;
    } else if (adminUser != null && adminUser.getUserName().equals(userName)) {
      user = adminUser;
    } else if (otherAdminUser != null && otherAdminUser.getUserName().equals(userName)) {
      user = otherAdminUser;
    }
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
