package org.gbif.validator.it.mocks;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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

  @SneakyThrows
  @Override
  public ResponseEntity<String> remoteAuth(String s, HttpHeaders httpHeaders) {
    return ResponseEntity.ok(OBJECT_MAPPER.writeValueAsString(testUser));
  }
}
