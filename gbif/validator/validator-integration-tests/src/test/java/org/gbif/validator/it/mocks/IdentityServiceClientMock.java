package org.gbif.validator.it.mocks;

import lombok.Builder;
import lombok.Data;
import org.gbif.api.model.common.GbifUser;
import org.gbif.ws.remoteauth.IdentityServiceClient;
import org.gbif.ws.remoteauth.LoggedUser;

@Data
@Builder
public class IdentityServiceClientMock implements IdentityServiceClient {

  private final LoggedUser testUser;
  private final String testCredentials;

  @Override
  public GbifUser get(String userName) {
    return testUser.toGbifUser();
  }

  @Override
  public LoggedUser login(String credentials) {
    return testUser;
  }

  @Override
  public GbifUser authenticate(String userName, String password) {
    return testUser.toGbifUser();
  }
}
