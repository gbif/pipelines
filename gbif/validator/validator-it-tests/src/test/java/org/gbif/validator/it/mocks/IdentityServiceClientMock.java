package org.gbif.validator.it.mocks;

import org.gbif.api.model.common.GbifUser;
import org.gbif.ws.remoteauth.IdentityServiceClient;
import org.gbif.ws.remoteauth.LoggedUser;
import org.gbif.ws.remoteauth.UserAdmin;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class IdentityServiceClientMock implements IdentityServiceClient {

  private final LoggedUser testUser;
  private final String testCredentials;

  @Override
  public UserAdmin getUserAdmin(String s) {
    return new UserAdmin(testUser.toGbifUser(), false);
  }

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
