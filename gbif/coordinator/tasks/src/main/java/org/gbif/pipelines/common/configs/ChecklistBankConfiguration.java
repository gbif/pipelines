package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class ChecklistBankConfiguration {

  @Parameter(names = "--clb-api-url")
  public String url;

  @Parameter(names = "--clb-api-user")
  public String user;

  @Parameter(names = "--clb-api-password")
  public String password;

  @Parameter(names = "--clb-callback-url")
  public String callbackUrl;

  /** Signs the callback URL, must match validation.clbCallback.secret of the validator ws. */
  @Parameter(names = "--clb-callback-secret")
  public String callbackSecret;

  public ChecklistBankConfiguration(String url, String user, String password, String callbackUrl) {
    this(url, user, password, callbackUrl, null);
  }
}
