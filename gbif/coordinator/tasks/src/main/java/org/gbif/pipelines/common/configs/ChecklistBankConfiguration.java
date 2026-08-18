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
}
