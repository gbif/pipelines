package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;

@ToString
@Getter
@Setter
public class AirflowConfiguration {

  @Parameter(names = "--dag-name")
  public String dagName;

  @Parameter(names = "--user")
  public String user;

  @Parameter(names = "--pass")
  public String pass;

  @Parameter(names = "--address")
  public String address;

  @Parameter(names = "--api-check-delay-sec")
  public int apiCheckDelaySec = 5;

  @JsonIgnore
  public String getBasicAuthString() {
    String stringToEncode = user + ":" + pass;
    return Base64.getEncoder().encodeToString(stringToEncode.getBytes(StandardCharsets.UTF_8));
  }

  public Header[] getHeaders() {
    return new Header[] {
      new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + getBasicAuthString()),
      new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())
    };
  }
}
