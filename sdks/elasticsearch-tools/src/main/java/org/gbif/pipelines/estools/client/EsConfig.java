package org.gbif.pipelines.estools.client;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;

/** ES configuration. */
public class EsConfig {

  private final List<URL> hosts;
  private final String[] rawHosts;
  private final String username;
  private final String password;

  private EsConfig(@NonNull String[] hostsAddresses, String username, String password) {
    this.username = username;
    this.password = password;
    this.rawHosts = hostsAddresses;
    this.hosts =
        Arrays.stream(hostsAddresses)
            .map(
                address -> {
                  try {
                    return new URL(address);
                  } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(address + " is not a valid url", e);
                  }
                })
            .collect(Collectors.toList());
  }

  /**
   * Creates a {@link EsConfig} from the addresses received.
   *
   * @param hostsAddresses they should be valid URLs.
   * @return {@link EsConfig}.
   */
  public static EsConfig from(String... hostsAddresses) {
    return new EsConfig(hostsAddresses, null, null);
  }

  public static EsConfig from(String username, String password, String... hostsAddresses) {
    return new EsConfig(hostsAddresses, username, password);
  }

  public List<URL> getHosts() {
    return hosts;
  }

  public String[] getRawHosts() {
    return rawHosts;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
