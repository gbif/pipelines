package org.gbif.pipelines.esindexing.client;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class EsConfig {

  private static final int DEFAULT_PORT = 9200;
  private static final String DEFAULT_PROTOCOL = "http";

  private final List<URL> hosts;

  private EsConfig(String[] hostsAddresses) {
    Objects.requireNonNull(hostsAddresses);

    hosts = new ArrayList<>();
    Arrays.stream(hostsAddresses).forEach(address -> {
      try {
        hosts.add(new URL(address));
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(address + " is not a valid url", e);
      }
    });
  }

  public static EsConfig from(String... hostsAddresses) {
    return new EsConfig(hostsAddresses);
  }

  public List<URL> getHosts() {
    return hosts;
  }

}
