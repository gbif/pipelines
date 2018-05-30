package org.gbif.pipelines.esindexing.client;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class EsConfig {

  private static final int DEFAULT_PORT = 9200;
  private static final String DEFAULT_PROTOCOL = "http";

  private final List<EsHost> hosts;

  private EsConfig(Builder builder) {
    Objects.requireNonNull(builder.hosts);
    Preconditions.checkArgument(!builder.hosts.isEmpty());
    hosts = builder.hosts;
  }

  private EsConfig(List<String> hostsAddresses) {
    Objects.requireNonNull(hostsAddresses);
    Preconditions.checkArgument(!hostsAddresses.isEmpty());

    hosts = new ArrayList<>();
    hostsAddresses.forEach(address -> {
      try {
        URL url = new URL(address);
        hosts.add(new EsHost(url.getHost(), url.getPort(), url.getProtocol()));
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(address + " is not a valid url", e);
      }
    });
  }

  // TODO: cambiar la list por array
  public static EsConfig fromHostAddresses(List<String> hostsAddresses) {
    return new EsConfig(hostsAddresses);
  }

  public static Builder builder() {
    return new Builder();
  }

  public List<EsHost> getHosts() {
    return hosts;
  }

  public static class EsHost {

    private final String hostname;
    private final Integer port;
    private final String protocol;

    private EsHost(Builder.HostBuilder builder) {
      hostname = builder.hostname;
      port = builder.port;
      protocol = builder.protocol;
    }

    private EsHost(String hostname, Integer port, String protocol) {
      this.hostname = hostname;
      this.port = port;
      this.protocol = protocol;
    }

    public String getHostname() {
      return hostname;
    }

    public Integer getPort() {
      return Objects.isNull(port) ? DEFAULT_PORT : port;
    }

    public String getProtocol() {
      return Strings.isNullOrEmpty(protocol) ? DEFAULT_PROTOCOL : protocol;
    }
  }

  public static class Builder {

    List<EsHost> hosts;

    public HostBuilder addHost() {
      return new HostBuilder(this);
    }

    public EsConfig build() {
      return new EsConfig(this);
    }

    public class HostBuilder {

      private String hostname;
      private int port;
      private String protocol;
      private Builder builder;

      private HostBuilder(Builder builder) {
        this.builder = builder;
      }

      public HostBuilder withHostname(String hostname) {
        this.hostname = hostname;
        return this;
      }

      public HostBuilder withPort(int port) {
        this.port = port;
        return this;
      }

      public HostBuilder withProtocol(String protocol) {
        this.protocol = protocol;
        return this;
      }

      public Builder done() {
        Objects.requireNonNull(hostname);
        Preconditions.checkArgument(!hostname.isEmpty());

        builder.hosts.add(new EsHost(this));
        return builder;
      }
    }

  }

}
