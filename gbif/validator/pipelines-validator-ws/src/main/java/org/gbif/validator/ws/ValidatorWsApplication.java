package org.gbif.validator.ws;

import org.gbif.registry.persistence.config.MyBatisConfiguration;
import org.gbif.ws.security.AppKeySigningService;
import org.gbif.ws.security.FileSystemKeyStore;
import org.gbif.ws.security.GbifAuthenticationManagerImpl;
import org.gbif.ws.server.filter.AppIdentityFilter;
import org.gbif.ws.server.filter.IdentityFilter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class})
@EnableConfigurationProperties
@ComponentScan(
    basePackages = {
      "org.gbif.ws.server.interceptor",
      "org.gbif.ws.server.aspect",
      "org.gbif.ws.server.filter",
      "org.gbif.ws.server.advice",
      "org.gbif.ws.server.mapper",
      "org.gbif.validator.service",
      "org.gbif.mail",
      "org.gbif.validator.ws",
      "org.gbif.ws.remoteauth",
      "org.gbif.ws.security"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {
            MyBatisConfiguration.class,
            AppKeySigningService.class,
            FileSystemKeyStore.class,
            IdentityFilter.class,
            AppIdentityFilter.class,
            GbifAuthenticationManagerImpl.class
          })
    })
@EnableScheduling
public class ValidatorWsApplication {

  public static void main(String[] args) {
    System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
    SpringApplication.run(ValidatorWsApplication.class, args);
  }
}
