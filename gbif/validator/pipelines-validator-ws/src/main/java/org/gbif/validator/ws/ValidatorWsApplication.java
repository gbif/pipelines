package org.gbif.validator.ws;

import org.gbif.registry.identity.service.UserSuretyDelegateImpl;
import org.gbif.registry.persistence.config.MyBatisConfiguration;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.elasticsearch.ElasticSearchRestHealthContributorAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication(exclude = {ElasticSearchRestHealthContributorAutoConfiguration.class})
@EnableConfigurationProperties
@ComponentScan(
    basePackages = {
      "org.gbif.ws.server.interceptor",
      "org.gbif.ws.server.aspect",
      "org.gbif.ws.server.filter",
      "org.gbif.ws.server.advice",
      "org.gbif.ws.server.mapper",
      "org.gbif.registry.persistence",
      "org.gbif.validator.service",
      "org.gbif.validator.ws",
      "org.gbif.ws.security",
      "org.gbif.registry.identity",
      "org.gbif.registry.surety"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {UserSuretyDelegateImpl.class, MyBatisConfiguration.class})
    })
@EnableFeignClients
@MapperScan("org.gbif.validation.persistence.mapper")
public class ValidatorWsApplication {

  public static void main(String[] args) {
    System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
    SpringApplication.run(ValidatorWsApplication.class, args);
  }
}
