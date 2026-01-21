package org.gbif.validator.ws.it;

import com.zaxxer.hikari.HikariDataSource;
import java.util.Collections;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.mail.validator.ValidatorEmailService;
import org.gbif.validator.it.EmbeddedPostgresServer;
import org.gbif.validator.it.mocks.IdentityServiceClientMock;
import org.gbif.validator.it.mocks.MessagePublisherMock;
import org.gbif.validator.it.mocks.RemoteAuthClientMock;
import org.gbif.validator.ws.config.ValidatorWsConfiguration;
import org.gbif.validator.ws.file.DownloadFileManager;
import org.gbif.validator.ws.security.RegistrySecurityConfiguration;
import org.gbif.ws.remoteauth.IdentityServiceClient;
import org.gbif.ws.remoteauth.LoggedUser;
import org.gbif.ws.remoteauth.RemoteAuthClient;
import org.gbif.ws.remoteauth.RemoteAuthWebSecurityConfigurer;
import org.gbif.ws.security.AppKeySigningService;
import org.gbif.ws.security.FileSystemKeyStore;
import org.gbif.ws.security.GbifAuthenticationManagerImpl;
import org.gbif.ws.server.filter.AppIdentityFilter;
import org.gbif.ws.server.filter.IdentityFilter;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.test.context.ActiveProfiles;

@TestConfiguration
@SpringBootApplication(exclude = {LiquibaseAutoConfiguration.class, FlywayAutoConfiguration.class})
@EnableAsync
@PropertySource("classpath:application-test.yml")
@ComponentScan(
    basePackages = {
      "org.gbif.ws.server.interceptor",
      "org.gbif.ws.server.aspect",
      "org.gbif.ws.server.filter",
      "org.gbif.ws.server.advice",
      "org.gbif.ws.server.mapper",
      "org.gbif.ws.remoteauth",
      "org.gbif.ws.security",
      "org.gbif.validator.service",
      "org.gbif.validator.ws.resource",
      "org.gbif.validator.ws.config"
    },
    excludeFilters = {
      @ComponentScan.Filter(
          type = FilterType.ASSIGNABLE_TYPE,
          classes = {
            ValidatorWsConfiguration.class,
            RegistrySecurityConfiguration.class,
            AppKeySigningService.class,
            FileSystemKeyStore.class,
            IdentityFilter.class,
            AppIdentityFilter.class,
            GbifAuthenticationManagerImpl.class
          })
    })
@ActiveProfiles("test")
public class ValidatorWsItConfiguration extends ValidatorWsConfiguration {

  public static final String LIQUIBASE_MASTER_FILE = "org/gbif/validator/liquibase/master.xml";

  public static final LoggedUser TEST_USER =
      LoggedUser.builder()
          .userName("admin")
          .email("nothing@gbif.org")
          .roles(Collections.singleton(UserRole.USER.name()))
          .build();
  public static final String TEST_USER_PASSWORD = "hi";

  /**
   * Created from here to avoid scanning this class package in which other test configuration are
   * located.
   */
  @Bean
  public DownloadFileManager downloadFileManager() {
    return new DownloadFileManager();
  }

  @Bean
  public MessagePublisher messagePublisher() {
    return new MessagePublisherMock();
  }

  @Bean
  public ValidatorEmailService emailService() {
    return Mockito.mock(ValidatorEmailService.class);
  }

  @Bean
  public IdentityServiceClient identityAccessServiceClient() {
    return IdentityServiceClientMock.builder().testUser(TEST_USER).build();
  }

  @Bean
  public RemoteAuthClient remoteAuthClient() {
    return RemoteAuthClientMock.builder().testUser(TEST_USER).build();
  }

  @Bean
  public EmbeddedPostgresServer embeddedPostgresServer() {
    return new EmbeddedPostgresServer(LIQUIBASE_MASTER_FILE);
  }

  @Bean(name = "validationDataSourceProperties")
  @Primary
  @ConditionalOnProperty(name = "testdb", havingValue = "true")
  public DataSourceProperties dataSourceProperties(EmbeddedPostgresServer embeddedPostgresServer) {
    return embeddedPostgresServer.dataSourceProperties();
  }

  @Bean(name = "validationDataSource")
  @Primary
  @ConfigurationProperties("validation.datasource.hikari")
  @ConditionalOnProperty(name = "testdb", havingValue = "true")
  public HikariDataSource dataSource(DataSourceProperties dataSourceProperties) {
    return dataSourceProperties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
  }

  @Configuration
  public class SecurityConfiguration extends RemoteAuthWebSecurityConfigurer {}
}
