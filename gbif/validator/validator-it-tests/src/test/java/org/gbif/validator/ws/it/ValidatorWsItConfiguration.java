package org.gbif.validator.ws.it;

import static org.mockito.Mockito.mock;

import com.zaxxer.hikari.HikariDataSource;
import java.util.Collections;
import org.gbif.api.model.common.GbifUser;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.mail.validator.ValidatorEmailService;
import org.gbif.registry.identity.service.BasicUserSuretyDelegate;
import org.gbif.registry.identity.service.UserSuretyDelegate;
import org.gbif.registry.identity.service.UserSuretyDelegateImpl;
import org.gbif.registry.identity.util.RegistryPasswordEncoder;
import org.gbif.registry.persistence.mapper.UserMapper;
import org.gbif.registry.security.RegistryUserDetailsService;
import org.gbif.registry.surety.ChallengeCodeManager;
import org.gbif.registry.surety.OrganizationChallengeCodeManager;
import org.gbif.registry.surety.UserChallengeCodeManager;
import org.gbif.validator.it.EmbeddedPostgresServer;
import org.gbif.validator.it.mocks.ChallengeCodeManagerMock;
import org.gbif.validator.it.mocks.MessagePublisherMock;
import org.gbif.validator.it.mocks.UserMapperMock;
import org.gbif.validator.ws.config.ValidatorWsConfiguration;
import org.gbif.validator.ws.file.DownloadFileManager;
import org.gbif.validator.ws.security.RegistrySecurityConfiguration;
import org.gbif.ws.security.NoAuthWebSecurityConfigurer;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
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
      "org.gbif.ws.security",
      "org.gbif.registry.persistence",
      "org.gbif.registry.identity",
      "org.gbif.registry.surety",
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
            UserSuretyDelegateImpl.class,
            UserChallengeCodeManager.class,
            OrganizationChallengeCodeManager.class
          })
    })
@ActiveProfiles("test")
public class ValidatorWsItConfiguration extends ValidatorWsConfiguration {

  public static final String LIQUIBASE_MASTER_FILE = "org/gbif/validator/liquibase/master.xml";

  public static final GbifUser TEST_USER = new GbifUser();
  public static final String TEST_USER_PASSWORD = "hi";

  public static final RegistryPasswordEncoder PASSWORD_ENCODER = new RegistryPasswordEncoder();

  static {
    TEST_USER.setUserName("admin");
    TEST_USER.setEmail("nothing@gbif.org");
    TEST_USER.setPasswordHash(PASSWORD_ENCODER.encode(TEST_USER_PASSWORD));
    TEST_USER.setRoles(Collections.singleton(UserRole.USER));
  }

  /**
   * Created from here to avoid scanning this class package in which other test configuration are
   * located.
   */
  @Bean
  public DownloadFileManager downloadFileManager() {
    return new DownloadFileManager();
  }

  @Bean
  public UserMapper userMapperMock() {
    UserMapper userMapper = new UserMapperMock();
    userMapper.create(TEST_USER);
    return userMapper;
  }

  @Bean
  public ChallengeCodeManager<Integer> challengeCodeManagerMock() {
    return new ChallengeCodeManagerMock();
  }

  @Bean
  public UserDetailsService userDetailsService(UserMapper userMapper) {
    return new RegistryUserDetailsService(userMapper);
  }

  @Bean
  public PasswordEncoder passwordEncoder() {
    return new RegistryPasswordEncoder();
  }

  @Bean
  public UserSuretyDelegate userSuretyDelegate(ChallengeCodeManager<Integer> challengeCodeManager) {
    return new BasicUserSuretyDelegate(challengeCodeManager);
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
  public InstallationService installationService() {
    InstallationService installationService = mock(InstallationService.class);
    return installationService;
  }

  @Bean
  public OrganizationService organizationService() {
    OrganizationService organizationService = mock(OrganizationService.class);
    return organizationService;
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
  public static class ValidatorWebSecurity extends NoAuthWebSecurityConfigurer {

    public ValidatorWebSecurity(
        UserDetailsService userDetailsService,
        ApplicationContext context,
        PasswordEncoder passwordEncoder) {
      super(userDetailsService, context, passwordEncoder);
    }
  }
}
