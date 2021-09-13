package org.gbif.validator.ws.security;

import com.zaxxer.hikari.HikariDataSource;
import java.net.URI;
import java.util.Locale;
import java.util.UUID;
import lombok.SneakyThrows;
import org.gbif.api.model.common.DOI;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Language;
import org.gbif.mybatis.type.CountryTypeHandler;
import org.gbif.mybatis.type.LanguageTypeHandler;
import org.gbif.mybatis.type.UriTypeHandler;
import org.gbif.mybatis.type.UuidTypeHandler;
import org.gbif.registry.identity.service.BasicUserSuretyDelegate;
import org.gbif.registry.identity.service.UserSuretyDelegate;
import org.gbif.registry.identity.util.RegistryPasswordEncoder;
import org.gbif.registry.persistence.mapper.CommentMapper;
import org.gbif.registry.persistence.mapper.ContactMapper;
import org.gbif.registry.persistence.mapper.DatasetMapper;
import org.gbif.registry.persistence.mapper.EndpointMapper;
import org.gbif.registry.persistence.mapper.IdentifierMapper;
import org.gbif.registry.persistence.mapper.InstallationMapper;
import org.gbif.registry.persistence.mapper.MachineTagMapper;
import org.gbif.registry.persistence.mapper.OrganizationMapper;
import org.gbif.registry.persistence.mapper.TagMapper;
import org.gbif.registry.persistence.mapper.UserMapper;
import org.gbif.registry.persistence.mapper.handler.DOITypeHandler;
import org.gbif.registry.persistence.mapper.handler.LocaleTypeHandler;
import org.gbif.registry.persistence.mapper.surety.ChallengeCodeMapper;
import org.gbif.registry.security.LegacyAuthorizationService;
import org.gbif.registry.security.LegacyAuthorizationServiceImpl;
import org.gbif.registry.security.RegistryUserDetailsService;
import org.gbif.registry.surety.ChallengeCodeManager;
import org.gbif.registry.ws.client.InstallationClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.security.NoAuthWebSecurityConfigurer;
import org.gbif.ws.server.filter.IdentityFilter;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.mapper.MapperFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * Configuration for all data sources, MyBatis mappers and services required by the Registry
 * security modules.
 */
@Configuration
public class RegistrySecurityConfiguration {

  @Bean(name = "registryDatasourceProperties")
  @ConfigurationProperties("registry.datasource")
  public DataSourceProperties registryDataSourceProperties() {
    return new DataSourceProperties();
  }

  @Bean(name = "registryDataSource")
  @ConfigurationProperties("registry.datasource.hikari")
  public HikariDataSource registryDataSource() {
    return registryDataSourceProperties()
        .initializeDataSourceBuilder()
        .type(HikariDataSource.class)
        .build();
  }

  @Bean(name = "registrySqlSessionFactory")
  public SqlSessionFactoryBean registryServiceSqlSessionFactory(
      @Qualifier("registryDataSource") HikariDataSource dataSource) {
    SqlSessionFactoryBean sqlSession = new SqlSessionFactoryBean();
    sqlSession.setDataSource(dataSource);
    // Configure the location of the Mybatis XML file
    org.apache.ibatis.session.Configuration configuration =
        new org.apache.ibatis.session.Configuration();
    configuration.getTypeHandlerRegistry().register(UUID.class, UuidTypeHandler.class);
    configuration.getTypeHandlerRegistry().register(URI.class, UriTypeHandler.class);
    configuration.getTypeHandlerRegistry().register(DOI.class, DOITypeHandler.class);
    configuration.getTypeHandlerRegistry().register(Country.class, CountryTypeHandler.class);
    configuration.getTypeHandlerRegistry().register(Language.class, LanguageTypeHandler.class);
    configuration.getTypeHandlerRegistry().register(Locale.class, LocaleTypeHandler.class);
    configuration.getTypeHandlerRegistry().register("org.gbif.registry.persistence.handler");
    configuration.setMapUnderscoreToCamelCase(true);
    sqlSession.setConfiguration(configuration);
    return sqlSession;
  }

  @SneakyThrows
  private static <T> MapperFactoryBean<T> registerMapper(
      SqlSessionFactoryBean sqlSessionFactoryBean, Class<T> clazz) {
    MapperFactoryBean<T> factoryBean = new MapperFactoryBean<>(clazz);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }

  @Bean
  public MapperFactoryBean<UserMapper> userMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, UserMapper.class);
  }

  @Bean
  public MapperFactoryBean<ChallengeCodeMapper> challengeCodeMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, ChallengeCodeMapper.class);
  }

  @Bean
  public MapperFactoryBean<OrganizationMapper> organizationMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, OrganizationMapper.class);
  }

  @Bean
  public MapperFactoryBean<TagMapper> tagMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, TagMapper.class);
  }

  @Bean
  public MapperFactoryBean<ContactMapper> contactMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, ContactMapper.class);
  }

  @Bean
  public MapperFactoryBean<IdentifierMapper> identifierMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, IdentifierMapper.class);
  }

  @Bean
  public MapperFactoryBean<EndpointMapper> endpointMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, EndpointMapper.class);
  }

  @Bean
  public MapperFactoryBean<MachineTagMapper> machineTagMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, MachineTagMapper.class);
  }

  @Bean
  public MapperFactoryBean<CommentMapper> commentMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, CommentMapper.class);
  }

  @Bean
  public MapperFactoryBean<DatasetMapper> datasetMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, DatasetMapper.class);
  }

  @Bean
  public MapperFactoryBean<InstallationMapper> installationMapper(
      @Qualifier("registrySqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean) {
    return registerMapper(sqlSessionFactoryBean, InstallationMapper.class);
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
  public LegacyAuthorizationService legacyAuthorizationService(
      OrganizationMapper organizationMapper,
      DatasetMapper datasetMapper,
      InstallationMapper installationMapper) {
    return new LegacyAuthorizationServiceImpl(
        organizationMapper, datasetMapper, installationMapper);
  }

  @Bean
  public InstallationService installationService(@Value("${registry.ws.url}") String apiUrl) {
    ClientBuilder clientBuilder = new ClientBuilder();
    return clientBuilder.withUrl(apiUrl).build(InstallationClient.class);
  }

  @Bean
  public OrganizationService organizationService(@Value("${registry.ws.url}") String apiUrl) {
    ClientBuilder clientBuilder = new ClientBuilder();
    return clientBuilder.withUrl(apiUrl).build(OrganizationService.class);
  }

  @Configuration
  @EnableWebSecurity
  public static class ValidatorWebSecurity extends NoAuthWebSecurityConfigurer {

    private final InstallationIdentityFilter installationIdentityFilter;

    @SneakyThrows
    public ValidatorWebSecurity(
        UserDetailsService userDetailsService,
        ApplicationContext context,
        PasswordEncoder passwordEncoder,
        InstallationIdentityFilter installationIdentityFilter) {
      super(userDetailsService, context, passwordEncoder);
      this.installationIdentityFilter = installationIdentityFilter;
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
      super.configure(http);
      http.addFilterAfter(installationIdentityFilter, IdentityFilter.class);
    }
  }
}
