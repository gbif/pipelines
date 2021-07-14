/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.validator.ws.config;

import com.zaxxer.hikari.HikariDataSource;
import java.util.UUID;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.mybatis.type.UuidTypeHandler;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.persistence.mapper.MetricsJsonTypeHandler;
import org.gbif.validator.persistence.mapper.ValidationMapper;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.mapper.MapperFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/** Validation data source and MyBatis configuration. */
@Configuration
public class ValidatorDataSourceConfiguration {

  @Bean(name = "validationDataSourceProperties")
  @Primary
  @ConfigurationProperties("validation.datasource")
  @ConditionalOnProperty(name = "testdb", havingValue = "false")
  public DataSourceProperties dataSourceProperties() {
    return new DataSourceProperties();
  }

  @Bean(name = "validationDataSource")
  @Primary
  @ConfigurationProperties("validation.datasource.hikari")
  @ConditionalOnProperty(name = "testdb", havingValue = "false")
  public HikariDataSource dataSource() {
    return dataSourceProperties()
        .initializeDataSourceBuilder()
        .type(HikariDataSource.class)
        .build();
  }

  @Bean(name = "validationSqlSessionFactory")
  @Primary
  public SqlSessionFactoryBean validationSqlSessionFactory(
      @Qualifier("validationDataSource") HikariDataSource dataSource) {
    SqlSessionFactoryBean sqlSession = new SqlSessionFactoryBean();
    sqlSession.setDataSource(dataSource);
    org.apache.ibatis.session.Configuration configuration =
        new org.apache.ibatis.session.Configuration();
    configuration.setMapUnderscoreToCamelCase(true);

    // Type handlers
    configuration.getTypeHandlerRegistry().register(UUID.class, UuidTypeHandler.class);
    configuration.getTypeAliasRegistry().registerAlias("UuidTypeHandler", UuidTypeHandler.class);

    configuration.getTypeHandlerRegistry().register(Metrics.class, MetricsJsonTypeHandler.class);
    configuration
        .getTypeAliasRegistry()
        .registerAlias("MetricsJsonTypeHandler", MetricsJsonTypeHandler.class);

    // Type aliases
    configuration.getTypeAliasRegistry().registerAlias("Validation", Validation.class);
    configuration.getTypeAliasRegistry().registerAlias("Pageable", Pageable.class);
    configuration.getTypeAliasRegistry().registerAlias("UUID", UUID.class);

    sqlSession.setConfiguration(configuration);

    return sqlSession;
  }

  @Bean
  public MapperFactoryBean<ValidationMapper> validationMapper(
      @Qualifier("validationSqlSessionFactory") SqlSessionFactoryBean sqlSessionFactoryBean)
      throws Exception {
    MapperFactoryBean<ValidationMapper> factoryBean =
        new MapperFactoryBean<>(ValidationMapper.class);
    factoryBean.setSqlSessionFactory(sqlSessionFactoryBean.getObject());
    return factoryBean;
  }
}
