package org.gbif.validator.it;

import java.sql.Connection;
import java.util.Properties;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.SneakyThrows;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.testcontainers.containers.PostgreSQLContainer;

public class EmbeddedPostgresServer implements InitializingBean, DisposableBean {

  private PostgreSQLContainer postgreSQLContainer;
  private final String liquibaseChangeLog;

  public EmbeddedPostgresServer(String liquibaseChangeLog) {
    this.liquibaseChangeLog = liquibaseChangeLog;
  }

  @Override
  public void destroy() {
    if (postgreSQLContainer != null) {
      postgreSQLContainer.stop();
    }
  }

  @Override
  public void afterPropertiesSet() {
    postgreSQLContainer = new PostgreSQLContainer("postgres:" + getPostgresVersion());
    postgreSQLContainer.withReuse(true);
    postgreSQLContainer.start();
    runLiquibase(liquibaseChangeLog);
  }

  @SneakyThrows
  private static String getPostgresVersion() {
    Properties properties = new Properties();
    properties.load(
        EmbeddedPostgresServer.class.getClassLoader().getResourceAsStream("maven.properties"));
    return properties.getProperty("postgres.version");
  }

  @SneakyThrows
  private void runLiquibase(String changeLogPath) {
    Connection connection = postgreSQLContainer.createConnection("");
    Database database =
        DatabaseFactory.getInstance()
            .findCorrectDatabaseImplementation(new JdbcConnection(connection));
    Liquibase liquibase = new Liquibase(changeLogPath, new ClassLoaderResourceAccessor(), database);
    liquibase.update(new Contexts(), new LabelExpression());
  }

  /** Creates the registry datasource settings from the embedded database. */
  public DataSourceProperties dataSourceProperties() {
    DataSourceProperties dataSourceProperties = new DataSourceProperties();
    dataSourceProperties.setPassword(postgreSQLContainer.getPassword());
    dataSourceProperties.setUsername(postgreSQLContainer.getUsername());
    dataSourceProperties.setUrl(postgreSQLContainer.getJdbcUrl());
    return dataSourceProperties;
  }
}
