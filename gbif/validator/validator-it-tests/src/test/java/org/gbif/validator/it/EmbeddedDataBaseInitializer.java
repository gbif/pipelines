package org.gbif.validator.it;

import io.zonky.test.db.postgres.embedded.ConnectionInfo;
import io.zonky.test.db.postgres.embedded.DatabasePreparer;
import io.zonky.test.db.postgres.embedded.PreparedDbProvider;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;

public class EmbeddedDataBaseInitializer {
  private final DataSource dataSource;
  private final PreparedDbProvider provider;
  private final ConnectionInfo connectionInfo;

  public EmbeddedDataBaseInitializer(DatabasePreparer preparer) {
    try {
      this.provider = PreparedDbProvider.forPreparer(preparer, new CopyOnWriteArrayList());
      this.connectionInfo = provider.createNewDatabase();
      this.dataSource = provider.createDataSourceFromConnectionInfo(this.connectionInfo);
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public ConnectionInfo getConnectionInfo() {
    return connectionInfo;
  }
}
