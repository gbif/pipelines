package org.gbif.validator.persistence.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.SneakyThrows;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.postgresql.util.PGobject;

/** Generic TypeHandler for Java types mapped to json data. */
public abstract class JsonTypeHandler<T> extends BaseTypeHandler<T> {

  private static final ObjectMapper MAPPER =
      JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport()
          .registerModule(new JavaTimeModule());

  private final Class<T> clazz;

  public JsonTypeHandler(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  @SneakyThrows
  public void setNonNullParameter(PreparedStatement ps, int i, T parameter, JdbcType jdbcType) {
    if (ps != null) {
      PGobject ext = new PGobject();
      ext.setType("json");
      ext.setValue(parameter != null ? MAPPER.writeValueAsString(parameter) : null);
      ps.setObject(i, ext);
    }
  }

  @Override
  public T getNullableResult(ResultSet resultSet, String s) throws SQLException {
    return readValue(resultSet.getString(s));
  }

  @Override
  public T getNullableResult(ResultSet resultSet, int i) throws SQLException {
    return readValue(resultSet.getString(i));
  }

  @Override
  public T getNullableResult(CallableStatement callableStatement, int i) throws SQLException {
    return readValue(callableStatement.getString(i));
  }

  /** Converts any nullable string into the target type. */
  @SneakyThrows
  private T readValue(String value) {
    if (value != null) {
      return MAPPER.readValue(value, clazz);
    }
    return null;
  }
}
