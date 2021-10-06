package org.gbif.validator.persistence.mapper;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

/** Converts between Set<String> and Postgres Text[] and vice-versa. */
public class StringArraySetTypeHandler extends BaseTypeHandler<Set<String>> {

  @Override
  public void setNonNullParameter(
      PreparedStatement ps, int i, Set<String> parameter, JdbcType jdbcType) throws SQLException {
    Array array = ps.getConnection().createArrayOf("text", parameter.toArray());
    ps.setArray(i, array);
  }

  @Override
  public Set<String> getNullableResult(ResultSet rs, String columnName) throws SQLException {
    return toSet(rs.getArray(columnName));
  }

  @Override
  public Set<String> getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
    return toSet(rs.getArray(columnIndex));
  }

  @Override
  public Set<String> getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
    return toSet(cs.getArray(columnIndex));
  }

  private Set<String> toSet(Array pgArray) throws SQLException {
    if (pgArray == null) {
      return new HashSet<>();
    }

    String[] strings = (String[]) pgArray.getArray();
    return containsOnlyNulls(strings) ? new HashSet<>() : new HashSet<>(Arrays.asList(strings));
  }

  private boolean containsOnlyNulls(String[] strings) {
    return Arrays.stream(strings).noneMatch(Objects::nonNull);
  }
}
