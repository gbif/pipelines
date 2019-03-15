package org.gbif.pipeleins.hbase;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Date;
import java.util.UUID;

import org.gbif.api.util.VocabularyUtils;
import org.gbif.dwc.terms.Term;
import org.gbif.hbase.util.ResultReader;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Strings;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A convenience class for making things easier when reading the fields of an HBase result from the occurrence table.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExtResultReader {

  //Validation messages
  private static final String ROW_CAN_T_BE_NULL_MSG = "row can't be null";
  private static final String COLUMN_CAN_T_BE_NULL_MSG = "column can't be null";

  private static String CF = Columns.OCCURRENCE_COLUMN_FAMILY;

  public static int getKey(Result row) {
    return Bytes.toInt(row.getRow());
  }

  public static String getString(Result row, String column) {
    return getString(row, column, null);
  }

  public static String getString(Result row, Term column) {
    return getString(row, Columns.column(column), null);
  }

  public static String getString(Result row, String column, @Nullable String defaultValue) {
    checkNotNull(row, ROW_CAN_T_BE_NULL_MSG);
    checkNotNull(column, COLUMN_CAN_T_BE_NULL_MSG);
    return ResultReader.getString(row, CF, column, defaultValue);
  }

  public static UUID getUuid(Result row, String column) {
    String uuid = getString(row, column);
    return uuid == null ? null : UUID.fromString(uuid);
  }

  public static UUID getUuid(Result row, Term column) {
    String uuid = getString(row, Columns.column(column));
    return uuid == null ? null : UUID.fromString(uuid);
  }

  public static Double getDouble(Result row, String column) {
    return getDouble(row, column, null);
  }

  public static Double getDouble(Result row, Term column) {
    return getDouble(row, Columns.column(column), null);
  }

  public static Double getDouble(Result row, String column, @Nullable Double defaultValue) {
    checkNotNull(row, ROW_CAN_T_BE_NULL_MSG);
    checkNotNull(column, COLUMN_CAN_T_BE_NULL_MSG);
    return ResultReader.getDouble(row, CF, column, defaultValue);
  }

  public static Integer getInteger(Result row, String column) {
    return getInteger(row, column, null);
  }

  public static Integer getInteger(Result row, Term column) {
    return getInteger(row, Columns.column(column), null);
  }

  public static Integer getInteger(Result row, String column, @Nullable Integer defaultValue) {
    checkNotNull(row, ROW_CAN_T_BE_NULL_MSG);
    checkNotNull(column, COLUMN_CAN_T_BE_NULL_MSG);
    return ResultReader.getInteger(row, CF, column, defaultValue);
  }

  public static Date getDate(Result row, String column) {
    Long time = getLong(row, column);
    return time == null ? null : new Date(time);
  }

  public static Date getDate(Result row, Term column) {
    return getDate(row, Columns.column(column));
  }

  public static Long getLong(Result row, String column) {
    return getLong(row, column, null);
  }

  public static Long getLong(Result row, Term column) {
    return getLong(row, Columns.column(column), null);
  }

  public static Long getLong(Result row, String column, @Nullable Long defaultValue) {
    checkNotNull(row, ROW_CAN_T_BE_NULL_MSG);
    checkNotNull(column, COLUMN_CAN_T_BE_NULL_MSG);
    return ResultReader.getLong(row, CF, column, defaultValue);
  }

  public static byte[] getBytes(Result row, Term column) {
    return getBytes(row, Columns.column(column), null);
  }

  public static byte[] getBytes(Result row, String column) {
    return getBytes(row, column, null);
  }

  public static byte[] getBytes(Result row, String column, @Nullable byte[] defaultValue) {
    checkNotNull(row, ROW_CAN_T_BE_NULL_MSG);
    checkNotNull(column, COLUMN_CAN_T_BE_NULL_MSG);
    return ResultReader.getBytes(row, CF, column, defaultValue);
  }

  public static <T extends Enum<?>> T getEnum(Result row, Term column, Class<T> enumClass) {
    String value = getString(row, Columns.column(column), null);
    if (!Strings.isNullOrEmpty(value)) {
      try {
        return (T) VocabularyUtils.lookupEnum(value, enumClass);
      } catch (IllegalArgumentException e) {
        // value not matching enum!!! LOG???
      }
    }
    return null;
  }

  public static URI getUri(Result row, Term column) {
    String uri = getString(row, Columns.column(column));
    try {
      return uri == null ? null : URI.create(uri);
    } catch (Exception e) {
    }
    return null;
  }

  public static BigDecimal getBigDecimal(Result row, Term column) {
    byte[] content = getBytes(row, column);
    if (content != null) {
      return Bytes.toBigDecimal(content);
    }
    return null;
  }
}
