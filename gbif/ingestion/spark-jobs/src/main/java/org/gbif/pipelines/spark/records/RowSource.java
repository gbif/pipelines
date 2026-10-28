package org.gbif.pipelines.spark.records;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

/**
 * Builds the source of a document, as {@link RecordsConverter} reads it, straight from its Spark
 * row. It gives the same map as writing the row with {@code to_json} and parsing it with Jackson,
 * without the JSON in between:
 *
 * <ul>
 *   <li>null fields of a struct are left out, null elements of arrays and maps kept
 *   <li>integral numbers are an Integer when they fit, a Long otherwise
 *   <li>floats, doubles and decimals are a Double, read from their JSON text
 * </ul>
 *
 * <p>Only the types listed in {@link #supports(DataType)} are converted, a schema with any other
 * (timestamps, dates, binary) is written as JSON instead, as their JSON text depends on the
 * session.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class RowSource {

  /** Whether the documents of the schema can be converted without JSON */
  static boolean supports(DataType type) {
    if (type instanceof StructType struct) {
      for (StructField field : struct.fields()) {
        if (!supports(field.dataType())) {
          return false;
        }
      }
      return true;
    }
    if (type instanceof ArrayType array) {
      return supports(array.elementType());
    }
    if (type instanceof MapType map) {
      return map.keyType() instanceof StringType && supports(map.valueType());
    }
    return type instanceof StringType
        || type instanceof BooleanType
        || type instanceof ByteType
        || type instanceof ShortType
        || type instanceof IntegerType
        || type instanceof LongType
        || type instanceof FloatType
        || type instanceof DoubleType
        || type instanceof DecimalType;
  }

  /** The document as a mutable map, the converter removes the excluded fields from it */
  static Map<String, Object> toSource(Row row) {
    StructType schema = row.schema();
    Map<String, Object> source = new LinkedHashMap<>();
    for (int i = 0; i < schema.length(); i++) {
      if (!row.isNullAt(i)) {
        source.put(schema.fields()[i].name(), value(row.get(i)));
      }
    }
    return source;
  }

  private static Object value(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Row row) {
      return toSource(row);
    }
    if (value instanceof scala.collection.Seq<?> seq) {
      List<Object> list = new ArrayList<>(seq.size());
      JavaConverters.seqAsJavaList(seq).forEach(v -> list.add(value(v)));
      return list;
    }
    if (value instanceof scala.collection.Map<?, ?> map) {
      Map<String, Object> result = new LinkedHashMap<>();
      JavaConverters.mapAsJavaMap(map).forEach((k, v) -> result.put(k.toString(), value(v)));
      return result;
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
      return ((Number) value).intValue();
    }
    if (value instanceof Long l) {
      return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE ? (Object) l.intValue() : l;
    }
    if (value instanceof Float f) {
      return number(Float.toString(f));
    }
    if (value instanceof Double d) {
      return number(Double.toString(d));
    }
    if (value instanceof BigDecimal d) {
      return number(d.toString());
    }
    // strings and booleans
    return value;
  }

  /** A number as Jackson reads it from its JSON text */
  private static Object number(String text) {
    if (text.equals("NaN") || text.equals("Infinity") || text.equals("-Infinity")) {
      // quoted in the JSON
      return text;
    }
    if (text.indexOf('.') >= 0 || text.indexOf('e') >= 0 || text.indexOf('E') >= 0) {
      return Double.valueOf(text);
    }
    BigInteger integer = new BigInteger(text);
    if (integer.bitLength() < 32) {
      return integer.intValue();
    }
    return integer.bitLength() < 64 ? (Object) integer.longValue() : integer;
  }
}
