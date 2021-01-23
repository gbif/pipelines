package au.org.ala.clustering;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.gbif.pipelines.io.avro.IndexRecord;

/** A wrapper around a IndexRecord giving typed access to named terms with null handling. */
public class OccurrenceFeatures {

  private final IndexRecord row;

  /** @param row The underlying DataSet row to expose */
  public OccurrenceFeatures(IndexRecord row) {
    this.row = row;
  }

  public Object get(String field) {
    if (row.getStrings().containsKey(field)) {
      return row.getStrings().get(field);
    }
    if (row.getLongs().containsKey(field)) {
      return row.getLongs().get(field);
    }
    if (row.getInts().containsKey(field)) {
      return row.getInts().get(field);
    }
    if (row.getDoubles().containsKey(field)) {
      return row.getDoubles().get(field);
    }
    if (row.getBooleans().containsKey(field)) {
      return row.getBooleans().get(field);
    }
    return null;
  }

  public Long getLong(String field) {
    return row.getLongs().get(field);
  }

  public String getString(String field) {
    return row.getStrings().get(field);
  }

  public Integer getInt(String field) {
    return row.getInts().get(field);
  }

  public Double getDouble(String field) {
    return row.getDoubles().get(field);
  }

  public List<String> getStrings(String... field) {
    List<String> vals = new ArrayList<>(field.length);
    Arrays.stream(field).forEach(s -> vals.add(getString(s)));
    return vals;
  }
}
