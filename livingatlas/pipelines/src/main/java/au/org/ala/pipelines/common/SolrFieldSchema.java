package au.org.ala.pipelines.common;

public class SolrFieldSchema implements java.io.Serializable {
  public enum Types {
    STRING("string"),
    DOUBLE("double"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DATE("date"),
    BOOLEAN("boolean");

    private String type;

    Types(String type) {
      this.type = type;
    }

    public String getValue() {
      return type;
    }

    @Override
    public String toString() {
      return String.valueOf(type);
    }

    public static Types valueOfType(String type) {
      for (Types e : values()) {
        if (e.type.equals(type)) {
          return e;
        }
      }
      return null;
    }
  }

  public Types type = Types.STRING;
  public boolean multiple = false;

  public SolrFieldSchema(String type, boolean multiple) {
    this.type = Types.valueOfType(type);
    this.multiple = multiple;
  }
}
