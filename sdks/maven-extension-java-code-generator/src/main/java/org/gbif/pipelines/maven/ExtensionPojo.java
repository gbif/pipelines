package org.gbif.pipelines.maven;

import java.util.List;

public class ExtensionPojo {

  private final String tableName;

  private final String namespace;

  private final String packagePath;

  private final String rowType;

  private final List<Setter> setters;

  private final String extensionPackage;

  public ExtensionPojo(
      String tableName,
      String namespace,
      String packagePath,
      String rowType,
      List<Setter> setters,
      String extensionPackage) {
    this.tableName = tableName;
    this.namespace = namespace;
    this.packagePath = packagePath;
    this.rowType = rowType;
    this.setters = setters;
    this.extensionPackage = extensionPackage;
  }

  public String getTableName() {
    return tableName;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getPackagePath() {
    return packagePath;
  }

  public String getRowType() {
    return rowType;
  }

  public List<Setter> getSetters() {
    return setters;
  }

  public String getExtensionPackage() {
    return extensionPackage;
  }

  public static class Setter {
    private final String qualifier;
    private final String name;
    private final String vName;

    public Setter(String qualifier, String name) {
      this.qualifier = qualifier;

      // name
      if (name.equals("Class")) {
        this.name = "Class$";
      } else {
        String clearedName = name;
        if (Character.isDigit(name.charAt(0))) {
          clearedName = name + "$1";
        }
        if (clearedName.contains(":")) {
          int i = clearedName.indexOf(":");
          clearedName =
              clearedName.substring(0, i)
                  + clearedName.substring(i + 1, i + 2).toUpperCase()
                  + clearedName.substring(i + 2);
        }
        this.name = clearedName;
      }

      // v_name
      if (this.name.contains("$")) {
        this.vName = "V" + this.name.substring(0, this.name.indexOf('$'));
      } else {
        this.vName = "V" + this.name;
      }
    }

    public String getQualifier() {
      return qualifier;
    }

    public String getName() {
      return name;
    }

    public String getvName() {
      return vName;
    }
  }
}
