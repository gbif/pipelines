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
      if (name.equalsIgnoreCase("Class") || name.equalsIgnoreCase("Class_")) {
        this.name = "Class_";
      } else {
        String clearedName = name;
        // _16_recover
        if (Character.isDigit(name.charAt(0))) {
          clearedName = "_" + name;
        }

        if (clearedName.indexOf(":") > 0) {

          int firstSemicolon = clearedName.indexOf(":");

          String firstPart = clearedName.substring(0, firstSemicolon);
          firstPart = Character.toUpperCase(firstPart.charAt(0)) + firstPart.substring(1);

          String secondPart = clearedName.substring(firstSemicolon + 1).replaceAll("[:\\-]", "_");

          this.name = firstPart + "_" + secondPart;
        } else {
          this.name =
              Character.toUpperCase(clearedName.charAt(0))
                  + name.substring(1).replaceAll("[_\\-]", "");
        }
      }

      if (this.name.toLowerCase().startsWith("_")) {
        this.vName = "V" + this.name.toLowerCase();
      } else if (this.name.toLowerCase().equals("class_")) {
        this.vName = "V_class";
      } else {
        this.vName = "V_" + this.name.toLowerCase();
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
