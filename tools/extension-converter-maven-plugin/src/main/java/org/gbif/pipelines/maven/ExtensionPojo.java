package org.gbif.pipelines.maven;

import java.util.List;

public class ExtensionPojo {

  private String tableName;

  private String namespace;

  private String packagePath;

  private String rowType;

  private List<Setter> setters;

  private String extensionPackage;

  public ExtensionPojo() {}

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

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getPackagePath() {
    return packagePath;
  }

  public void setPackagePath(String packagePath) {
    this.packagePath = packagePath;
  }

  public String getRowType() {
    return rowType;
  }

  public void setRowType(String rowType) {
    this.rowType = rowType;
  }

  public List<Setter> getSetters() {
    return setters;
  }

  public void setSetters(List<Setter> setters) {
    this.setters = setters;
  }

  public String getExtensionPackage() {
    return extensionPackage;
  }

  public void setExtensionPackage(String extensionPackage) {
    this.extensionPackage = extensionPackage;
  }

  public static class Setter {
    private String qualifier;
    private String name;
    private String vName;

    public Setter() {}

    public Setter(String qualifier, String name) {
      this.qualifier = qualifier;
      this.name = name.equals("Class") ? "Class$" : name;
      this.vName = "V" + name;
    }

    public String getQualifier() {
      return qualifier;
    }

    public void setQualifier(String qualifier) {
      this.qualifier = qualifier;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getvName() {
      return vName;
    }

    public void setvName(String vName) {
      this.vName = vName;
    }
  }
}
