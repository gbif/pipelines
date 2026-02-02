package org.gbif.maven;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * This generates a set of POJOs from Avro schema files (.avsc) found in the specified input
 * directory. The POJOs are as aligned as possible to the same structure as the Avro code
 * generation.
 *
 * <p>This class is expected to be removed when Avro is removed from the project and the POJOs can
 * be maintained directly in src/main/java. Code quality reflects this temporary nature.
 */
@Mojo(name = "generate-pojos")
public class AvroPojoGenerator extends AbstractMojo {

  @Parameter(property = "inputDir", required = true)
  private File inputDir;

  @Parameter(property = "outputDir", required = true)
  private File outputDir;

  @Parameter(property = "package-name", required = true)
  private String packageName; // used if namespace is not in the avro object

  private static final Map<String, String> PRIMITIVE_MAP =
      Map.of(
          "string", "String",
          "int", "int",
          "long", "long",
          "boolean", "boolean",
          "float", "float",
          "double", "double",
          "bytes", "byte[]");

  private final ObjectMapper mapper = new ObjectMapper();
  private final Map<String, String> generatedClasses = new HashMap<>();

  public void execute() throws MojoExecutionException {

    try {
      mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      generateFromDirectory(inputDir.getAbsolutePath(), outputDir.getAbsolutePath());
    } catch (IOException e) {
      throw new MojoExecutionException("Failed to generate POJOs", e);
    }
  }

  public void generateFromDirectory(String inputDir, String outputDir) throws IOException {
    Files.walk(Paths.get(inputDir))
        .filter(path -> path.toString().endsWith(".avsc"))
        .forEach(
            path -> {
              try {
                JsonNode schemaNode = mapper.readTree(path.toFile());
                processSchema(schemaNode, outputDir);
              } catch (IOException e) {
                getLog().error("Failed to process " + path, e);
              }
            });
  }

  private void processSchema(JsonNode schemaNode, String outputDir) throws IOException {
    if (schemaNode.isArray()) {
      Iterator<JsonNode> iter = schemaNode.iterator();
      while (iter.hasNext()) {
        processSchema(iter.next(), outputDir);
      }
    } else {
      String type = schemaNode.get("type").asText();

      if ("record".equals(type)) {
        generateRecordClass(schemaNode, outputDir);
      } else if ("enum".equals(type)) {
        generateEnum(schemaNode, outputDir);
      }
    }
  }

  private void generateRecordClass(JsonNode schemaNode, String outputDir) throws IOException {
    String className = schemaNode.get("name").asText();
    String namespace =
        schemaNode.has("namespace") ? schemaNode.get("namespace").asText() : packageName;
    String packageDir = namespace.replace('.', '/');
    File outputFolder = new File(outputDir, packageDir);
    if (!outputFolder.exists()) outputFolder.mkdirs();

    String javaCode = buildClass(schemaNode, namespace);
    File javaFile = new File(outputFolder, className + ".java");

    try (FileWriter writer = new FileWriter(javaFile)) {
      writer.write(javaCode);
    }

    generatedClasses.put(namespace + "." + className, className);
  }

  private String buildClass(JsonNode schemaNode, String namespace) {
    String className = schemaNode.get("name").asText();
    JsonNode fields = schemaNode.get("fields");

    Set<String> imports = new HashSet<>();

    imports.addAll(
        List.of(
            "java.io.Serializable",
            "lombok.Data",
            "lombok.Builder",
            "lombok.Getter",
            "lombok.Setter",
            "lombok.Setter",
            "lombok.EqualsAndHashCode",
            "lombok.NoArgsConstructor",
            "lombok.experimental.SuperBuilder",
            "lombok.NoArgsConstructor"));
    if (fields.size() < 255) {
      imports.add("lombok.AllArgsConstructor");
    }

    StringBuilder sb = new StringBuilder();

    if (!namespace.isEmpty()) {
      sb.append("package ").append(namespace).append(";\n");
    }

    StringBuilder body = new StringBuilder();
    for (JsonNode field : fields) {
      String name = field.get("name").asText();
      name = normalizeName(name);
      String type = resolveType(field.get("type"), namespace, imports);
      if ("class".equals(name) || "v_class".equals(name) || "v_class".equals(name))
        name = name + "_";
      if (type.equals("IssueRecord")) body.append("    @lombok.Builder.Default\n");
      if (type.equals("org.gbif.pipelines.io.avro.IssueRecord"))
        body.append("    @lombok.Builder.Default\n");
      if (type.startsWith("List")) body.append("    @lombok.Builder.Default\n");
      if (type.startsWith("Map")) body.append("    @lombok.Builder.Default\n");

      body.append("    private ").append(type).append(" ").append(name);

      if (type.startsWith("List")) body.append("= new ArrayList<>()");
      if (type.startsWith("Map")) body.append("= new HashMap<>()");
      if (type.equals("IssueRecord")) body.append("= IssueRecord.newBuilder().build()");
      if (type.equals("org.gbif.pipelines.io.avro.IssueRecord"))
        body.append("= org.gbif.pipelines.io.avro.IssueRecord.newBuilder().build()");

      body.append(";\n");
    }

    if (body.toString().contains("List<")) imports.add("java.util.List");
    if (body.toString().contains("List<")) imports.add("java.util.ArrayList");
    if (body.toString().contains("Map<")) imports.add("java.util.Map");
    if (body.toString().contains("Map<")) imports.add("java.util.HashMap");
    if (body.toString().contains("Set<")) imports.add("java.util.Set");

    imports.add("org.gbif.pipelines.io.avro.Record");
    imports.add("org.gbif.pipelines.io.avro.Issues");
    imports.add("org.gbif.pipelines.io.avro.Created");

    for (String imp : imports) {
      sb.append("import ").append(imp).append(";\n");
    }

    if (fields.size() < 255) {
      sb.append(
          """
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    @Builder(builderClassName = "Builder", builderMethodName = "newBuilder", setterPrefix = "set")
    """);
    } else { // use SuperBuilder to avoid constructor issues with too many parameters
      sb.append(
          """
    @Data
    @Getter
    @Setter
    @NoArgsConstructor
    @EqualsAndHashCode
    @SuperBuilder(builderMethodName = "newBuilder", setterPrefix = "set")
    """);
    }

    sb.append("public class ").append(className);

    // custom interfaces
    Set interfaces = new HashSet();
    if (body.toString().contains("String id;")) interfaces.add("Record");
    if (body.toString().contains("IssueRecord issues")) interfaces.add("Issues");
    if (body.toString().contains("Long created;")) interfaces.add("Created");
    interfaces.add("Serializable");

    if (!interfaces.isEmpty()) {
      sb.append(" implements ");
      sb.append(String.join(", ", interfaces));
    }

    sb.append(" {\n");
    sb.append(body);
    sb.append("}\n");

    return sb.toString();
  }

  private void generateEnum(JsonNode schemaNode, String outputDir) throws IOException {
    String enumName = schemaNode.get("name").asText();
    String namespace =
        schemaNode.has("namespace") ? schemaNode.get("namespace").asText() : packageName;
    String packageDir = namespace.replace('.', '/');
    File outputFolder = new File(outputDir, packageDir);
    if (!outputFolder.exists()) outputFolder.mkdirs();

    StringBuilder sb = new StringBuilder();
    if (!namespace.isEmpty()) {
      sb.append("package ").append(namespace).append(";\n");
    }

    sb.append("public enum ").append(enumName).append(" {\n");
    List<String> symbols = new ArrayList<>();
    for (JsonNode symbol : schemaNode.get("symbols")) {
      symbols.add(symbol.asText());
    }
    sb.append(String.join(", ", symbols));
    sb.append("\n  } \n ");

    File javaFile = new File(outputFolder, enumName + ".java");
    try (FileWriter writer = new FileWriter(javaFile)) {
      writer.write(sb.toString());
    }
  }

  private String resolveType(JsonNode typeNode, String currentNamespace, Set<String> imports) {
    if (typeNode.isTextual()) {
      String rawType = typeNode.asText();
      return PRIMITIVE_MAP.getOrDefault(rawType, rawType);
    }

    if (typeNode.isObject()) {
      String type = typeNode.get("type").asText();

      switch (type) {
        case "array":
          imports.add("java.util.List");
          String itemType = resolveType(typeNode.get("items"), currentNamespace, imports);
          return "List<" + boxIfPrimitive(itemType) + ">";
        case "map":
          imports.add("java.util.Map");
          String valueType = resolveType(typeNode.get("values"), currentNamespace, imports);
          return "Map<String, " + boxIfPrimitive(valueType) + ">";
        case "record":
        case "enum":
          try {
            processSchema(typeNode, outputDir.getAbsolutePath());
          } catch (IOException e) {
            throw new RuntimeException("Failed to generate nested type", e);
          }
          String ns =
              typeNode.has("namespace") ? typeNode.get("namespace").asText() : currentNamespace;
          return ns.isEmpty()
              ? typeNode.get("name").asText()
              : ns + "." + typeNode.get("name").asText();
        default:
          return PRIMITIVE_MAP.getOrDefault(type, "Object");
      }
    }

    if (typeNode.isArray()) {
      // handle ["null", "primitive"] → boxed primitive
      JsonNode nonNullType = null;
      for (JsonNode subType : typeNode) {
        if (!subType.isTextual() || !"null".equals(subType.asText())) {
          nonNullType = subType;
          break;
        }
      }
      if (nonNullType != null) {
        String resolved = resolveType(nonNullType, currentNamespace, imports);
        return boxIfPrimitive(resolved); // box because nullable
      }
    }

    return "Object";
  }

  private static final Map<String, String> BOXED_MAP =
      Map.of(
          "int", "Integer",
          "long", "Long",
          "boolean", "Boolean",
          "float", "Float",
          "double", "Double",
          "byte[]", "byte[]", // leave as is
          "string", "String");

  private String boxIfPrimitive(String type) {
    return BOXED_MAP.getOrDefault(type, type);
  }

  protected String normalizeName2(String name) {
    if (name.startsWith("v_")) {
      return "v" + name.substring(2, 3).toUpperCase() + name.substring(3);
    } else return name;
  }

  // e.g. vDc_type -> vDcType
  public static String normalizeName(String input) {
    if (input.startsWith("v_")) {
      input = "v" + input.substring(2, 3).toUpperCase() + input.substring(3);
    }

    // hack (DnaDerivedDataTable)
    boolean startsWithUnderscore = input.startsWith("_"); // _16srecover become _16srecover

    while (input.contains("_")) {
      int idx = input.indexOf('_');
      if (idx < input.length() - 1) {
        String replacement = input.substring(idx + 1, idx + 2).toUpperCase();
        input = input.substring(0, idx) + replacement + input.substring(idx + 2);
      } else {
        input = input.substring(0, idx);
      }
    }
    if (input.equals("vClass")) {
      return "vClass_";
    } else {
      return startsWithUnderscore ? "_" + input : input;
    }
  }
}
