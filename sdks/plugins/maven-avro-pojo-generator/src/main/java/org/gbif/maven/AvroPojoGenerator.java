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

    if (fields.size() < 255) {
      imports.addAll(
          List.of(
              "lombok.Data",
              "lombok.Builder",
              "lombok.NoArgsConstructor",
              "lombok.AllArgsConstructor"));
    } else {
      imports.addAll(List.of("lombok.experimental.SuperBuilder", "lombok.Data"));
    }

    StringBuilder sb = new StringBuilder();

    if (!namespace.isEmpty()) {
      sb.append("package ").append(namespace).append(";\n");
    }

    StringBuilder body = new StringBuilder();
    for (JsonNode field : fields) {
      String name = field.get("name").asText();
      String type = resolveType(field.get("type"), namespace, imports);
      if ("class".equals(name)) name = name + "_";
      body.append("    private ").append(type).append(" ").append(name).append(";\n");
    }

    if (body.toString().contains("List<")) {
      imports.add("java.util.List");
    }

    for (String imp : imports) {
      sb.append("import ").append(imp).append(";\n");
    }

    if (fields.size() < 255) {
      sb.append("""
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    """);
    } else { // use SuperBuilder to avoid constructor issues with too many parameters
      sb.append("""
    @Data
    @SuperBuilder
    """);
    }

    sb.append("public class ").append(className).append(" {\n");
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
      return PRIMITIVE_MAP.getOrDefault(typeNode.asText(), typeNode.asText());
    } else if (typeNode.isObject()) {
      String type = typeNode.get("type").asText();
      switch (type) {
        case "array":
          imports.add("java.util.List");
          String itemsType = resolveType(typeNode.get("items"), currentNamespace, imports);
          return "List<" + itemsType + ">";
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
    } else if (typeNode.isArray()) {
      for (JsonNode subType : typeNode) {
        if (!subType.isTextual() || !"null".equals(subType.asText())) {
          return resolveType(subType, currentNamespace, imports);
        }
      }
    } else if (typeNode.isArray()) {
      for (JsonNode subType : typeNode) {
        if (!subType.isTextual() || !"null".equals(subType.asText())) {
          return resolveType(subType, currentNamespace, imports);
        }
      }
    }
    return "Object";
  }
}
