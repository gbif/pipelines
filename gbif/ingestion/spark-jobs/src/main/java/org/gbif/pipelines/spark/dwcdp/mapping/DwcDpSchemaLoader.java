package org.gbif.pipelines.spark.dwcdp.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Loads the official versioned DwC-DP schemas from {@code schemas/} on the classpath. */
public final class DwcDpSchemaLoader {
  private static final String ROOT = "schemas/";
  private final ClassLoader classLoader;
  private final ObjectMapper mapper;

  public DwcDpSchemaLoader() {
    this(Thread.currentThread().getContextClassLoader(), new ObjectMapper());
  }

  DwcDpSchemaLoader(ClassLoader classLoader, ObjectMapper mapper) {
    this.classLoader = classLoader;
    this.mapper = mapper;
  }

  public SchemaGraph current() {
    JsonNode bundles = read(ROOT + "bundles.json");
    List<String> latest = new ArrayList<>();
    for (JsonNode bundle : bundles.path("bundles")) {
      String index = text(bundle, "index");
      JsonNode indexNode = read(ROOT + index);
      if (indexNode.path("isLatest").asBoolean(false)) {
        latest.add(index.substring(0, index.lastIndexOf('/') + 1));
      }
    }
    if (latest.size() != 1) {
      throw new IllegalStateException("Expected exactly one latest DwC-DP schema bundle, found " + latest);
    }
    return loadVersionBase(latest.get(0));
  }

  public SchemaGraph version(String version) {
    return loadVersionBase(version.endsWith("/") ? version : version + "/");
  }

  private SchemaGraph loadVersionBase(String base) {
    JsonNode index = read(ROOT + base + "index.json");
    Map<String, SchemaResource> resources = new LinkedHashMap<>();
    List<PendingForeignKey> foreignKeys = new ArrayList<>();

    for (JsonNode descriptor : index.path("tableSchemas")) {
      JsonNode schema = read(ROOT + base + text(descriptor, "url"));
      String resourceName = text(schema, "name");
      Map<String, SchemaField> fields = new LinkedHashMap<>();
      for (JsonNode field : schema.path("fields")) {
        JsonNode constraints = field.path("constraints");
        String name = text(field, "name");
        fields.put(name, new SchemaField(name,
            constraints.path("required").asBoolean(false),
            constraints.path("unique").asBoolean(false)));
      }
      resources.put(resourceName,
          new SchemaResource(resourceName, fields,
              optionalText(schema, "primaryKey"), optionalText(schema, "weakPrimaryKey")));

      collectForeignKeys(schema.path("foreignKeys"), resourceName, false, foreignKeys);
      collectForeignKeys(schema.path("weakForeignKeys"), resourceName, true, foreignKeys);
    }

    List<SchemaRelation> relations = new ArrayList<>();
    for (PendingForeignKey fk : foreignKeys) {
      SchemaField sourceField = requireField(resources, fk.sourceResource, fk.sourceField);
      requireField(resources, fk.targetResource, fk.targetField);
      RelationCardinality forward = sourceField.unique()
          ? RelationCardinality.ONE_TO_ONE
          : RelationCardinality.MANY_TO_ONE;
      RelationCardinality reverse = sourceField.unique()
          ? RelationCardinality.ONE_TO_ONE
          : RelationCardinality.ONE_TO_MANY;
      relations.add(SchemaRelation.relation(
          fk.sourceResource, fk.sourceField, fk.targetResource, fk.targetField,
          fk.predicate, forward, fk.weak));
      relations.add(SchemaRelation.relation(
          fk.targetResource, fk.targetField, fk.sourceResource, fk.sourceField,
          fk.predicate, reverse, fk.weak));
    }
    return new OfficialSchemaGraph(resources, relations);
  }

  private static void collectForeignKeys(
      JsonNode foreignKeyArray,
      String resourceName,
      boolean weak,
      List<PendingForeignKey> foreignKeys) {
    for (JsonNode fk : foreignKeyArray) {
      JsonNode reference = fk.path("reference");
      foreignKeys.add(
          new PendingForeignKey(
              resourceName,
              scalarField(fk.path("fields")),
              text(reference, "resource", resourceName),
              scalarField(reference.path("fields")),
              optionalText(fk, "predicate").orElse(null),
              weak));
    }
  }

  private SchemaField requireField(
      Map<String, SchemaResource> resources, String resource, String field) {
    SchemaResource r = resources.get(resource);
    if (r == null) {
      throw new IllegalStateException("Foreign key references unknown resource " + resource);
    }
    SchemaField f = r.fields().get(field);
    if (f == null) {
      throw new IllegalStateException("Foreign key references unknown field " + resource + "." + field);
    }
    return f;
  }

  private JsonNode read(String path) {
    try (InputStream in = classLoader.getResourceAsStream(path)) {
      if (in == null) {
        throw new IllegalStateException("DwC-DP schema resource not found on classpath: " + path);
      }
      return mapper.readTree(in);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read DwC-DP schema resource: " + path, e);
    }
  }

  private static String scalarField(JsonNode node) {
    if (node.isTextual()) {
      return node.asText();
    }
    if (node.isArray() && node.size() == 1) {
      return node.get(0).asText();
    }
    throw new IllegalStateException("Composite FK fields are not supported by this first executor slice: " + node);
  }

  private static String text(JsonNode node, String field) {
    String value = node.path(field).asText(null);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required schema property '" + field + "' in " + node);
    }
    return value;
  }

  /**
   * Reads an optional schema property with a contextual fallback. Frictionless self-referencing
   * foreign keys may omit reference.resource; in that case the reference targets the current
   * resource.
   */
  private static String text(JsonNode node, String field, String defaultValue) {
    String value = node.path(field).asText(null);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  private static Optional<String> optionalText(JsonNode node, String field) {
    String value = node.path(field).asText(null);
    return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
  }

  private record PendingForeignKey(
      String sourceResource, String sourceField,
      String targetResource, String targetField,
      String predicate, boolean weak) {}
}
