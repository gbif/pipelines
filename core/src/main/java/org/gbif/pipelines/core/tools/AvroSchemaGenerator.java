package org.gbif.pipelines.core.tools;

import org.gbif.api.v2.NameUsageMatch2;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NullNode;

/**
 * Utility class to generate Avro Schemas programmatically.
 * <p>
 * This generator was created to be able to create an Avro schema from a Java class and do some modifications to that
 * schema. This functionality is not provided by the Avro library.
 */
public final class AvroSchemaGenerator {

  // default taxonomic schema
  private static final String DEFAULT_TAXON_SCHEMA_NAME = "TaxonRecord";
  private static final String DEFAULT_TAXON_SCHEMA_DOC = "A taxonomic record";
  private static final String DEFAULT_TAXON_SCHEMA_NAMESPACE = "org.gbif.pipelines.io.avro";

  /**
   * key -> class name , value -> {@link Schema} with schema and default value.
   */
  private static final Map<String, Schema> commonTypesSchemas = new HashMap<>();

  /**
   * key -> {@link org.apache.avro.Schema.Type} , value -> {@link JsonNode} witht the default value.
   */
  private static final EnumMap<Schema.Type, JsonNode> commonSchemasDefaults = new EnumMap<>(Schema.Type.class);

  static {
    // initialize schemas of common types
    commonTypesSchemas.put(Integer.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.INT));
    commonTypesSchemas.put(int.class.getSimpleName().toLowerCase(),
                           commonTypesSchemas.get(Integer.class.getSimpleName().toLowerCase()));
    commonTypesSchemas.put(String.class.getSimpleName(), Schema.create(Schema.Type.STRING));
    commonTypesSchemas.put(Boolean.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.BOOLEAN));
    commonTypesSchemas.put(Long.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.LONG));
    commonTypesSchemas.put(Float.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.FLOAT));
    commonTypesSchemas.put(Double.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.DOUBLE));
    commonTypesSchemas.put(Byte.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.BYTES));
    commonTypesSchemas.put(Short.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.INT));
    commonTypesSchemas.put(Character.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.STRING));
    commonTypesSchemas.put(char.class.getSimpleName().toLowerCase(),
                           commonTypesSchemas.get(Character.class.getSimpleName().toLowerCase()));

    // initialize defaults of common schemas
    commonSchemasDefaults.put(Schema.Type.BOOLEAN, BooleanNode.getFalse());
    // TODO: add more
  }

  private AvroSchemaGenerator() {}

  /**
   * Generates an Avro schema from a Java class.
   *
   * @param clazz      class to generate the schema from
   * @param schemaName name of the schema
   * @param schemaDoc  documentation of the schema
   * @param namespace  namespace of the schema
   *
   * @return {@link Schema} generated
   */
  public static Schema generateSchema(
    Class clazz, String schemaName, String schemaDoc, String namespace
  ) {
    return generateSchemaData(clazz, schemaName, schemaDoc, namespace).buildSchema();
  }

  /**
   * Generates a default taxonomic schema from a Java class.
   * <p>
   * It adds a ID field to the schema, since this is required in a taxonomic schema.
   *
   * @return {@link Schema} generated
   */
  public static Schema generateDefaultTaxonomicSchema() {
    SchemaData schemaData = generateSchemaData(NameUsageMatch2.class,
                                               DEFAULT_TAXON_SCHEMA_NAME,
                                               DEFAULT_TAXON_SCHEMA_DOC,
                                               DEFAULT_TAXON_SCHEMA_NAMESPACE);

    // add the record Id and build the schema
    return schemaData.addFieldAsFirstElement(new Schema.Field("id",
                                                              SchemaBuilder.builder().stringType(),
                                                              "The record id",
                                                              null)).buildSchema();
  }

  /**
   * Writes a schema to a File in the specified path.
   *
   * @param pathToWrite path where the schema will be written to
   * @param schema      schema to write to the file
   *
   * @throws IOException in case the operation could not be performed
   */
  public static void writeSchemaToFile(String pathToWrite, Schema schema) throws IOException {
    Files.write(Paths.get(pathToWrite),
                schema.toString(true).getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE);
  }

  private static SchemaData generateSchemaData(Class clazz, String schemaName, String schemaDoc, String namespace) {

    Objects.requireNonNull(clazz, "clazz argument is required");
    Objects.requireNonNull(schemaName, "schema name argument is required");
    Objects.requireNonNull(namespace, "namespace argument is required");

    // generate schema of type record without fields
    Schema schemaGenerated = Schema.createRecord(schemaName, schemaDoc, namespace, false);

    // create map with custom types to reuse in the schema generation
    Map<String, Schema> customTypes = new HashMap<>();

    // we always add the schema itself as a type
    customTypes.put(clazz.getSimpleName(), schemaGenerated);

    List<Schema.Field> schemaFields = new ArrayList<>();

    // get all the fields that will be added to the schema
    createFieldsRecursive(schemaFields, clazz.getDeclaredFields(), customTypes, namespace);

    // return data
    return new SchemaData(schemaGenerated, schemaFields);
  }

  private static void createFieldsRecursive(
    List<Schema.Field> avroFields, Field[] fields, Map<String, Schema> customSchemas, String namespace
  ) {
    for (Field field : fields) {
      // create schema depending on the file type TODO: handle more types (arrays, maps...)
      Schema schema = null;
      // enums
      if (field.getType().isEnum()) {
        schema = Schema.createEnum(capitalize(field.getName()),
                                   null,
                                   namespace,
                                   Arrays.stream(field.getType().getEnumConstants())
                                     .map(value -> value.toString())
                                     .collect(Collectors.toList()));
      }
      // collections
      else if (isCollection(field)) {
        schema = Schema.createArray(createSchema(getCollectionType(field), customSchemas));
      }
      // java types
      else if (isJavaType(field)) {
        schema = createSchema(field.getType().getSimpleName(), customSchemas);
      }
      // rest: custom types
      else {
        // to handle this type we create a new Schema of type record
        String recordName = capitalize(field.getType().getSimpleName());
        schema = Schema.createRecord(recordName, null, namespace, false);
        // add it to custom types map
        customSchemas.put(recordName, schema);

        // check fields of the type to determine if more schemas should be created
        List<Schema.Field> avroFieldsRecordSchema = new ArrayList<>();
        createFieldsRecursive(avroFieldsRecordSchema, field.getType().getDeclaredFields(), customSchemas, namespace);

        // add all the fields created to the schema
        schema.setFields(avroFieldsRecordSchema);
      }

      // create field and add it to the list
      schema = makeNullable(schema);
      avroFields.add(new Schema.Field(field.getName(), schema, null, defaultValueOf(schema)));
    }
  }

  private static boolean isCollection(Field field) {
    return Collection.class.isAssignableFrom(field.getType());
  }

  private static boolean isJavaType(Field field) {
    return field.getType().isPrimitive() || field.getType().getName().startsWith("java.lang");
  }

  private static String capitalize(String str) {
    return str == null ? "" : str.substring(0, 1).toUpperCase() + str.substring(1);
  }

  private static String getCollectionType(Field collection) {
    if (collection == null) {
      return "";
    }

    // generic
    if (collection.getGenericType() instanceof ParameterizedType) {
      String genericTypeName = collection.getGenericType().getTypeName();
      return genericTypeName.substring(genericTypeName.lastIndexOf('.') + 1, genericTypeName.lastIndexOf('>'));
    }

    return Object.class.getSimpleName();
  }

  private static Schema makeNullable(Schema schema) {
    return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
  }

  private static Schema createSchema(String className, Map<String, Schema> customSchemas) {
    if (customSchemas.containsKey(className)) {
      return customSchemas.get(className);
    }

    if (commonTypesSchemas.get(className.toLowerCase()) == null) {
      return Schema.create(Schema.Type.STRING);
    }

    return Optional.ofNullable(commonTypesSchemas.get(className.toLowerCase()))
      .orElse(Schema.create(Schema.Type.STRING));
  }

  /**
   * Currently we generate all the fields nullable so the default will be always null, but we leave this method for
   * the future.
   */
  private static JsonNode defaultValueOf(Schema schema) {
    // according to the avro specification, in union schemas the default value corresponds to the first schema
    Schema.Type schemaType =
      Schema.Type.UNION.equals(schema.getType()) ? schema.getTypes().get(0).getType() : schema.getType();

    return commonSchemasDefaults.getOrDefault(schemaType, NullNode.getInstance());
  }

  /**
   * Models the data neccessary to create a schema.
   */
  private static class SchemaData {

    private Schema rawSchema;
    private List<Schema.Field> fields;

    SchemaData(Schema rawSchema, List<Schema.Field> fields) {
      this.rawSchema = rawSchema;
      this.fields = fields;
    }

    SchemaData addField(Schema.Field field) {
      fields.add(field);
      return this;
    }

    SchemaData addFieldAsFirstElement(Schema.Field field) {
      fields.add(0, field);
      return this;
    }

    Schema buildSchema() {
      rawSchema.setFields(fields);
      return rawSchema;
    }

  }

}
