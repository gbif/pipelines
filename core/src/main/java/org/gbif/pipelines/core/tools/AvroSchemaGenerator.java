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

import com.google.common.base.CaseFormat;
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
  private static final Map<String, Schema> COMMON_TYPES_SCHEMAS = new HashMap<>();

  /**
   * key -> {@link org.apache.avro.Schema.Type} , value -> {@link JsonNode} witht the default value.
   */
  private static final EnumMap<Schema.Type, JsonNode> COMMON_SCHEMAS_DEFAULTS = new EnumMap<>(Schema.Type.class);

  static {
    // initialize schemas of common types
    COMMON_TYPES_SCHEMAS.put(Integer.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.INT));
    COMMON_TYPES_SCHEMAS.put(int.class.getSimpleName().toLowerCase(),
                             COMMON_TYPES_SCHEMAS.get(Integer.class.getSimpleName().toLowerCase()));
    COMMON_TYPES_SCHEMAS.put(String.class.getSimpleName(), Schema.create(Schema.Type.STRING));
    COMMON_TYPES_SCHEMAS.put(Boolean.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.BOOLEAN));
    COMMON_TYPES_SCHEMAS.put(Long.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.LONG));
    COMMON_TYPES_SCHEMAS.put(Float.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.FLOAT));
    COMMON_TYPES_SCHEMAS.put(Double.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.DOUBLE));
    COMMON_TYPES_SCHEMAS.put(Byte.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.BYTES));
    COMMON_TYPES_SCHEMAS.put(Short.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.INT));
    COMMON_TYPES_SCHEMAS.put(Character.class.getSimpleName().toLowerCase(), Schema.create(Schema.Type.STRING));
    COMMON_TYPES_SCHEMAS.put(char.class.getSimpleName().toLowerCase(),
                             COMMON_TYPES_SCHEMAS.get(Character.class.getSimpleName().toLowerCase()));

    // initialize defaults of common schemas
    COMMON_SCHEMAS_DEFAULTS.put(Schema.Type.BOOLEAN, BooleanNode.getFalse());
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
  public static Schema generateSchema(Class<?> clazz, String schemaName, String schemaDoc, String namespace) {
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

  private static SchemaData generateSchemaData(Class<?> clazz, String schemaName, String schemaDoc, String namespace) {

    Objects.requireNonNull(clazz, "clazz argument is required");
    Objects.requireNonNull(schemaName, "schema name argument is required");
    Objects.requireNonNull(namespace, "namespace argument is required");

    // generate schema of type record without fields
    Schema schemaGenerated = Schema.createRecord(schemaName, schemaDoc, namespace, false);

    // create map with custom types to reuse in the schema generation
    Map<String, Schema> customTypes = new HashMap<>();

    // we always add the schema itself as a type
    customTypes.put(clazz.getSimpleName(), schemaGenerated);

    List<Schema.Field> schemaFields = createFields(clazz, customTypes, namespace);

    // return data
    return new SchemaData(schemaGenerated, schemaFields);
  }


  private static List<Schema.Field> createFields(Class<?> clazz, Map<String, Schema> customSchemas,
                                   String namespace) {
    List<Schema.Field> schemaFields = new ArrayList<>();

    // get all the fields that will be added to the schema
    createFieldsRecursive(schemaFields, clazz.getDeclaredFields(), customSchemas, namespace);
    return schemaFields;
  }

  private static void createFieldsRecursive(List<Schema.Field> avroFields, Field[] fields,
                                            Map<String, Schema> customSchemas, String namespace) {
    for (Field field : fields) {
      // create schema depending on the file type TODO: handle more types (arrays, maps...)
      Schema schema;
      // enums
      if (field.getType().isEnum()) {
        schema = Schema.createEnum(capitalize(field.getName()),
                                   null,
                                   namespace,
                                   Arrays.stream(field.getType().getEnumConstants())
                                     .map(Object::toString)
                                     .collect(Collectors.toList()));
      } else if (isCollection(field)) { // collections
        schema = Schema.createArray(createSchema(getCollectionType(field), customSchemas));
      } else if (isJavaType(field)) { // java types
        schema = createSchema(field.getType().getSimpleName(), customSchemas);
      } else { // rest: custom types
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

    if (COMMON_TYPES_SCHEMAS.get(className.toLowerCase()) == null) {
      return Schema.create(Schema.Type.STRING);
    }

    return Optional.ofNullable(COMMON_TYPES_SCHEMAS.get(className.toLowerCase()))
      .orElse(Schema.create(Schema.Type.STRING));
  }

  /**
   * Currently we generate all the fields nullable so the default will be always null, but we leave this method for
   * the future.
   */
  private static JsonNode defaultValueOf(Schema schema) {
    // according to the avro specification, in union schemas the default value corresponds to the first schema
    Schema.Type schemaType =
      Schema.Type.UNION == schema.getType()? schema.getTypes().get(0).getType() : schema.getType();

    return COMMON_SCHEMAS_DEFAULTS.getOrDefault(schemaType, NullNode.getInstance());
  }

  /**
   * Models the data necessary to create a schema.
   */
  private static class SchemaData {

    private final Schema rawSchema;
    private final List<Schema.Field> fields;

    private SchemaData(Schema rawSchema, List<Schema.Field> fields) {
      this.rawSchema = rawSchema;
      this.fields = fields;
    }

    private SchemaData addFieldAsFirstElement(Schema.Field field) {
      fields.add(0, field);
      return this;
    }

    private Schema buildSchema() {
      rawSchema.setFields(fields);
      return rawSchema;
    }

  }

}
