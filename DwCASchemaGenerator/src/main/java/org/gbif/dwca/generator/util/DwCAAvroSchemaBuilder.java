package org.gbif.dwca.generator.util;

import org.gbif.dwca.Category;
import org.gbif.dwca.DwCATerm;

import java.util.List;

/**
 * Builds Avro Schema for provided DwCA Category
 *
 * @author clf358
 */
public class DwCAAvroSchemaBuilder implements DwCASchemaBuilder {

  private static final String OCCURENCE_ID = "occurrenceID";

  @Override
  public String buildSchema(Category category, DwCADataModel model) {
    if (!model.getCategoryIdentifierAndFieldsMap().containsKey(category.getCategoryIdentifier())) {
      throw new IllegalArgumentException(category.getCategoryIdentifier()
                                         + " identifier provided is not defined in spec or not available");
    } else {
      if (!model.getIdentifierCategoryMap().containsKey(category.getCategoryIdentifier())) {
        throw new RuntimeException("Model donot have provided category, Consider reparsing model");
      }
      if (!model.getCategoryIdentifierAndFieldsMap().containsKey(category.getCategoryIdentifier())) {
        throw new RuntimeException("Model donot have provided category, Consider reparsing model");
      }
      /**
       * obtain the information about the category from the data model. That is basic definition and
       * all terms possible under the category as fields.
       */
      DwCATerm categoryTerm = model.getIdentifierCategoryMap().get(category.getCategoryIdentifier());
      List<DwCATerm> fields = model.getCategoryIdentifierAndFieldsMap().get(category.getCategoryIdentifier());
      /**
       * generate avro schema from the available information
       */
      StringBuilder schemabuilder = new StringBuilder();
      schemabuilder.append("{\n\t\"type\":\"record\",")
        .append("\n")
        .append("\t\"namespace\":\"org.gbif.dwca.avro\",")
        .append("\n")
        .append("\t\"name\":\"")
        .append(category.name())
        .append("\",")
        .append("\n")
        .append("\t\"doc\":\"")
        .append(categoryTerm.getDefinition())
        .append("\",")
        .append("\n")
        .append("\t\"fields\":[")
        .append("\n");

      for (int i = 0; i < fields.size(); i++) {
        String termName = fields.get(i).getTermName();
        termName = termName.replaceAll(":", "_"); // avro schema donot support special characters
        // (check dcterms:type)
        /**
         * Since occurenceId is considered as Primary Key, we donot accept any default or null
         * values. For other fields null or default empty is possible
         */
        if (termName.equals(OCCURENCE_ID)) {
          schemabuilder.append("\t\t{\"name\":")
            .append("\"")
            .append(termName)
            .append("\",")
            .append("\"type\":\"string\"")
            .append(", \"doc\":\"")
            .append(fields.get(i).getIdentifier())
            .append("\"")
            .append("}");
        } else {
          schemabuilder.append("\t\t{\"name\":")
            .append("\"")
            .append(termName)
            .append("\",")
            .append("\"type\":[\"null\",\"string\"]")
            .append(",\"default\":\"\"")
            .append(", \"doc\":\"")
            .append(fields.get(i).getIdentifier())
            .append("\"")
            .append("}");
        }

        if (i != fields.size() - 1) schemabuilder.append(",");
        schemabuilder.append("\n");
      }

      schemabuilder.append("\t]").append("\n").append("}");
      return schemabuilder.toString();
    }
  }

}
