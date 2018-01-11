package org.gbif.dwca.generator.util;

import org.gbif.dwca.Category;

/**
 * Interface to build schema in desired format.
 *
 * @author clf358
 */
interface DwCASchemaBuilder {

  String buildSchema(Category category, DwCADataModel model);

}
