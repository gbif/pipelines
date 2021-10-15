package org.gbif.validator.persistence.mapper;

import com.google.common.base.CaseFormat;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ColumnMapper {

  public static String toColumnName(String field) {
    return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field);
  }
}
