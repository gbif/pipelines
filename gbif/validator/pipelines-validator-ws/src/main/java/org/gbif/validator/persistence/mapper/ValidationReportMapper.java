package org.gbif.validator.persistence.mapper;

import java.util.UUID;
import org.apache.ibatis.annotations.Param;

public interface ValidationReportMapper {

  void createOrUpdate(
      @Param("validationKey") UUID validationKey,
      @Param("attempt") int attempt,
      @Param("reportType") String reportType,
      @Param("report") String report);

  String get(
      @Param("validationKey") UUID validationKey,
      @Param("attempt") int attempt,
      @Param("reportType") String reportType);
}
