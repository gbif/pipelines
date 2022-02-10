package org.gbif.converters.converter;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.odftoolkit.simple.SpreadsheetDocument;
import org.odftoolkit.simple.table.Cell;
import org.odftoolkit.simple.table.Table;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * Experimental converter that reads an ODF(Open Document Format) spreadsheet (.ods) file and
 * produces a csv file.
 *
 * <p>Issues: odftoolkit table.getColumnCount() and table.getRowCount() are not working. See
 * https://issues.apache.org/jira/browse/ODFTOOLKIT-381
 *
 * <p>Limitation: the conversion will stop at the first empty line.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OdsConverter {

  private static final String DATE_VALUE_TYPE = "date";

  public static void convert(Path workbookFile, Path csvFile) throws IOException {
    try (FileInputStream fis = new FileInputStream(workbookFile.toFile());
        ICsvListWriter csvWriter =
            new CsvListWriter(
                new FileWriter(csvFile.toFile()), CsvPreference.STANDARD_PREFERENCE)) {

      SpreadsheetDocument doc = SpreadsheetDocument.loadDocument(fis);

      Objects.requireNonNull(doc, "spreadsheetDocument shall be provided");
      Objects.requireNonNull(csvWriter, "csvWriter shall be provided");

      if (doc.getSheetCount() == 0) {
        log.warn("No sheet found in the spreadsheetDocument");
        return;
      } // we work only on one sheet
      if (doc.getSheetCount() > 1) {
        log.warn("Detected more than 1 sheet, only reading the first one.");
      }

      Table table = doc.getSheetByIndex(0);
      List<String> headers =
          extractWhile(table, cell -> cell != null && isNotBlank(cell.getStringValue()));
      csvWriter.writeHeader(headers.toArray(new String[0]));

      boolean hasContent = true;
      int rowIdx = 1;
      while (hasContent) {
        List<String> line = extractLine(table, rowIdx, headers.size());
        hasContent = !line.stream().allMatch(StringUtils::isBlank);
        if (hasContent) {
          csvWriter.write(line);
        }
        rowIdx++;
      }

      // ensure to flush remaining content to csvWriter
      csvWriter.flush();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /** Extract line of data from a {@link Table}. */
  private static List<String> extractLine(Table table, int row, int expectedNumberOfColumns) {
    List<String> lineData = new ArrayList<>(expectedNumberOfColumns);

    for (int colIdx = 0; colIdx < expectedNumberOfColumns; colIdx++) {
      Cell cell = table.getCellByPosition(colIdx, row);

      if (cell == null) {
        lineData.add("");
      } else if (DATE_VALUE_TYPE.equalsIgnoreCase(cell.getValueType())) {
        // we should document what "H:" means
        Instant instant =
            cell.getFormatString().indexOf("H:") > 0
                ? cell.getDateTimeValue().toInstant()
                : cell.getDateValue().toInstant();

        // we need to use systemDefault ZoneId since the date we get from the library used it (by
        // using Calendar).
        LocalDateTime date = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        // set it to UTC for format it like 1990-01-02T00:00:00Z
        lineData.add(date.atZone(ZoneId.of("UTC")).toInstant().toString());
      } else {
        lineData.add(cell.getStringValue());
      }
    }

    return lineData;
  }

  /**
   * Extract data on a line while the provided function returns true.
   *
   * @param checkFn function to run on a Cell object to know if we should continue to extract data
   *     on the row.
   */
  private static List<String> extractWhile(Table table, Predicate<Cell> checkFn) {
    List<String> headers = new ArrayList<>();

    int row = 0;
    int colIdx = 0;
    while (checkFn.test(table.getCellByPosition(colIdx, row))) {
      headers.add(table.getCellByPosition(colIdx, row).getStringValue());
      colIdx++;
    }
    return headers;
  }
}
