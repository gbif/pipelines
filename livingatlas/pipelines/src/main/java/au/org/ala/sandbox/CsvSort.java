package au.org.ala.sandbox;

import java.io.*;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

/**
 * A class to sort csv files by the id field, and a reader for the sorted file. The first column
 * must be an id without , and not wrapped in "
 */
@Slf4j
public class CsvSort {

  private static final int CHUNK_SIZE = 10000; // Number of records loaded in memory

  /**
   * Sorts a CSV file by the first column, where the first column is an id without a comma and not
   * wrapped in ".
   *
   * @param inputFilePath the path to the input CSV file
   * @param outputFilePath the path to the output CSV file
   * @throws IOException if an I/O error occurs
   */
  public static void sortCsvByFirstColumn(String inputFilePath, String outputFilePath)
      throws IOException {
    List<File> sortedChunkFiles = new ArrayList<>();

    // Step 1: Read and sort chunks
    try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath))) {
      List<String> chunk = new ArrayList<>(CHUNK_SIZE);
      String line;
      while ((line = reader.readLine()) != null) {
        chunk.add(line);
        if (chunk.size() == CHUNK_SIZE) {
          sortedChunkFiles.add(writeSortedChunk(chunk));
          chunk.clear();
        }
      }
      if (!chunk.isEmpty()) {
        sortedChunkFiles.add(writeSortedChunk(chunk));
      }
    }

    // Step 2: Merge sorted chunks
    mergeSortedChunks(sortedChunkFiles, outputFilePath);
  }

  // sort by the first column
  private static File writeSortedChunk(List<String> chunk) throws IOException {
    chunk.sort(Comparator.comparing(columns -> columns.substring(0, columns.indexOf(","))));
    File tempFile = File.createTempFile("sortedChunk", ".csv");
    tempFile.deleteOnExit();

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
      for (String columns : chunk) {
        writer.write(columns);
        writer.newLine();
      }
    }
    return tempFile;
  }

  private static void mergeSortedChunks(List<File> sortedChunkFiles, String outputFilePath)
      throws IOException {
    PriorityQueue<CsvQueueReader> pq =
        new PriorityQueue<>(Comparator.comparing(reader -> reader.id));

    for (File file : sortedChunkFiles) {
      pq.add(new CsvQueueReader(new BufferedReader(new FileReader(file))));
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
      while (!pq.isEmpty()) {
        CsvQueueReader reader = pq.poll();
        String line = reader.next();

        if (line != null) {
          if (reader.hasNext()) {
            pq.add(reader);
          }

          writer.write(line);
          writer.newLine();
        } else {
          //          pq.remove(reader);
          reader.close();
        }
      }

      writer.flush();
    }
  }

  /**
   * A class to read a csv file line by line, where the first column is an id without a comma and
   * not wrapped in ". Use the public id when joining with other files.
   */
  public static class CsvQueueReader {
    private final BufferedReader reader;
    private String currentLine;
    public String id;

    public CsvQueueReader(BufferedReader reader) throws IOException {
      this.reader = reader;
      currentLine = reader.readLine();
      id = currentLine.substring(0, currentLine.indexOf(","));
    }

    public boolean hasNext() throws IOException {
      return currentLine != null;
    }

    public String next() throws IOException {
      String result = currentLine;
      if (result != null) {
        currentLine = reader.readLine();
        if (currentLine != null) {
          id = currentLine.substring(0, currentLine.indexOf(","));
        } else {
          id = null;
        }
      }
      return result;
    }

    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        log.error("Error closing reader", e);
      }
    }
  }
}
