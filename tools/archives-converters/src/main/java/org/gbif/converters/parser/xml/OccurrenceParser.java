/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.converters.parser.xml;

import com.sun.org.apache.xerces.internal.impl.io.MalformedByteSequenceException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.NodeCreateRule;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.converters.parser.xml.constants.ExtractionSimpleXPaths;
import org.gbif.converters.parser.xml.model.RawOccurrenceRecord;
import org.gbif.converters.parser.xml.parsing.RawXmlOccurrence;
import org.gbif.converters.parser.xml.parsing.response.file.ParsedSearchResponse;
import org.gbif.converters.parser.xml.parsing.xml.XmlFragmentParser;
import org.gbif.converters.parser.xml.util.XmlSanitizingReader;
import org.gbif.utils.file.CharsetDetection;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Entry point into the parsing of raw occurrence records as retrieved from publishers. Will attempt
 * to determine both XML encodings and schema type. Parse happens in two steps - first extracts each
 * record element into a RawXmlOccurrence, and then parses each of those into RawOccurrenceRecords.
 */
@Slf4j
public class OccurrenceParser {

  private static final String ENCONDING_EQ = "encoding=";
  private static final Pattern ENCODING_PATTERN = Pattern.compile(ENCONDING_EQ);
  private static final Pattern REPLACE_QUOTES_PAT = Pattern.compile("[\"']");

  public static final String ADD_RECORD_AS_XML = "addRecordAsXml";
  public static final String SET_ABCD_1_HEADER = "setAbcd1Header";

  public static List<RawXmlOccurrence> parse(File file) {
    return new OccurrenceParser().parseFile(file);
  }

  public List<RawOccurrenceRecord> parseResponseFileToRor(File inputFile) {
    List<RawXmlOccurrence> raws = parseResponseFileToRawXml(inputFile);
    return parseRawXmlToRor(raws);
  }

  /**
   * This parses a stream of uncompressed ABCD or DwC Occurrences into {@link RawXmlOccurrence}s. No
   * care is taken to handle wrong encodings or character sets in general. This might be changed
   * later on.
   *
   * @param is stream to parse
   * @return list of parsed occurrences
   * @throws ParsingException if there were any problems during parsing the stream
   */
  // TODO: Optionally handle compressed streams
  public List<RawXmlOccurrence> parseStream(InputStream is) {
    Objects.requireNonNull(is, "is can't be null");
    try {
      ParsedSearchResponse responseBody = new ParsedSearchResponse();

      parse(new InputSource(is), responseBody);

      return responseBody.getRecords();
    } catch (ParserConfigurationException | TransformerException e) {
      throw new ServiceUnavailableException("Error setting up Commons Digester", e);
    } catch (SAXException | IOException e) {
      throw new ParsingException("Parsing failed", e);
    }
  }

  /**
   * This parses a xml file of uncompressed ABCD or DwC Occurrences into {@link RawXmlOccurrence}s.
   * No care is taken to handle wrong encodings or character sets in general. This might be changed
   * later on.
   *
   * @param file xml response file
   * @return list of parsed occurrences
   * @throws ParsingException if there were any problems during parsing the stream
   */
  public List<RawXmlOccurrence> parseFile(File file) {
    try (InputStream inputStream = new FileInputStream(file)) {
      return parseStream(inputStream);
    } catch (IOException ex) {
      throw new ParsingException("Parsing failed", ex);
    }
  }

  private ParsedSearchResponse read(File gzipFile, Charset charset) {
    ParsedSearchResponse responseBody = null;
    log.debug("Trying charset [{}]", charset);
    try (FileInputStream fis = new FileInputStream(gzipFile);
        GZIPInputStream inputStream = new GZIPInputStream(fis);
        BufferedReader inputReader =
            new BufferedReader(
                new XmlSanitizingReader(new InputStreamReader(inputStream, charset)))) {
      responseBody = new ParsedSearchResponse();
      parse(new InputSource(inputReader), responseBody);
      log.debug("Success with charset [{}] - skipping any others", charset);
      return responseBody;
    } catch (SAXException e) {
      log.debug(
          "SAX exception when parsing gzipFile [{}] using encoding [{}] - trying another charset",
          gzipFile.getAbsolutePath(),
          charset,
          e);
    } catch (MalformedByteSequenceException e) {
      log.debug(
          "Malformed utf-8 byte when parsing with encoding [{}] - trying another charset", charset);
    } catch (IOException ex) {
      log.warn("Error reading input files", ex);
    } catch (ParserConfigurationException e) {
      log.warn(
          "Failed to pull raw parsing from response gzipFile [{}] - skipping gzipFile",
          gzipFile.getAbsolutePath(),
          e);
    } catch (TransformerException e) {
      log.warn(
          "Could not create parsing transformer for [{}] - skipping gzipFile",
          gzipFile.getAbsolutePath(),
          e);
    }
    return responseBody;
  }

  /** Parses a single response gzipFile and returns a List of the contained RawXmlOccurrences. */
  public List<RawXmlOccurrence> parseResponseFileToRawXml(File gzipFile) {

    log.debug(">> parseResponseFileToRawXml [{}]", gzipFile.getAbsolutePath());
    try {
      Optional<ParsedSearchResponse> responseBody =
          getCharsets(gzipFile).stream()
              .map(charset -> read(gzipFile, charset))
              .filter(Objects::nonNull)
              .findFirst();
      if (!responseBody.isPresent()) {
        log.warn(
            "Could not parse gzipFile (malformed parsing) - skipping gzipFile [{}]",
            gzipFile.getAbsolutePath());
      }
      log.debug("<< parseResponseFileToRawXml [{}]", gzipFile.getAbsolutePath());
      return responseBody.map(ParsedSearchResponse::getRecords).orElse(Collections.emptyList());
    } catch (IOException e) {
      log.warn(
          "Could not find response gzipFile [{}] - skipping gzipFile",
          gzipFile.getAbsolutePath(),
          e);
    }
    return Collections.emptyList();
  }

  /**
   * Utility method to extract character encodings from a gzip file. Charsets are a nightmare and
   * users can't be trusted, so strategy is try these encodings in order until one of them
   * (hopefully) works (note the last two could be repeats of the first two): - utf-8 - latin1
   * (iso-8859-1) - the declared encoding from the parsing itself - a guess at detecting the charset
   * from the raw gzipFile bytes
   */
  private static List<Charset> getCharsets(File gzipFile) throws IOException {
    List<Charset> charsets =
        Stream.of(StandardCharsets.UTF_8, StandardCharsets.ISO_8859_1).collect(Collectors.toList());

    // read parsing declaration
    try (FileInputStream fis = new FileInputStream(gzipFile);
        GZIPInputStream inputStream = new GZIPInputStream(fis);
        InputStreamReader inputStreamReader =
            new InputStreamReader(inputStream, Charset.defaultCharset());
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
      boolean gotEncoding = false;
      int lineCount = 0;
      while (bufferedReader.ready() && !gotEncoding && lineCount < 5) {
        String line = bufferedReader.readLine();
        lineCount++;
        if (line != null && line.contains(ENCONDING_EQ)) {
          String encoding = ENCODING_PATTERN.split(line, 0)[1];
          // drop trailing ?>
          encoding = encoding.substring(0, encoding.length() - 2);
          // drop quotes
          encoding = REPLACE_QUOTES_PAT.matcher(encoding).replaceAll("").trim();
          log.debug("Found encoding [{}] in parsing declaration", encoding);
          try {
            charsets.add(Charset.forName(encoding));
          } catch (Exception e) {
            log.debug(
                "Could not find supported charset matching detected encoding of [{}] - trying other guesses instead",
                encoding);
          }
          gotEncoding = true;
        }
      }
    }
    // attempt detection from bytes
    charsets.add(CharsetDetection.detectEncoding(gzipFile));
    return charsets;
  }

  /**
   * A Digester parser, uses ABCD and DwC rules to parse XML input source
   *
   * @param inputSource xml response
   * @param responseBody storage for Digester
   */
  private void parse(InputSource inputSource, ParsedSearchResponse responseBody)
      throws ParserConfigurationException, SAXException, IOException {

    Digester digester = new Digester();
    digester.setNamespaceAware(true);
    digester.setValidating(false);
    digester.push(responseBody);

    NodeCreateRule rawAbcd = new NodeCreateRule();
    digester.addRule(ExtractionSimpleXPaths.ABCD_RECORD_XPATH, rawAbcd);
    digester.addSetNext(ExtractionSimpleXPaths.ABCD_RECORD_XPATH, ADD_RECORD_AS_XML);

    NodeCreateRule rawAbcd1Header = new NodeCreateRule();
    digester.addRule(ExtractionSimpleXPaths.ABCD_HEADER_XPATH, rawAbcd1Header);
    digester.addSetNext(ExtractionSimpleXPaths.ABCD_HEADER_XPATH, SET_ABCD_1_HEADER);

    NodeCreateRule rawDwc1_0 = new NodeCreateRule();
    digester.addRule(ExtractionSimpleXPaths.DWC_1_0_RECORD_XPATH, rawDwc1_0);
    digester.addSetNext(ExtractionSimpleXPaths.DWC_1_0_RECORD_XPATH, ADD_RECORD_AS_XML);

    NodeCreateRule rawDwc1_4 = new NodeCreateRule();
    digester.addRule(ExtractionSimpleXPaths.DWC_1_4_RECORD_XPATH, rawDwc1_4);
    digester.addSetNext(ExtractionSimpleXPaths.DWC_1_4_RECORD_XPATH, ADD_RECORD_AS_XML);

    // TODO: dwc_manis appears to work without a NodeCreateRule here - why?

    NodeCreateRule rawDwc2009 = new NodeCreateRule();
    digester.addRule(ExtractionSimpleXPaths.DWC_2009_RECORD_XPATH, rawDwc2009);
    digester.addSetNext(ExtractionSimpleXPaths.DWC_2009_RECORD_XPATH, ADD_RECORD_AS_XML);

    digester.parse(inputSource);
  }

  private List<RawOccurrenceRecord> parseRawXmlToRor(List<RawXmlOccurrence> rawRecords) {
    return rawRecords.stream()
        .map(XmlFragmentParser::parseRecord)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}
