# Plan: match phylogenetic tree tips to occurrences (gbif/pipelines#1573)

Extension: https://rs.gbif.org/sandbox/extension/gbif/1.0/phylogenetic_material_citation_2026-10-07.xml
rowType: `http://rs.gbif.org/terms/1.0/PhylogeneticMaterialCitation`

## Goal

Datasets publish a phylogenetic tree (Newick or Nexus) through the registry. The Phylogenetic
Material Citation extension links each occurrence to a tip of that tree. It is only a link:
`phylogeneticTreeTipID`, `phylogeneticTreeFileName`, `phylogeneticTreeName`.

After `runTransforms` in `OccurrenceInterpretationPipeline`:

1. fetch the dataset's tree files from the registry;
2. traverse each tree and, for every tip, find the occurrence whose extension row points to it,
   linking whenever possible (see "Tip matching rules");
3. take from that occurrence's interpreted taxonomy the 3 columns we need:
   - `scientificName`
   - `taxonKey` (the most specific usage key, no higher classification)
   - `matchIssues`
4. write one CSV per tree (tip id + those 3 columns);
5. tips with no linked occurrence: run a taxonomy match on the tip label (**TODO**).

Occurrence records are **not** modified. This step only produces the tree enrichment output.

## Assumptions / decisions

- One extension row per occurrence.
- Taxonomy columns come from the occurrence's `MultiTaxonRecord`, from the `TaxonRecord` whose
  `datasetKey == OccurrenceJsonConverter.DEFAULT_TAXONOMY_KEY`:
  - `scientificName` = `usage.name`
  - `taxonKey` = `usage.key`
  - `matchIssues` = `issues.issueList`, joined with `|`
- Extension terms are hardcoded for now (TODO: move to dwc-api).
- Newick/Nexus parsing uses BioJava's build of forester (`org.biojava.thirdparty:forester:1.039`).
  `biojava-phylo` no longer exists in BioJava 7 (its code moved to `biojava-alignment`), and it
  only wrapped forester without adding Newick/Nexus parsing. Forester's `NexusPhylogeniesParser`
  resolves the `TRANSLATE` block, so tips come back with their real labels. The jar only contains
  `org.forester` classes; `openchart` (not in Maven Central, GUI only) is excluded.

## Tip matching rules

The goal is to link whenever possible. For each tip `T` of tree `N` in file `F`, the candidate
occurrences are those whose extension row has `phylogeneticTreeTipID == T`, ranked:

1. **File match.** `phylogeneticTreeFileName == F`, and `phylogeneticTreeName` is either `N` or
   empty. An empty tree name links to tip `T` in every tree of `F`.
2. **Tip ID only.** No row matches at rank 1, so we take a row with the same tip ID whatever its
   file name or tree name. This covers wrong or missing file names, renamed files, and conflicting
   tree names.

- If several occurrences share the best rank for a tip, take the one with the lowest internal
  gbifID (`Occurrence.id`, not `dwc:occurrenceID`). This is only a tie-breaker, so reruns produce
  the same CSV. It can happen when several specimens cite the same tip, with duplicate records, or
  with tip-ID-only matches across files.
  - Tied occurrences with the **same** `taxonKey`: not a conflict, because the CSV row is the same
    whichever we pick. Nothing is recorded.
  - Tied occurrences with **different** `taxonKey`s: a conflict. Count it in the metadata and add a
    warning with the tip, the candidate gbifIDs and their taxonKeys.
- The rank isn't a CSV column, which keeps the agreed 3 taxonomy columns. The metadata file counts
  tips matched at each rank.
- Tree names: Newick files have no tree names, and Nexus trees can be unnamed. Use the parsed name
  when there is one (e.g. `UNTITLED` in the sample file), otherwise `tree{n}` by position in the
  file. The same name is used for matching and for the CSV file name.

## Registry API

- List the dataset's trees:
  `GET {gbifApi}/v1/experimental/dataset/{datasetKey}/phylogeny`

  ```json
  [{"fileName":"global_tree_renamed.nex","size":1173495,"format":"NEXUS","error":"...",
    "trees":[],"treesTruncated":false,"isValid":false}]
  ```

- Download one tree:
  `GET {gbifApi}/v1/experimental/dataset/{datasetKey}/phylogeny/{fileName}`
  e.g. https://api.gbif-test.org/v1/experimental/dataset/3a1ca64a-e5bd-49a1-abe1-9af509c1143d/phylogeny/global_tree_renamed.nex
  (Nexus, about 9.5k tips, labels like `Lamiales_Lamiaceae_Lavandula_angustifolia`)

## Steps

### 1. Hardcoded extension terms

- New `PhylogeneticMaterialCitationTerms` constants class in `sdks/core` with the rowType and the 3
  term URIs: `http://rs.gbif.org/terms/1.0/phylogeneticTreeTipID`, `.../phylogeneticTreeFileName`
  and `.../phylogeneticTreeName`.
- `// TODO replace with dwc-api terms`.

### 2. Allow the extension through

`loadExtendedRecords` (`OccurrenceInterpretationPipeline.java:638`) drops any extension not listed
in `extensionsAllowedForVerbatimSet`. Add the rowType to:

- `gbif/ingestion/spark-jobs/pipelines.yaml`
- `gbif/ingestion/spark-jobs/src/test/resources/pipelines_template.yaml`
- `gbif/ingestion/spark-coordinator/src/test/resources/pipelines-configmap-test.yaml`

### 3. Registry client for phylogeny files

- `MetadataService` (`sdks/core/.../ws/metadata/MetadataService.java`) gets two new calls:
  - `@GET("/v1/experimental/dataset/{datasetId}/phylogeny") Call<List<PhylogenyFile>> getPhylogenyFiles(...)`
  - `@GET("/v1/experimental/dataset/{datasetId}/phylogeny/{fileName}") @Streaming Call<ResponseBody> getPhylogenyFile(...)`
- New `ws/metadata/response/PhylogenyFile` POJO with `fileName`, `size`, `format`, `error`,
  `trees`, `treesTruncated` and `isValid`.
- `MetadataServiceClient`: matching methods that reuse `performCall`.
- Skip everything when `config.isBypassRegistry()`.

### 4. Tree reading with BioJava

- Add `org.biojava.thirdparty:forester` (version managed in the root `pom.xml`) to `sdks/core`.
- Supported formats (`NEXUS`, `NEWICK`, named as the registry reports them) are the cases of
  `PhylogeneticTreeReader.createParser`, which returns the configured forester parser. A new format
  (e.g. PhyloXML, which forester also parses) is one new case.
  - The format comes only from the registry's `format` field. There's no content detection: the
    registry already works the format out, and forester's own detection treats anything it doesn't
    recognise as Newick.
  - A missing or unsupported format makes the file `INVALID` with "Unsupported format: X".
  - `TreeParseResult.format` records the format used, for the metadata file.
- New `PhylogeneticTreeReader` in `sdks/core`. It takes the file content and `format`, parses with
  the forester Newick/Nexus parser, and iterates the external nodes of each tree. It returns a
  `TreeParseResult` with:
  - `tips`: a `List<TreeTip>` (nested in `TreeParseResult`), where `TreeTip` holds `fileName`, `treeName`, `tipId` (the node
    name) and the label;
  - `status`: `VALID`, `REPAIRED` or `INVALID`;
  - `repairsApplied`: the names of the repairs that were applied, e.g. `EXPONENT_UNDERSCORE`;
  - `errors`: the parser error messages, both from the first attempt and, if any, the retry.

  These feed the metadata file in step 5.
- **Invalid files: try to fix, otherwise skip with a warning.**
  - Don't trust the registry's `isValid` flag alone. Always try to parse, and fall back to repairs
    only when parsing fails.
  - Flow: parse → on failure apply the known repairs to the text → parse again → if it still
    fails, return status `INVALID` with no tips, and `log.warn` with the dataset, file name and
    parser error. One bad file must not fail the pipeline or stop the other files.
  - Repairs live in `PhylogeneticTreeReader.repair`, which applies each fix directly (a regex and
    its replacement) and records the name of each fix that changed the content. Each one is
    applied only to branch-length tokens (after `:`) so tip labels aren't touched.
    - Known case: the exponent sign of a branch length was replaced by `_` (`1.884795486e_05`).
      The sample file was written by the R package ape, which writes `e-05` correctly, and is
      named `global_tree_renamed.nex`, so a label-renaming script probably replaced `-` with `_`
      across the file. `(\d)[eE]_(\d)` becomes `$1e-$2`.
    - Candidates if forester rejects them: `NaN` branch lengths (present in the sample file, e.g.
      `9321:NaN`), which can be dropped (`:NaN` → empty) since we don't use lengths.
  - Log at `info` when a file needed repairs, so problems upstream stay visible.
- Unit tests with small Newick and Nexus (`TRANSLATE`, several trees) fixtures, plus repair cases:
  `e_05` fixed and parsed (status `REPAIRED`, repair listed), `NaN` lengths, and an unfixable file
  that returns `INVALID` with its errors and no exception.

### 5. Spark step: `PhylogeneticTreePipeline`

New `PhylogeneticTreePipeline.run(spark, config, datasetId, attempt, interpreted)`:

1. **Occurrence side.** Map `interpreted` (`Dataset<Occurrence>`) to rows of `occurrenceId`,
   `fileName`, `treeName`, `tipId`, `scientificName`, `taxonKey` and `matchIssues`:
   - read the extension row from `verbatim` (`ExtendedRecord`), and drop occurrences without it;
   - read the default-checklist `TaxonRecord` from `taxon` (`MultiTaxonRecord`).

   If no occurrence has the extension, return early so we make no registry calls.
2. **Tree side, on the driver.**
   - Call `/phylogeny`.
   - Download **every** file the registry lists for the dataset, not only those named in the
     extension rows, because the tip-ID-only fallback can match tips in any file. Parse each one
     with `PhylogeneticTreeReader` and build a `Dataset<TreeTip>`. Trees are small (around 10k
     tips).
   - Log a warning for files referenced by occurrences but missing in the registry, and for files
     skipped because they couldn't be parsed or repaired. Neither writes a CSV, but both get a
     metadata file (step 5). Rows pointing at a missing file can still be linked through the
     tip-ID-only fallback.
3. **Join**, following the tip matching rules:
   - Left join tips with occurrence rows on `tipId` only.
   - Compute `matchRank`:
     - `1` when `fileName` matches and `treeName` is equal or empty on the row;
     - `2` otherwise.
   - Keep one occurrence per `(fileName, treeName, tipId)` with a window
     `row_number() over (partition by fileName, treeName, tipId order by matchRank, occurrenceId)`.
   - Conflicts: over the same partition, restricted to the candidates at the best rank, count the
     distinct `taxonKey`s. A tip is a conflict only when that count is greater than 1. Collect the
     conflicting tips with their candidate gbifIDs and taxonKeys for the metadata warnings, capped
     at e.g. 100 listed so the file stays small. The count is always exact.
   - Tips without an occurrence: `// TODO taxonomy match on tip label`. For now leave the columns
     empty.
   - Log counts of tips matched at rank 1, tips matched at rank 2, unmatched tips, conflicts, and
     extension rows that didn't link to any tip.
4. **Write to HDFS.** Exactly one CSV file per tree, at
   `{phylogenyConfig.outputPath}/{datasetKey}/{attempt}/{fileName}/{treeName}.csv`, with a header
   and the columns `tipId,scientificName,taxonKey,matchIssues`.
   - Spark writes `coalesce(1)` with header on to a temp directory, e.g.
     `{datasetKey}/{attempt}/{fileName}/_tmp_{treeName}`.
   - Rename the single `part-*.csv` to `{treeName}.csv` with the Hadoop `FileSystem`, overwriting
     any existing file, then delete the temp directory.
5. **Write the metadata file.** One `metadata.json` per tree file, in the same directory as its
   CSVs: `{phylogenyConfig.outputPath}/{datasetKey}/{attempt}/{fileName}/metadata.json`.
   - Written for every tree file referenced by the extension, including skipped ones (`INVALID`,
     `NOT_FOUND`), so problems are visible without reading the logs.
   - Written with the Hadoop `FileSystem` and Jackson (`MAPPER`) from the driver, not Spark.
     Overwrite if it exists.
   - Content (a `PhylogenyFileMetadata` POJO):

     ```json
     {
       "datasetKey": "3a1ca64a-e5bd-49a1-abe1-9af509c1143d",
       "attempt": 12,
       "fileName": "global_tree_renamed.nex",
       "format": "NEXUS",
       "status": "REPAIRED",
       "repairsApplied": ["EXPONENT_UNDERSCORE"],
       "warnings": [
         "File was invalid and had to be repaired: failed to parse number from \"1.884795486e_05\""
       ],
       "trees": [
         {
           "treeName": "UNTITLED",
           "csv": "UNTITLED.csv",
           "tips": 9524,
           "matchedByFile": 9050,
           "matchedByTipIdOnly": 50,
           "unmatchedTips": 424,
           "conflicts": 2
         }
       ],
       "extensionRowsNotLinked": 3,
       "created": "2026-11-12T10:00:00Z"
     }
     ```

   - `status`:
     - `VALID`: parsed on the first attempt;
     - `REPAIRED`: invalid, fixed and parsed; `repairsApplied` and a warning with the original
       error are always present;
     - `INVALID`: couldn't be fixed, so it was skipped; there's no CSV, `trees` is empty, and the
       warnings hold the parser errors;
     - `NOT_FOUND`: referenced by the extension but not listed by the registry `/phylogeny`
       endpoint.
   - `warnings` collects every warning logged for that file (the same messages as `log.warn`), so
     the log and the metadata stay consistent. It includes `TreeParseResult.warnings` from the
     reader, e.g. `Tip label 'Puma_concolor' appears 2 times in tree 'tree_a'` (repeated labels
     are kept once, because tips are matched by label only).

Configuration, following the `DnaConfig` pattern:

- New `PhylogenyConfig` (`sdks/config/.../config/model/PhylogenyConfig.java`, `Serializable`) with:
  - `boolean enabled = false`
  - `String outputPath`: the HDFS base path for the CSVs
- `PipelinesConfig`: add `private PhylogenyConfig phylogenyConfig = new PhylogenyConfig();`.
- Add a `phylogenyConfig:` section to `gbif/ingestion/spark-jobs/pipelines.yaml`, the test
  templates, and the `spark-coordinator` configmap.
- If `enabled` is true but `outputPath` is empty, fail fast with a clear error.

Wiring in `runInterpretation`: call it right after `runTransforms`, guarded by
`config.getPhylogenyConfig().isEnabled()`. It only reads `interpreted`, so the order relative to the
DNA step and the outputs doesn't matter.

- New `OccurrenceInterpretType.PHYLOGENY` plus a case in `handleInterpretTypes` to rerun it alone
  from `SIMPLE_OCCURRENCE`.

### 6. Tests

- `PhylogeneticTreeReader`: Newick, Nexus with `TRANSLATE`, multiple trees, malformed branch
  lengths.
- Registry client: mock server, like the other `ws` clients.
- Occurrence-side mapping: occurrence with and without extension, with and without a taxon match.
- Pipeline test: verbatim fixture with the extension, a local tree file, the registry mocked, and an
  assertion that exactly one `{treeName}.csv` exists under `{datasetKey}/{attempt}/{fileName}/`
  (no temp directory left behind) and on its content (matched and unmatched tips).
- Matching rules:
  - file + tree match;
  - file match with an empty tree name (linked in every tree of the file);
  - wrong or missing file name, linked by tip ID only;
  - a rank-1 row beats a rank-2 row for the same tip;
  - two occurrences at the same rank with the same taxonKey: lowest gbifID wins, no conflict;
  - two occurrences at the same rank with different taxonKeys: lowest gbifID wins, conflict
    counted and warning added;
  - unnamed trees get `tree{n}`.
- Metadata file: `metadata.json` is next to the CSV for `VALID` and `REPAIRED` files (with
  `repairsApplied` and the warning). It is written without a CSV for `INVALID` and `NOT_FOUND`
  files. Its counts match the CSV.
