# Partial reinterpretation

For example, when changes to interpreting required a selection of datasets to be reinterpreted, or when datasets have been added/removed from a network *en masse*.

1. Identify the required datasets.
   a. Example for up to 1000 datasets in a network:
      ```sh
      curl -Ss 'https://api.gbif.org/v1/network/99d66b6c-9087-452f-a9d4-f15f2c2d0e7e/constituents?limit=1000' | jq '{ datasetsToInclude: [ .results[].key ] }'
      ```
   b. Example from a text file of dataset keys:
      ```
      cat keys | jq -Rsn '{ datasetsToInclude: [ inputs | . / "\n" ][0] }'
      ```

2. Send the request:
   ```
   $command_from_step_1 | \
       curl -u user:password -H 'Content-Type: application/json' -X POST -d@- 'https://api.gbif.org/v1/pipelines/history/run?steps=VERBATIM_TO_INTERPRETED&useLastSuccessful=true&reason=Reprocessing+reason+here'
