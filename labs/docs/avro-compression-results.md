## Avro Compression Test Results
Avro is benchmarked for its compression. Different compression codecs and sync intervals are tried to compare with read speed, write speed and compression size.

### Introduction

#### Terminology
1. Compression codec: Avro compresses the write using various codecs like deflate(1-9),snappy,bzip2 etc. This test runs it against deflate and snappy.
2. sync Intervals: The approximate number of uncompressed bytes to write in each file block. Values should be between 2 KB to 2 MB.
3. Read Time: Time required to read the compressed avro file generated after write step. Evaluated in milli second.
4. Write Time: Time required to convert DwC to Avro using provided compression codec and sync intervals. Evaluated in millisecond.
5. Compression Size : File size of compressed avro obtained after write.

#### System Configuration
Test was performed on :
1. CPU : 2.8 GHz i7 (4 CPU, 8 Cores)
2. Memory: 16 GB
3. Java - 1.8.0_151
4. Heap Size: Xmx768m

All tests are run on single thread and average of 3 runs are taken.

#### Results
Results for 50K datasets are shown below:

<table border="0">
<tr><th>Dataset</th><th>syncInterval</th><th>originalsize</th><th>compressedsize</th><th>read(ms)</th><th>write(ms)</th><th>codec</th><th>occurrences</th></tr>
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>14.33MB</td><td>894.4</td><td>1920.0</td><td>deflate-1</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>14.27MB</td><td>891.6</td><td>1944.0</td><td>deflate-1</td><td>50316</td></tr>
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>13.36MB</td><td>866.0</td><td>1931.33</td><td>deflate-2</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>13.30MB</td><td>872.66</td><td>1919.0</td><td>deflate-2</td><td>50316</td></tr>   
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>12.21MB</td><td>861.66</td><td>1923.0</td><td>deflate-3</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>12.15MB</td><td>858.34</td><td>1888.67</td><td>deflate-3</td><td>50316</td></tr>
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>11.91MB</td><td>879.34</td><td>2453.35</td><td>deflate-4</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>11.84MB</td><td>870.0</td><td>2381.35</td><td>deflate-4</td><td>50316</td></tr>  
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>10.43MB</td><td>842.0</td><td>2436.0</td><td>deflate-5</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>10.36MB</td><td>855.66</td><td>2479.35</td><td>deflate-5</td><td>50316</td></tr>
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>9.58MB</td><td>809.66</td><td>2598.0</td><td>deflate-6</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>9.51MB</td><td>818.0</td><td>2607.65</td><td>deflate-6</td><td>50316</td></tr>
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>9.50MB</td><td>823.0</td><td>2668.35</td><td>deflate-7</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>9.42MB</td><td>821.0</td><td>2644.35</td><td>deflate-7</td><td>50316</td></tr>
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>9.25MB</td><td>845.0</td><td>3569.65</td><td>deflate-8</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>9.17MB</td><td>776.0</td><td>3387.35</td><td>deflate-8</td><td>50316</td></tr>
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>9.24MB</td><td>784.34</td><td>4742.3</td><td>deflate-9</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>9.17MB</td><td>900.0</td><td>4735.66</td><td>deflate-9</td><td>50316</td></tr>
<tr><td>50K</td><td>1048576</td><td>46.85MB</td><td>23.80MB</td><td>574.0</td><td>1498.33</td><td>snappy</td><td>50316</td></tr>
<tr><td>50K</td><td>2097152</td><td>46.85MB</td><td>23.78MB</td><td>571.66</td><td>1482.33</td><td>snappy</td><td>50316</td></tr>
</table> 

#### Analysis and Conclusion
Deflate codec compresses the data well when the compared from 1-9 from 14.33MB in deflate-1 to 9.17MB in deflate-9 for a 46.85MB file. But the write time also increases from 1920ms deflate-1 to 4742ms. However, Snappy performance decently with compression i.e. around 23.80MB from dataset of same size which is around 50% of original file size. Interestingly snappy also has far better read and write time compared to any deflate version i.e. 571 ms and 1482ms respectively for read and write respectively.
This test conclude that if the compression size is key criteria then deflate-9 can be  a better choice. However, if we can compromise a little on compression size, using snappy can get faster read and write. 

#### Running Utility against any dataset
The test could be reproduced on different dataset using the provided utility. 
Following is the step to run the AvroCompressionTestUtility.
java -cp labs-1.0-SNAPSHOT-shaded.jar org.gbif.pipelines.labs.performance.AvroCompressionTestUtility <path/to/dataset> </path/to/result.csv> <repetition eg.2>

> Note: Entire test result with different datasets are available as avro-compression-test.csv
