# Introduction to Celeborn's Java Columnar Shuffle

## Overview

Celeborn presents a Java Columnar Shuffle designed to enhance performance and efficiency in SparkSQL and DataFrame operations. This innovative approach leverages a columnar format for shuffle operations, achieving a higher compression rate than traditional Row-based Shuffle methods. This improvement leads to significant savings in disk space usage during shuffle operations.

## Benefits

- **High Compression Rate**: By organizing data into a columnar format, this feature significantly increases the compression ratio, reducing the disk space required for Shuffle data.

## Configuration

To leverage Celeborn's Java Columnar Shuffle, you need to apply a patch and configure certain settings in Spark 3.x. Follow the steps below for implementation:

### Step 1: Apply the Patch

1. Obtain the `Celeborn_Columnar_Shuffle_spark3.patch` file that contains the modifications needed for enabling Columnar Shuffle in Spark 3.x.
2. Navigate to your Spark source directory.
3. Apply the patch.

### Step 2: Configure Celeborn Settings

To enable Columnar Shuffle, adjust the following configurations in your Spark application:
Open the Spark configuration file or set these parameters in your Spark application.
Add the following configuration settings:

```
spark.celeborn.columnarShuffle.enabled true
spark.celeborn.columnarShuffle.encoding.dictionary.enabled true
```

If you require further performance optimization, consider enabling code generation with:

```
spark.celeborn.columnarShuffle.codegen.enabled true
```
