# Parquet Summary

Summarise metadata of a parquet file.

## What this tool is not

**This tool is not fit for purpose**. I don't care what your purpose is, this isn't it. It is being written as a way for
me to play around with C++, and because I often find myself needing a tool such as this. I _think_ it works, more or
less, but make no guarantees of this. Use at your own risk.

## Example

```shell
./parquet_summary test_parquet_file.parquet
File: data/test_parquet_file.parquet
Rows: 100000000
Columns: 4
Created by: parquet-cpp-arrow version 23.0.0

 Column  PhysicalType NullCount   Min     Max   Chunks ChunksMissingMinMax ChunksMissingNullCount
doubles     DOUBLE        0     0.00000 0.99999   96            0                    0
integers    INT64         0      -100     99      96            0                    0
strings   BYTE_ARRAY      0       Cat     Dog     96            0                    0
booleans   BOOLEAN        0        0       1      96            0                    0
```

## Wishlist

There are several features that I'd like to add. These are:

### Support of more platforms

This has been built and tested on my local machine (using Linux). I'd like to get this working on Apple and Windows
devices too. This shouldn't be too much additional work as the utility is quite small at the moment.

### Split into multiple files

I find having a single ``main.cpp`` file quite messy. This should be split into several smaller files

### Support all Physical Types

Parquet supports more physical types than I've implemented. Most importantly is ``INT96``, which is used for nanosecond
datetime types. This has been skipped as it doesn't map to a native C++ type and I still need to think about how to
implement this nicely.

### Support all Logical Types

Parquet distinguishes Physical Types (how the data is stored) from Logical Types (how the data should be interpreted).
Currently ``parquet_summary`` does not implement any functionality regarding Logical Types. This might be changed in
the future as these features are useful.

### More comprehensive statistics

It would be useful for me in the future to have better descriptive statistics of the data. This requires reading the
actual data (not just the metadata) which will be much slower. Because of this, these should only be included iff they
are explicitly requested.

## Building from source

### Installing the ``arrow`` and ``parquet`` parquet

Here is the [official guide](https://arrow.apache.org/install/), for those who  don't want to read the details, here
is how I installed the libraries:

```shell
sudo apt update
sudo apt install -y -V ca-certificates lsb-release wget
wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-dev libparquet-dev
```

This has been tested on Ubuntu 24.04.

### Building using CMake

First set up a build directory with the following command:

```shell
cmake -B build
```

Then build the project using:

```shell
cmake --build build
```

This builds an executable ``parquet_summary`` in the ``build`` directory. Which can now be called on your parquet file
of need:

```shell
build/parquet_summary your_file.parquet
```
