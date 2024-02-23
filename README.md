![logo](ldbc-logo.png)

# FinBench DataGen SPG-Extension

The LDBC FinBench Data Generator (Datagen) produces the datasets for the [LDBC FinBench's workloads](https://ldbcouncil.org/benchmarks/finbench/). This project extends [LDBC FinBench Datagen](https://github.com/ldbc/ldbc_finbench_datagen) for [SPG](https://github.com/OpenSPG/openspg)(semantic-enhanced property graph)

## Extension Implementation

### Extended Data Schema

We extended data schema following best practices for knowledge modeling in [OpenSPG document](https://openspg.yuque.com/ndx6g9/ns5nw2/gdaiwlgb8e7ms68s).

![Extended Schema](https://raw.githubusercontent.com/FessGo/knowledge-graph-warehouse-bench/main/dataset/ldbc_finbench_datagen_spg_extension/extended-data-schema.png)

### Extended Data Generator Implementation

We extended the data generator following the extended data schema as well, built-in dictionaries are utilized for data generation.

## Quick Start

### Pre-requisites & Workflow

Please refer to original datagen project.

### Generation of Data

- Deploy Spark
  - use `scripts/get-spark-to-home.sh` to download pre-built spark to home directory and then decompress it.
  - Set the PATH environment variable to include the Spark binaries.
- Build the project
  - run `mvn clean package -DskipTests` to package the artifacts.
- Run locally and generate lpg(labeled property graph) dataset
  - Run `sh script/run_local_lpg.sh`
- Run locally and generate spg(semantic-enhanced property graph) dataset
  - Run `sh script/run_local_spg.sh`
- Run in cloud or cluster is not supported for now.

# Related Work

- LDBC FinBench DataGen https://github.com/ldbc/ldbc_finbench_datagen
- OpenSPG: https://github.com/OpenSPG/openspg

 
