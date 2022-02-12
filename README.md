# Feast on Spark

This repo adds possibility to run [Feast](https://github.com/feast-dev/feast) on spark.

## Get started
### Install feast:
```shell
pip install feast
```

### Install feast-postgres:
```shell
git clone https://github.com/qooba/feast-spark.git
cd feast-spark
pip install -e .
```

### Create a feature repository:
```shell
feast init feature_repo
cd feature_repo
```

### Offline store:
To configure the offline store edit `feature_store.yaml`
```yaml
project: feature_repo
registry: data/registry.db
provider: local
online_store:
    ...
offline_store:
    type: feast_spark.SparkOfflineStore # MUST be this value
```

### Example

TODO

## References

[feast-postgres](https://github.com/nossrannug/feast-postgres) - I have used parts of Makefiles and github workflows
