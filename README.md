# Feast on Spark

This repo adds possibility to run [Feast](https://github.com/feast-dev/feast) on spark.

## Get started
### Install feast:
```shell
pip install feast
```

### Install feast-postgres:
```shell
git clone https://github.com/qooba/feast-pyspark.git
cd feast-pyspark
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
    type: feast_pyspark.SparkOfflineStore
    spark_conf:
        spark.master: "local[*]"
        spark.ui.enabled: "false"
        spark.eventLog.enabled: "false"
        spark.sql.session.timeZone: "UTC"
```

### Example

TODO

## References

[feast-spark-offline-store](https://github.com/Adyen/feast-spark-offline-store/) - spark configuration and session

[feast-postgres](https://github.com/nossrannug/feast-postgres) - parts of Makefiles and github workflows

