from datetime import datetime
from typing import Callable, List, Optional, Union, Dict

import pandas as pd
import pyarrow
import pytz
from pydantic.typing import Literal

from feast import FileSource, OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import FeastJoinKeysDuringMaterialization
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.infra.provider import (
    _get_requested_feature_views_to_features_dict,
)
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage

from delta.tables import DeltaTable
from pyspark.sql.functions import col
import pyspark.sql.dataframe as sd

def _run_spark_field_mapping(
    table: sd.DataFrame, field_mapping: Dict[str, str],
):
    if field_mapping:
        return table.select([col(c).alias(mapping.get(c, c)) for c in table.columns])
    else
        return table

class SparkOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for local (file-based) store """

    type: Literal[
        "feast_custom_offline_store.spark.SparkOfflineStore"
    ] = "feast_custom_offline_store.spark.SparkOfflineStore"
    """ Offline store type selector"""


class SparkRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
    ):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = (
            on_demand_feature_views if on_demand_feature_views else []
        )

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    @log_exceptions_and_usage
    def _to_df_internal(self) -> pd.DataFrame:
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        return pyarrow.Table.from_pandas(df)


class SparkOfflineStore(OfflineStore):
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        if not isinstance(entity_df, pd.DataFrame) and not isinstance(
            entity_df, dd.DataFrame
        ):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )
        entity_df_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL  # local modifiable copy of global variable
        if entity_df_event_timestamp_col not in entity_df.columns:
            datetime_columns = entity_df.select_dtypes(
                include=["datetime", "datetimetz"]
            ).columns
            if len(datetime_columns) == 1:
                print(
                    f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
                )
                entity_df_event_timestamp_col = datetime_columns[0]
            else:
                raise ValueError(
                    f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
                )
        (
            feature_views_to_features,
            on_demand_feature_views_to_features,
        ) = _get_requested_feature_views_to_features_dict(
            feature_refs,
            feature_views,
            registry.list_on_demand_feature_views(config.project),
        )

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():

            spark.conf.set("spark.sql.session.timeZone", "UTC")
            # Create a copy of entity_df to prevent modifying the original
            entity_df_with_features = spark.createDataFrame(entity_df)

            # Sort event timestamp values
            entity_df_with_features = entity_df_with_features.orderBy(
                col(entity_df_event_timestamp_col)
            )

            # Load feature view data from sources and join them incrementally
            for feature_view, features in feature_views_to_features.items():
                event_timestamp_column = (
                    feature_view.batch_source.event_timestamp_column
                )
                created_timestamp_column = (
                    feature_view.batch_source.created_timestamp_column
                )

                # Build a list of entity columns to join on (from the right table)
                join_keys = []
                for entity_name in feature_view.entities:
                    entity = registry.get_entity(entity_name, project)
                    join_key = feature_view.projection.join_key_map.get(
                        entity.join_key, entity.join_key
                    )
                    join_keys.append(join_key)

                right_entity_key_columns = [
                    event_timestamp_column,
                    created_timestamp_column,
                ] + join_keys
                right_entity_key_columns = [c for c in right_entity_key_columns if c]

                # feature_view.batch_source.s3_endpoint_override

                df_to_join = DeltaTable.forPath(feature_view.batch_source.path)

                # Build a list of all the features we should select from this source
                feature_names = []
                columns_map = {}
                for feature in features:
                    # Modify the separator for feature refs in column names to double underscore. We are using
                    # double underscore as separator for consistency with other databases like BigQuery,
                    # where there are very few characters available for use as separators
                    if full_feature_names:
                        formatted_feature_name = (
                            f"{feature_view.projection.name_to_use()}__{feature}"
                        )
                    else:
                        formatted_feature_name = feature
                    # Add the feature name to the list of columns
                    feature_names.append(formatted_feature_name)

                # Ensure that the source dataframe feature column includes the feature view name as a prefix
                df_to_join = _run_spark_field_mapping(df_to_join, columns_map)

                # Select only the columns we need to join from the feature dataframe
                df_to_join = df_to_join.select([col(c) for c in right_entity_key_columns + feature_names])

                # Get only data with requested entities
                ttl.total_seconds()

                df_to_join = entity_df_with_features.join(df_to_join, join_keys, "left").filter(
                    ( col(event_timestamp_column) >= col(entity_df_event_timestamp_col) - expr(f"INTERVAL {ttl_seconds} seconds") ) &
                    ( col(event_timestamp_column) <= col(entity_df_event_timestamp_col) )
                )

                if created_timestamp_column:
                    df_to_join = df_to_join.orderBy(col(created_timestamp_column).desc(), col(event_timestamp_column).desc())
                else:
                    df_to_join = df_to_join.orderBy(col(event_timestamp_column).desc())

                df_to_join = df_to_join.dropDuplicates([join_keys])

                # Rename columns by the field mapping dictionary if it exists
                if feature_view.batch_source.field_mapping is not None:
                    df_to_join = _run_spark_field_mapping(
                        df_to_join, feature_view.batch_source.field_mapping
                    )
                # Rename entity columns by the join_key_map dictionary if it exists
                if feature_view.projection.join_key_map:
                    df_to_join = _run_spark_field_mapping(
                        df_to_join, feature_view.projection.join_key_map
                    )

                entity_df_with_features = df_to_join.drop(event_timestamp_column)

                # Ensure that we delete dataframes to free up memory
                del df_to_join

            return entity_df_with_features.persist()

        job = SparkRetrievalJob(
            evaluation_function=evaluate_historical_retrieval,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
        )
        return job

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(data_source, FileSource)

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_offline_job():

            storage_options = (
                {
                    "client_kwargs": {
                        "endpoint_url": data_source.s3_endpoint_override
                    }
                }
                if data_source.s3_endpoint_override
                else None
            )

            source_df = dd.read_parquet(
                data_source.path, storage_options=storage_options
            )

            source_df_types = source_df.dtypes
            event_timestamp_column_type = source_df_types[event_timestamp_column]

            if created_timestamp_column:
                created_timestamp_column_type = source_df_types[
                    created_timestamp_column
                ]

            if (
                not hasattr(event_timestamp_column_type, "tz")
                or event_timestamp_column_type.tz != pytz.UTC
            ):
                source_df[event_timestamp_column] = source_df[
                    event_timestamp_column
                ].apply(
                    lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc),
                    meta=(event_timestamp_column, "datetime64[ns, UTC]"),
                )

            if created_timestamp_column and (
                not hasattr(created_timestamp_column_type, "tz")
                or created_timestamp_column_type.tz != pytz.UTC
            ):
                source_df[created_timestamp_column] = source_df[
                    created_timestamp_column
                ].apply(
                    lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc),
                    meta=(event_timestamp_column, "datetime64[ns, UTC]"),
                )

            source_df = source_df.persist()

            source_columns = set(source_df.columns)
            if not set(join_key_columns).issubset(source_columns):
                raise FeastJoinKeysDuringMaterialization(
                    data_source.path, set(join_key_columns), source_columns
                )

            ts_columns = (
                [event_timestamp_column, created_timestamp_column]
                if created_timestamp_column
                else [event_timestamp_column]
            )

            source_df = source_df.sort_values(by=event_timestamp_column)

            source_df = source_df[
                (source_df[event_timestamp_column] >= start_date)
                & (source_df[event_timestamp_column] < end_date)
            ]

            source_df = source_df.persist()

            columns_to_extract = set(
                join_key_columns + feature_name_columns + ts_columns
            )
            if join_key_columns:
                source_df = source_df.drop_duplicates(
                    join_key_columns, keep="last", ignore_index=True
                )
            else:
                source_df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                columns_to_extract.add(DUMMY_ENTITY_ID)

            source_df = source_df.persist()

            return source_df[list(columns_to_extract)].persist()

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return SparkRetrievalJob(
            evaluation_function=evaluate_offline_job, full_feature_names=False,
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob: ...

