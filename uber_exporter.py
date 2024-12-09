from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_big_query(data: dict, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    for table_name, table_data in data.items():
        print(f"Exporting table: {table_name}, Type: {type(table_data)}")
        if not isinstance(table_data, pd.DataFrame):
            print(f"Skipping {table_name} as it is not a DataFrame.")
            continue

        table_id = f'uber_pro_dataset.{table_name}'
        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            table_data,
            table_id,
            if_exists='replace',  # Replace if table exists
        )
        print(f"Exported {table_name} successfully!")
