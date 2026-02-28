import sys
import os
import unittest
from unittest.mock import patch, MagicMock

# Mock Airflow imports before they are loaded
mock_airflow = MagicMock()
sys.modules["airflow"] = mock_airflow
sys.modules["airflow.operators.python"] = mock_airflow
sys.modules["airflow.providers.google.cloud.transfers.local_to_gcs"] = mock_airflow
sys.modules["airflow.providers.google.cloud.operators.bigquery"] = mock_airflow

# Add the directory containing the DAG to the path
sys.path.append(os.path.abspath('nyc_taxi_pipeline/airflow'))

import pandas as pd
import nyc_taxi_pipeline

class TestNycTaxiPipeline(unittest.TestCase):

    @patch('requests.get')
    def test_download_taxi_data(self, mock_get):
        # Prepare mock response
        mock_response = MagicMock()
        mock_response.content = b"fake parquet data"
        mock_get.return_value = mock_response
        
        # Override constants for testing
        nyc_taxi_pipeline.MONTHS = ['2023-01']
        nyc_taxi_pipeline.DOWNLOAD_DIR = '/tmp/test_download'
        
        # Run the task
        result = nyc_taxi_pipeline.download_taxi_data()
        
        # Verify
        self.assertEqual(result, '/tmp/test_download')
        self.assertTrue(os.path.exists('/tmp/test_download/yellow_tripdata_2023-01.parquet'))
        mock_get.assert_called_once()
        
        # Cleanup
        if os.path.exists('/tmp/test_download/yellow_tripdata_2023-01.parquet'):
            os.remove('/tmp/test_download/yellow_tripdata_2023-01.parquet')

    @patch('pandas.read_parquet')
    @patch('pandas.DataFrame.to_parquet')
    def test_preprocess_data(self, mock_to_parquet, mock_read_parquet):
        # Prepare mock data
        df = pd.DataFrame({
            'fare_amount': [10.0, -5.0, 20.0],
            'trip_distance': [1.0, 2.0, 3.0]
        })
        mock_read_parquet.return_value = df
        
        # Prepare mock TaskInstance
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = '/tmp/test_download'
        
        # Override constants
        nyc_taxi_pipeline.MONTHS = ['2023-01']
        nyc_taxi_pipeline.DOWNLOAD_DIR = '/tmp/test_download'
        
        # Run the task
        result = nyc_taxi_pipeline.preprocess_data(ti=mock_ti)
        
        # Verify
        self.assertEqual(result, '/tmp/test_download')
        # Check that negative fare amounts were filtered out
        self.assertTrue(mock_to_parquet.called)

if __name__ == '__main__':
    unittest.main()
