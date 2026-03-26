import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.pipeline.exporter import export_data
from src.pipeline.cli import main


class TestExporter:
    def test_export_csv(self):
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        filepath = "output.csv"

        with patch.object(pd.DataFrame, 'to_csv') as mock_to_csv:
            export_data(df, filepath, format='csv')
            mock_to_csv.assert_called_once_with(filepath, index=False)

    def test_export_parquet(self):
        df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        filepath = "output.parquet"

        with patch.object(pd.DataFrame, 'to_parquet') as mock_to_parquet:
            export_data(df, filepath, format='parquet')
            mock_to_parquet.assert_called_once_with(filepath, index=False)

    def test_export_invalid_format(self):
        df = pd.DataFrame({'a': [1, 2]})
        with pytest.raises(ValueError, match="Unsupported format"):
            export_data(df, "out.txt", format="txt")


class TestCLI:
    @patch('src.pipeline.cli.load_config')
    @patch('src.pipeline.cli.process_rainfall_data')
    @patch('src.pipeline.cli.export_data')
    @patch('src.pipeline.cli.GEEClient')
    @patch('src.pipeline.cli.CHIRPSProvider')
    @patch('src.pipeline.cli.GPMProvider')
    @patch('src.pipeline.cli.load_aoi')
    @patch('sys.argv', ['pipeline', '--provider', 'chirps', '--start-date', '2020-01-01', '--end-date', '2020-01-02', '--output', 'out.csv', '--aoi', 'aoi.geojson'])
    def test_cli_chirps_flow(self, mock_load_aoi, mock_gpm, mock_chirps, mock_gee, mock_export, mock_process, mock_config):
        # Setup mocks
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance
        mock_config_instance.aoi.geojson_path = 'aoi.geojson'
        mock_config_instance.date_range.start_date = '2020-01-01'
        mock_config_instance.date_range.end_date = '2020-01-02'

        mock_provider_instance = MagicMock()
        mock_chirps.return_value = mock_provider_instance
        mock_provider_instance.get_rainfall_data.return_value = [{'date': '2020-01-01', 'precipitation': 10}]

        mock_df = pd.DataFrame([{'date': '2020-01-01', 'precipitation': 10}])
        mock_process.return_value = mock_df

        mock_aoi_geometry = MagicMock()
        mock_load_aoi.return_value = mock_aoi_geometry

        main()

        # Assertions
        mock_config.assert_called_once()
        mock_load_aoi.assert_called_once_with('aoi.geojson')
        mock_chirps.assert_called_once()
        mock_gee.return_value.fetch_in_chunks.assert_called_once()
        mock_process.assert_called_once()
        mock_export.assert_called_once_with(mock_df, 'out.csv', 'csv')

    @patch('src.pipeline.cli.load_config')
    @patch('src.pipeline.cli.process_rainfall_data')
    @patch('src.pipeline.cli.export_data')
    @patch('src.pipeline.cli.GEEClient')
    @patch('src.pipeline.cli.GPMProvider')
    @patch('src.pipeline.cli.load_aoi')
    @patch('sys.argv', ['pipeline', '--provider', 'gpm', '--output', 'out.parquet', '--format', 'parquet', '--aoi', 'aoi.geojson', '--start-date', '2020-01-01', '--end-date', '2020-01-02'])
    def test_cli_gpm_flow(self, mock_load_aoi, mock_gpm, mock_gee, mock_export, mock_process, mock_config):
        # Setup mocks
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance
        mock_config_instance.aoi.geojson_path = 'aoi.geojson'
        mock_config_instance.date_range.start_date = '2020-01-01'
        mock_config_instance.date_range.end_date = '2020-01-02'

        mock_provider_instance = MagicMock()
        mock_gpm.return_value = mock_provider_instance
        mock_provider_instance.get_rainfall_data.return_value = []

        mock_df = pd.DataFrame()
        mock_process.return_value = mock_df

        mock_aoi_geometry = MagicMock()
        mock_load_aoi.return_value = mock_aoi_geometry

        main()

        # Assertions
        mock_gpm.assert_called_once()
        mock_load_aoi.assert_called_once_with('aoi.geojson')
        mock_export.assert_called_once_with(mock_df, 'out.parquet', 'parquet')
