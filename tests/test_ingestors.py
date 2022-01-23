import datetime
import pytest
from ingestion import DataIngestor, DataWriter
from unittest.mock import mock_open, patch


@pytest.fixture
@patch("ingestion.DataIngestor.__abstractmethods__", set())
def data_ingestor_fixture():
    return  DataIngestor(
            writer = DataWriter,
            coins=['TEST', 'HOW'],
            default_start_date=datetime.date(2021,6,21)
        )


@patch('ingestion.DataIngestor.__abstractmethods__', set())
class TestIngestors:
    def test_checkpoint_filename(self, data_ingestor_fixture):
        actual = data_ingestor_fixture._checkpoint_filename

        expected = 'DataIngestor._checkpoint'
        assert actual == expected

    
    def test_load_checkpoint_no_checkpoint(self, data_ingestor_fixture):
        actual = data_ingestor_fixture._load_checkpoint()    
        expected = None
        assert actual == expected

    @patch("builtins.open", new_callable=mock_open, read_data="2021-06-25")
    def test_load_checkpoint_existing_checkpoint(self, mock, data_ingestor_fixture):
        actual = data_ingestor_fixture._load_checkpoint()    
        expected = datetime.date(2021,6,25)
        assert actual == expected        

    @patch("ingestion.DataIngestor._write_checkpoint", return_value=None)
    def test_update_checkpoint_updated(self, mock, data_ingestor_fixture):
        data_ingestor = data_ingestor_fixture

        data_ingestor._update_checkpoint(value=datetime.date(2019,1,1))
        expected = datetime.date(2019,1,1)
        assert data_ingestor._checkpoint == expected


    @patch("ingestion.DataIngestor._write_checkpoint", return_value=None)
    def test_update_checkpoint_write(self, mock, data_ingestor_fixture):
        data_ingestor = data_ingestor_fixture

        data_ingestor._update_checkpoint(value=datetime.date(2019,1,1))
        mock.assert_called_once()


    @patch("builtins.open", new_callable=mock_open, read_data="2021-06-25")   
    @patch("ingestion.DataIngestor._checkpoint_filename", return_value="foobar.checkpoint") 
    def test_write_checkpoint(self, mock_checkpoint_filename, mock_open_file, data_ingestor_fixture):
        data_ingestor = data_ingestor_fixture
        data_ingestor._write_checkpoint()
        mock_open_file.assert_called_with(mock_checkpoint_filename,'w')
