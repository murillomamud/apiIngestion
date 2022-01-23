import datetime
import pytest
from unittest.mock import mock_open, patch
import requests
from ingestion import DaySummaryAPI, MercadoBitcoinApi, TradesApi


class TestDaySummaryApi:

    @pytest.mark.parametrize("coin, date, expected", 

    [

        ('BTC', datetime.date(2021,6,21), 'https://www.mercadobitcoin.net/api/BTC/day-summary/2021/6/21'),
        ('ETH', datetime.date(2021,6,21), 'https://www.mercadobitcoin.net/api/ETH/day-summary/2021/6/21'),
        ('ETH', datetime.date(2019,1,2), 'https://www.mercadobitcoin.net/api/ETH/day-summary/2019/1/2')

    ])

    def test_get_endpoint(self, coin, date, expected):
        api = DaySummaryAPI(coin=coin)
        actual = api._get_endpoint(date=date)
        assert actual == expected


class TestTradesApi:

    @pytest.mark.parametrize("coin, date_from, date_to, expected", 

    [

        ("TEST", datetime.datetime(2019,1,1), datetime.datetime(2019,1,2),'https://www.mercadobitcoin.net/api/TEST/trades/1546308000/1546394400' ),
        ("TEST", datetime.datetime(2019,6,12), datetime.datetime(2019,6,15),'https://www.mercadobitcoin.net/api/TEST/trades/1560308400/1560567600' ),
        ("TEST", None, None,'https://www.mercadobitcoin.net/api/TEST/trades' ),
        ("TEST", None, datetime.datetime(2019,6,15),'https://www.mercadobitcoin.net/api/TEST/trades' ),
        ("TEST", datetime.datetime(2019,6,15), None,'https://www.mercadobitcoin.net/api/TEST/trades/1560567600' ),        

    ])

    def test_get_endpoint(self, coin, date_from, date_to, expected):
        api = TradesApi(coin=coin)
        actual = api._get_endpoint(date_from=date_from, date_to=date_to)
        assert actual == expected


    def test_get_endpoint_date_from_gt_date_to(self):
        with pytest.raises(RuntimeError):
            TradesApi(coin="TEST")._get_endpoint(datetime.datetime(2021,6,15), datetime.datetime(2021,6,12))


    @pytest.mark.parametrize(
        "date, expected",
        [
            (datetime.datetime(2019,1,1), 1546308000)
        ]
    )
    def test_get_unix_epoch(self, date, expected):
        actual = TradesApi(coin='TEST')._get_unix_epoch(date)
        assert actual == expected

@pytest.fixture()
@patch("ingestion.MercadoBitcoinApi.__abstractmethods__", set())
def fixture_mercado_bitcoin_api():
    return MercadoBitcoinApi(
            coin='test'
          )



def mocked_requests_get(*args, **kwargs):
    class MockResponse(requests.Response):
        def __init__(self, json_data, status_code):
            super().__init__()
            self.status_code = status_code
            self.json_data = json_data

        def json(self):
            return self.json_data

        def raise_for_status(self) -> None:
            if self.status_code != 200:
                raise Exception
    
    if(args[0] == 'valid_endpoint'):
       return MockResponse(json_data={"foo":"bar"}, status_code=200)
    else:
       return MockResponse(json_data=None, status_code=404)


#@patch("ingestion.MercadoBitcoinApi.__abstractmethods__", set())
class TestMercadoBitcoinApi:


    @patch("requests.get")
    @patch("ingestion.MercadoBitcoinApi._get_endpoint", return_value="valid_endpoint")
    def test_get_data(self, mock_get_endpoint, mock_requests, fixture_mercado_bitcoin_api):
        fixture_mercado_bitcoin_api.get_data()
        mock_requests.assert_called_once_with('valid_endpoint')

    @patch("requests.get", side_effect=mocked_requests_get)
    @patch("ingestion.MercadoBitcoinApi._get_endpoint", return_value='valid_endpoint')
    def test_get_data_with_valid_endpoint(self, mock_get_endpoint, mock_requests,fixture_mercado_bitcoin_api):
        actual = fixture_mercado_bitcoin_api.get_data()
        expected = {"foo":"bar"}
        assert actual == expected


    @patch("requests.get", side_effect=mocked_requests_get)
    @patch("ingestion.MercadoBitcoinApi._get_endpoint", return_value='invalid_endpoint')
    def test_get_data_with_invalid_endpoint(self, mock_get_endpoint, mock_requests,fixture_mercado_bitcoin_api):
        with pytest.raises(Exception):
            fixture_mercado_bitcoin_api.get_data()