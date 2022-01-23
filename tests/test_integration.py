from ingestion import DaySummaryAPI
import datetime

class TestDaySummaryApi:
    def test_get_date(self):
        actual = DaySummaryAPI(coin='BTC').get_data(date=datetime.date(2021,1,2))
        print(actual)
        expected = {'date': '2021-01-02', 'opening': 153458.29999999, 'closing': 172189.98444999, 'lowest': 153457.4, 'highest': 174174, 'volume': '71283322.98027414', 'quantity': '428.28441276', 'amount': 28668, 'avg_price': 166439.21855783}
        assert actual == expected

    def test_get_date_better(self):
        actual = DaySummaryAPI(coin='BTC').get_data(date=datetime.date(2021,1,2)).get("date")
        print(actual)
        expected = '2021-01-02'
        assert actual == expected        
