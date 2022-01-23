from abc import ABC, abstractmethod
from typing import List
import requests
import logging
import datetime
import json
import os
from schedule import repeat, every, run_pending
import time
from backoff import on_exception, expo
import ratelimit


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class MercadoBitcoinApi(ABC):

    def __init__(self, coin:str) -> None:
        self.coin = coin
        self.baseEndpoint = "https://www.mercadobitcoin.net/api"

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        pass

    @on_exception(expo,ratelimit.exception.RateLimitException, max_tries=10)
    @ratelimit.limits(calls=29, period=30) 
    @on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
    def get_data(self, **kwargs) -> dict:
        endpoint = self._get_endpoint(**kwargs)
        logger.info(f"getting data from endpoint: {endpoint}")
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.json()


class DaySummaryAPI(MercadoBitcoinApi):

    type = "day-summary"

    def _get_endpoint(self, date: datetime.date) -> str:
        return f"{self.baseEndpoint}/{self.coin}/{self.type}/{date.year}/{date.month}/{date.day}"



class TradesApi(MercadoBitcoinApi):
    type = 'trades'

    def _get_unix_epoch(self, date:datetime) -> int: 
        return int(date.timestamp())


    def _get_endpoint(self, date_from: datetime.datetime = None, date_to:datetime.datetime = None) -> str:
        if date_from and not date_to:

            unix_date_from = self._get_unix_epoch(date_from)            
            endpoint = f"{self.baseEndpoint}/{self.coin}/{self.type}/{unix_date_from}"

        elif date_from and date_to: 
            if date_from > date_to:
                raise RuntimeError('Date from cannot be > than date-to')
            unix_date_to = self._get_unix_epoch(date_to)            
            unix_date_from = self._get_unix_epoch(date_from)            
            endpoint = f"{self.baseEndpoint}/{self.coin}/{self.type}/{unix_date_from}/{unix_date_to}"            


        else:

            endpoint = f"{self.baseEndpoint}/{self.coin}/{self.type}"

        return endpoint

class DataTypeNotSupportedException(Exception):
    def __init__(self, data):
        self.data = data 
        self.message = f"Data Type {type(data)} is not supported for ingestion"
        super().__init__(self.message)


class DataWriter:
    def __init__(self, coin:str, api:str) -> None:
        self.coin = coin 
        self.api = api 
        self.filename = f"{self.api}/{self.coin}/{datetime.datetime.now()}.json"

    
    def _write_row(self, row:str) -> None: 
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        with open(self.filename, 'a') as f: 
            f.write(row)

    
    def write(self, data: [List, dict]):
        if isinstance(data,dict):
            self._write_row(json.dumps(data) + "\n")

        elif isinstance(data, List): 
            for element in data: 
                self.write(element)
        
        else:

            raise DataTypeNotSupportedException(data)

        

class DataIngestor(ABC):
    def __init__(self, writer: DataWriter, coins: List[str], default_start_date: datetime.datetime) -> None:
        self.coins = coins
        self.default_start_date = default_start_date
        self.writer = writer
        self._checkpoint = self._load_checkpoint()
    
    @property
    def _checkpoint_filename(self) -> str: 
        return f"{self.__class__.__name__}._checkpoint"


    def _write_checkpoint(self):
        with open(self._checkpoint_filename, 'w') as f:
            f.write(f"{self._checkpoint}")


    def _load_checkpoint(self) -> datetime.date:
        try:
            with open(self._checkpoint_filename, 'r') as f:
                return datetime.datetime.strptime(f.read(), "%Y-%m-%d").date()
        except FileNotFoundError:
            return None


    def _get_checkpoint(self):
        if not self._checkpoint:
            return self.default_start_date
        else: 
            return self._checkpoint

    def _update_checkpoint(self, value):
        self._checkpoint = value 
        self._write_checkpoint()



    @abstractmethod
    def ingest(self) -> None:
        pass



class DaySummaryIngestor(DataIngestor):
    def __init__(self,writer:DataWriter, coins:List[str], default_start_date:datetime.date):
        super().__init__(writer, coins, default_start_date)
        

    def ingest(self) -> None:
        date = self._get_checkpoint()
        if date < datetime.date.today(): 
            for coin in self.coins:
                api = DaySummaryAPI(coin=coin)                
                data = api.get_data(date=date)
                self.writer(coin=coin, api=api.type).write(data)
                #update date
            self._update_checkpoint(date + datetime.timedelta(days=1))


#writer = DataWriter('day_summary_new.json')
ingestor = DaySummaryIngestor(writer = DataWriter, coins = ['BTC', 'ETH', 'LTC'], default_start_date=datetime.date(2021,6,1))

#@repeat(every(1).seconds)
#def job(): 
    #ingestor.ingest()


#while True:
    #run_pending()
    #time.sleep(0.5)

#data = DaySummaryAPI("BTC").get_data(date=datetime.date(2021,6,22))
#writer = DataWriter('day_summary.json')
#writer.write(data)



#data = TradesApi("BTC").get_data()
#writer = DataWriter('trades.json')
#writer.write(data)


#print(TradesApi("BTC").get_data())
#print(TradesApi("BTC").get_data(date_from=datetime.datetime(2021,6,2)))
#print(TradesApi("BTC").get_data(date_from=datetime.datetime(2021,6,2), date_to=datetime.datetime(2021,6,7)))
#print(DaySummaryAPI(coin='BTC').get_data(date=datetime.date(2021,6,21)))



#print(MercadoBitcoinApi(coin='BTC').get_data())
#print(MercadoBitcoinApi(coin='LTC').get_data())