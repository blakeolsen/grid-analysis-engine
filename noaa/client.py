from datetime import date, timedelta
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import os
import pandas as pd
import requests


class WeatherClient:
    __auth = {
        'token': os.environ['NOAA_TOKEN'],
    }
    __url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2'
    __max_distance = 1000 # miles
    geolocator = Nominatim(user_agent="grid-analysis-engine")

    def __init__(self):
        self.stations = None
        pass

    def streamed_results(self, url, headers=None, params=None) -> pd.DataFrame:
        step = 1000
        offset = 0
        __headers = self.__auth if (not headers) else headers.update(self.__auth)
        __params = {} if (not params) else params.copy()

        accumulated = []
        while True:
            __params['offset'] = offset
            __params['limit'] = step
            response = requests.get(url, headers=__headers, params=__params).json()
            if 'results' not in response:
                break
            meta = response['metadata']['resultset']
            accumulated.extend(response['results'])
            if meta['offset'] + meta['limit'] >= meta['count']:
                break
            offset += step
        return pd.DataFrame(accumulated)

    def list_stations(self) -> pd.DataFrame:
        cache = f"{os.path.dirname(os.path.abspath(__file__))}/stations.csv"
        if os.path.exists(cache):
            print('loading stations from file')
            return pd.read_csv(cache)
        else:
            print('streaming stations via api')
            return self.streamed_results(url=f"{self.__url}/stations")

    def nearest_stations(self, name: str):
        """
        :param name: name of location to compare to
        :return: ordered list of stations from nearest to farthest
        """
        if not self.stations:
            self.stations = self.list_stations()

        location = self.geolocator.geocode(name)

        def distance(cmp):
            return geodesic((cmp.latitude, cmp.longitude), (location.latitude, location.longitude))

        with_distance = self.stations
        with_distance['distance'] = with_distance.apply(distance, axis=1)
        return with_distance.sort_values(by=['distance'])

    def data_for_day(self, dataset_id: str, station_id: str, day: date) -> pd.DataFrame:
        response = self.streamed_results(
            url=f"{self.__url}/data",
            params={
                "datasetid": dataset_id,
                "stationid": station_id,
                "startdate": day.isoformat(),
                "enddate": (day + timedelta(day=1)).isoformat(),
                "units": "standard",
            },
        )
        return response

    def get(self, location: str, start_date: date, end_date) -> pd.DataFrame:
        dataset = 'NORMAL_HLY'
        ordered_stations = self.nearest_stations(location)['id']
        return
