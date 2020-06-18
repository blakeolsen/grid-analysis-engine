from datetime import date
import os
import requests

class WeatherClient:
    headers = {
        'token': os.environ['NOAA_TOKEN'],
    }

    base_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2'

    cities = {
        'Beaumont, TX US': 'CITY:US480007',
        'Chicago, IL US': 'CITY:US170006',
        'Columbus, OH US': 'CITY:US390011',
        'Des Moines, IA US': 'CITY:US190008',
        'Detroit, MI US': 'CITY:US260006',
        'Fargo, ND US': 'CITY:US380003',
        'Green Bay, WI US': 'CITY:US550002',
        'Indianapolis, IN US': 'CITY:US180004',
        'Jackson, MS US': 'CITY:US280011',
        'Kansas City, MO US': 'CITY:US290008',
        'Little Rock, AR US': 'CITY:US050013',
        'Minneapolis, MN US': 'CITY:US270013',
        'Nashville, TN US': 'CITY:US470016',
        'New Orleans, LA US': 'CITY:US220016',
        'Oklahoma City, OK US': 'CITY:US400013',
        'Sioux City, IA US': 'CITY:US190017',
        'St. Louis, MO US': 'CITY:US290021',
    }

    def __init__(self):
        pass

    def streamed_results(self, url, headers=dict(), params=dict()):
        step = 1000
        offset = 0
        accumulated = []
        while True:
            temp_params = params.copy()
            temp_params['offset'] = offset
            temp_params['limit'] = step
            response = requests.get(url, headers=headers, params=params).json()
            if 'results' not in response:
                break
            accumulated.append(response['results'])
            offset += step
        return accumulated

    def find_code(self, keyword: str, type: str = 'CITY'):
        results = self.streamed_results(
            url=f"{self.base_url}/locations",
            headers=self.headers,
            params={'locationcategoryid': type},
        )
        codes = list(filter(lambda location: keyword in location['name'], results))
        return [code['id'] for code in codes]

    def hourly_data(self, code: str, start_date: date, end_date: date):
        results = self.streamed_results(
            url=f"{self.base_url}/data",
            headers=self.headers,
            params={
                'datasetid': 'NORMAL_HLY',
                'locationid': code,
                'startdate': start_date.isoformat(),
                'enddate': end_date.isoformat(),
                'units': 'standard',
            },
        )
        return results
