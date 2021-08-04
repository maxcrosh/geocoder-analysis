import scrapy
import time
import pandas as pd
import os
from shapely.geometry import Point, LineString
from geopy.distance import geodesic

from items import GeocoderAnalysisItem
from settings import WAREHOUSE, API_KEY

class GeocoderSevenSpider(scrapy.Spider):
    name = 'geocoder_seven'
    allowed_domains = ['geocode.search.hereapi.com']
    current_time = round(time.time())

    custom_settings = {
        "FEEDS": {
            f"{WAREHOUSE}/{name}-full-{current_time}.csv": {
                "format": "csv", 
                "encoding": "utf8",
            },
        },
        "FEED_EXPORT_FIELDS": [
            'geoid',
            'original_address',
            'title',
            'result_type',
            'housenumber_type',
            'country',
            'state',
            'county',
            'city',
            'district',
            'postalcode',
            'housenumber',
            'original_lat',
            'original_lng',
            'lat',
            'lng',
            'distance',
            'query_score',
        ]
    }

    def start_requests(self):
        initial_data = os.path.join(os.path.dirname(__file__), os.path.join('test_data', 'data.csv'))
        features_df = pd.read_csv(initial_data, delimiter=';')

        for index, row in features_df.iterrows():
            lat = row.get('lat')
            lng = row.get('lng')
            address = row.get('address')

            url = f"https://geocode.search.hereapi.com/v1/geocode?apiKey={API_KEY}&q={address}"
            
            yield scrapy.Request(
                url=url, 
                method='GET', 
                # dont_filter=True,
                callback=self.parse, 
                cb_kwargs=dict(
                    original_lat=lat, 
                    original_lng=lng, 
                    original_address=address
                    )
                )


    def parse(self, response, original_lat, original_lng, original_address):
        item = GeocoderAnalysisItem()

        item['original_lat'] = original_lat
        item['original_lng'] = original_lng
        item['original_address'] = original_address

        try:
            data = response.json()
            
            if len(data['items']) != 0:
                row = data['items'][0]

                distance = geodesic((original_lat, original_lng), (row.get('position').get('lat'),  row.get('position').get('lng'))).meters

                item['geoid'] = row.get('id')
                item['original_address'] = original_address
                item['title'] = row.get('title')
                item['result_type'] = row.get('resultType')
                item['housenumber_type'] = row.get('houseNumberType')
                item['country'] = row.get('address').get('countryName')
                item['state'] = row.get('address').get('state')
                item['county'] = row.get('address').get('county')
                item['city'] = row.get('address').get('city')
                item['district'] = row.get('address').get('district')
                item['postalcode'] = row.get('address').get('postalCode')
                item['housenumber'] = row.get('address').get('houseNumber') if row.get('address').get('houseNumber') else None
                item['lat'] = row.get('position').get('lat')
                item['lng'] = row.get('position').get('lng')
                item['distance'] = distance
                item['query_score'] = row.get('scoring').get('queryScore')
        
        except:
            print('err')

        yield item
        