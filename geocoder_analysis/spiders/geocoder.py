import scrapy
import time
import pandas as pd
import os
from shapely.geometry import Point, LineString
from geopy.distance import geodesic

from items import GeocoderAnalysisItem
from settings import WAREHOUSE, API_KEY

class GeocoderSpider(scrapy.Spider):
    name = 'geocoder'
    allowed_domains = ['geocoder.ls.hereapi.com']
    current_time = round(time.time())

    custom_settings = {
        "FEEDS": {
            f"{WAREHOUSE}/{name}-full-{current_time}.csv": {
                "format": "csv", 
                "encoding": "utf8",
            },
        },
        "FEED_EXPORT_FIELDS": [
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

            url = f"https://geocoder.ls.hereapi.com/6.2/geocode.json?apiKey={API_KEY}&searchtext={address}&gen=9"
            
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
        
        item['original_address'] = original_address
        item['original_lat'] = original_lat
        item['original_lng'] = original_lng

        try:
            data = response.json()
            
            if len(data['Response']['View'][0]['Result']) != 0:
                row = data['Response']['View'][0]['Result'][0]

                distance = geodesic((original_lat, original_lng), (row.get('Location').get('DisplayPosition').get('Latitude'), row.get('Location').get('DisplayPosition').get('Longitude'))).meters

                # item['geoid'] = row.get('id')
                
                item['title'] = row.get('Location').get('Address').get('Label')
                item['result_type'] = row.get('MatchLevel')
                item['housenumber_type'] = row.get('MatchType')
                item['country'] = row.get('Location').get('Address').get('Country')
                item['state'] = row.get('Location').get('Address').get('State')
                item['county'] = row.get('Location').get('Address').get('County')
                item['city'] = row.get('Location').get('Address').get('City')
                item['district'] = row.get('Location').get('Address').get('District')
                item['postalcode'] = row.get('Location').get('Address').get('PostalCode')
                item['housenumber'] = row.get('Location').get('Address').get('HouseNumber') if row.get('Location').get('Address').get('HouseNumber') else None
                item['lat'] = row.get('Location').get('DisplayPosition').get('Latitude')
                item['lng'] = row.get('Location').get('DisplayPosition').get('Longitude')
                item['distance'] = distance
                item['query_score'] = row.get('Relevance')
            
        except:
            print('err')

        yield item
        