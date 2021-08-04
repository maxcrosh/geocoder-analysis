# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class GeocoderAnalysisItem(scrapy.Item):
    geoid = scrapy.Field()
    original_address = scrapy.Field()
    title = scrapy.Field()
    result_type = scrapy.Field()
    housenumber_type = scrapy.Field()
    country = scrapy.Field()
    state = scrapy.Field()
    county = scrapy.Field()
    city = scrapy.Field()
    district = scrapy.Field()
    postalcode = scrapy.Field()
    housenumber = scrapy.Field()
    original_lat = scrapy.Field()
    original_lng = scrapy.Field()
    lat = scrapy.Field()
    lng = scrapy.Field()
    distance = scrapy.Field()
    query_score = scrapy.Field()