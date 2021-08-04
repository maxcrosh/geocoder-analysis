import scrapy
from scrapy.crawler import CrawlerProcess
from spiders.geocoder_seven import GeocoderSevenSpider
from spiders.geocoder import GeocoderSpider

process = CrawlerProcess()

process.crawl(GeocoderSevenSpider)
process.crawl(GeocoderSpider)
process.start()