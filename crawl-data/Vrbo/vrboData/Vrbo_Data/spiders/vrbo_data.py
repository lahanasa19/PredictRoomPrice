# -*- coding: utf-8 -*-
import scrapy
import json


class VrboDataSpider(scrapy.Spider):
    name = 'vrbo_data'
    allowed_domains = ['https://www.vrbo.com/2573035ha?adultsCount=2']
    
    def __init__(self, url, amenitieFileName):
        self.start_urls = [url]
        self.amenitieFileName = amenitieFileName

    def parse(self, response):
        print("procesing:"+response.url)

        #data=response.css('.four-pack__block-title::text').extract()
        #data2=response.css('.four-pack__detail-item::text').extract()
        #my_dict = {data[i]: data2[i] for i in range(len(data))}
        
        listItems=response.css('.amenities-categorized-modal__amenity-list-item div::text').extract() 
        #rateScore=response.css('.review__headline::text').extract()
        #for k,item in my_dict.items():
            #create a dictionary to store the scraped info
        
        scraped_info = {
            'Amenities': listItems
        }
        with open('/home/dell/code/crawl-data/Vrbo/vrboData/Amenities/'+ self.amenitieFileName+ '.json', 'a') as json_file:
            json.dump(scraped_info, json_file)
            json_file.write(',\n')  # Add a newline after each object
            print("Objects written to JSON file successfully.")

        yield scraped_info
        
