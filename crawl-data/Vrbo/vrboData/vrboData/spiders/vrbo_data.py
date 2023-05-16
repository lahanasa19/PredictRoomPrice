# -*- coding: utf-8 -*-
import scrapy


class VrboDataSpider(scrapy.Spider):
    name = 'vrbo_data'
    allowed_domains = ['https://www.vrbo.com/2573035ha?adultsCount=2']
    start_urls = ['https://www.vrbo.com/2573035ha?adultsCount=2']

    def parse(self, response):
        print("procesing:"+response.url)
        #Extract data using css selectors

        d = response.css('ul.four-pack list-unstyled').extract()
        data=response.css('.four-pack__block-title::text').extract()
        data2=response.css('.four-pack__detail-item::text').extract()
        my_dict = {data[i]: data2[i] for i in range(len(data))}
        price=response.css('span.rental-price__amount::text').extract_first()
        unit=response.css('.rental-price__label::text').get()
        max_guest=response.css('#input-trip-details-guests-picker::attr(value)')
        rule=response.css('.house-rules__bullet p::text').extract()
        amenity=response.css('.amenities-categorized-modal__category h3::text').extract()
        listItems=response.css('.amenities-categorized-modal__amenity-list-item div::text').extract()
        rateScore=response.css('.review__headline::text').extract()
        print("llllllllllllllllllll", response.text)
        for k,item in my_dict.items():
            #create a dictionary to store the scraped info
            scraped_info = {
                #key:value
                k : item,
                'htnl': response.text
            }

            #yield or give the scraped info to scrapy
            yield scraped_info
        
