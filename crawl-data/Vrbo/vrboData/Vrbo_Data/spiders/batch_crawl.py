from vrbo_data import VrboDataSpider
from scrapy.crawler import CrawlerProcess
import pandas as pd
import json
from hdfs import InsecureClient
import sys

#param1: (0,1): 0-crawl amenities 1-save csv file
#param2: amenitie/raw file name
args = sys.argv[1:]  
rawPath = 'crawl-data/Vrbo/vrboData/Raw_Data/'
amenitiesPath = 'crawl-data/Vrbo/vrboData/Amenities/'

def save_file_to_hdfs(tmp_hdfs_path, tmp_local_path):
  client = InsecureClient('http://localhost:9870', user='root')
  client.upload('/data/' + tmp_hdfs_path, tmp_local_path)
0
raw_file_path =  rawPath + args[1] + ".json"
amenities_file_path = amenitiesPath + args[1] + ".json"
csv_file_path = "/home/dell/code/crawl-data/Vrbo/vrboData/Clean_Data/"
hdfs_file_path = "crawl-data/Vrbo/vrboData/Provinces/"
with open(raw_file_path) as f:
    data = json.load(f)

dataSchema = {
    'High_chair': [], 
    'Workspace' : [], 
    'Lock_on_door' : [], 
    'Pool' : [], 
    'Beach' : [], 
    'pdpUrlType' : [], 
    'Snowflake' : [], 
    'View_ocean' : [], 
    'No_detector_co2' : [], 
    'Jacuzzi' : [], 
    'roomTypeCategory' : [], 
    'weeklyPriceFactor' : [], 
    'Tv' : [], 
    'Surveillance' : [], 
    'Bath' : [], 
    'maxGuestCapacity' : [], 
    'Wash' : [], 
    'name' : [], 
    'Sauna' : [],
    'Longitude': [],
    'Smoking_allowed' : [],
    'canInstantBook' : [], 
    'listingObjType' : [], 
    'contextualPicturesCount' : [], 
    'No_detector_smoke' : [], 
    'Luggage_drop' : [], 
    'avgRatingLocalized' : [], 
    'Pets' : [], 
    'Cooking_basics' : [],
    'Ev_charger' : [], 
    'Latitude' : [],
    'No_lock_on_door' : [],
    'Hair_dryer' : [], 
    'price' : [], 
    'reviewCount' : [], 
    'Elevator' : [], 
    'Refrigetor' : [], 
    'Wifi' : [], 
    'isSuperHost' : [], 
    'Toys' : [], 
    'Microwave' : [], 
    'city' : [], 
    'structuredStayDisplayPrice' : [], 
    'Patio_Balcony' : [], 
    'Dryer' : [], 
    'View_city' : [], 
    'isVerified' : [], 
    'Fire_place' : [], 
    'Flower' : [], 
    'Car_rental': []
}
def save_to_csv(amenities):
    for k, i in enumerate(data['listings']):
        dataSchema['High_chair'].append(0)
        dataSchema['Bath'].append(0)
        dataSchema['View_ocean'].append(0)
        dataSchema['Workspace'].append(0)
        dataSchema['Lock_on_door'].append(0)
        dataSchema['pdpUrlType'].append(0)
        dataSchema['Snowflake'].append(0)
        dataSchema['Surveillance'].append(0)
        dataSchema['maxGuestCapacity'].append(i['sleeps'])
        dataSchema['roomTypeCategory'].append(i['propertyType'])
        dataSchema['Jacuzzi'].append(0)
        dataSchema['weeklyPriceFactor'].append(0)
        dataSchema['name'].append(i['propertyMetadata']['headline'])
        dataSchema['Sauna'].append(0)
        dataSchema['canInstantBook'].append(i['instantBookable'])
        dataSchema['listingObjType'].append(0), 
        dataSchema['contextualPicturesCount'].append(len(i['images']))
        dataSchema['Luggage_drop'].append(0)
        dataSchema['avgRatingLocalized'].append(i['averageRating'])
        dataSchema['Ev_charger'].append(0)
        dataSchema['Latitude'].append(i['geoCode']['latitude'])
        dataSchema['Longitude'].append(i['geoCode']['longitude'])
        dataSchema['price'].append(i['priceSummary']['formattedAmount'])
        dataSchema['reviewCount'].append(i['reviewCount'])
        dataSchema['Toys'].append(0)
        dataSchema['structuredStayDisplayPrice'].append(i['priceSummary']['formattedAmount']), 
        dataSchema['isSuperHost'].append(1)
        dataSchema['No_lock_on_door'].append(0)
        dataSchema['Beach'].append(1)
        dataSchema['city'].append('Haiphong') 
        if('Washing machine' in amenities['lists'][k]['Amenities']):
            dataSchema['Wash'].append(1)
        else:
            dataSchema['Wash'].append(0)
        if('Smart TV' in amenities['lists'][k]['Amenities']):
            dataSchema['Tv'].append(1)
        else:
            dataSchema['Tv'].append(0)
        if('Carbon monoxide detector' in amenities['lists'][k]['Amenities']):
            dataSchema['No_detector_co2'].append(1)
        else:
            dataSchema['No_detector_co2'].append(0)
        if ('Indoor Pool' in amenities['lists'][k]['Amenities']):
            dataSchema['Pool'].append(1)
        else:
            dataSchema['Pool'].append(0)
        if('Hair Dryer' in amenities['lists'][k]['Amenities']):
            dataSchema['Hair_dryer'].append(1)
        else:
            dataSchema['Hair_dryer'].append(0)
        dataSchema['Pets'].append(0)
        if('Internet' in amenities['lists'][k]['Amenities'] or 'Free wifi' in amenities['lists'][k]['Amenities']): 
            dataSchema['Wifi'].append(1)
        else:
            dataSchema['Wifi'].append(0)
        if ('Microwave' in amenities['lists'][k]['Amenities']):
            dataSchema['Microwave'].append(1)
        else:
            dataSchema['Microwave'].append(0)
        if('Elevator' in amenities['lists'][k]['Amenities']):
            dataSchema['Elevator'].append(1)
        else:
            dataSchema['Elevator'].append(0)
        if('Microwave'in amenities['lists'][k]['Amenities'] or 'Oven' in amenities['lists'][k]['Amenities']): 
            dataSchema['Cooking_basics'].append(1)
        else:
            dataSchema['Cooking_basics'].append(0)
        if('Refrigerator' in amenities['lists'][k]['Amenities']):
            dataSchema['Refrigetor'].append(1)
        else:
            dataSchema['Refrigetor'].append(0)
        if('Smoke detector' in amenities['lists'][k]['Amenities']):
            dataSchema['No_detector_smoke'].append(1)
            dataSchema['Smoking_allowed'].append(1)
        else: 
            dataSchema['No_detector_smoke'].append(0)
            dataSchema['Smoking_allowed'].append(0)
        if('Clothes dryer' in amenities['lists'][k]['Amenities']):
            dataSchema['Dryer'].append(1)
        else: 
            dataSchema['Dryer'].append(0)
        dataSchema['Patio_Balcony'].append(0) 
        dataSchema['View_city'].append(1) 
        dataSchema['isVerified'].append(0) 
        if('Fireplace' in amenities['lists'][k]['Amenities']):
            dataSchema['Fire_place'].append(1)
        else:
            dataSchema['Fire_place'].append(0)
        dataSchema['Flower'].append(0) 
        if ('Car Available' in amenities['lists'][k]['Amenities']):
            dataSchema['Car_rental'].append(1)
        else:
            dataSchema['Car_rental'].append(0)
        df = pd.DataFrame(dataSchema)
        
        df.to_csv(csv_file_path, index=False)
        print("DataFrame converted and saved as CSV successfully.")
        save_file_to_hdfs(hdfs_file_path+ args[1]+ '/prices.csv', csv_file_path + args[1] +"/prices.csv")

       
def crawl_amenities(): 
    process = CrawlerProcess()
    for k, i in enumerate(data['listings']):
        path = 'https://www.vrbo.com'+ i['detailPageUrl']+ '&modal=amenities-details' 
        process.crawl(VrboDataSpider, url=path, amenitieFileName=args[1])
    process.start() 

if(args[1] == "1") :
    with open(amenities_file_path) as f:
        amenities = json.load(f)
    save_to_csv(amenities)
else:
    print("crawl amenities", args[1])
    crawl_amenities()
