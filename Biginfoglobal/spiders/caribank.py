import scrapy
from scrapy.http import TextResponse
from lxml import html
from datetime import datetime, timedelta
import time
import csv
import requests
import re
class CaribankSpider(scrapy.Spider):
    name = "caribank"
    page_no = 0
    last_page = 0
    is_end = False
    output_file = 'caribank_tender_data.csv'
    scraped_data = []
    start_url = "https://www.caribank.org/work-with-us/procurement/procurement-notices"
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,  # Optional: Add a delay to avoid being blocked
    }

    def start_requests(self):
        yield scrapy.Request(url=f"{self.start_url}?page={self.page_no}", callback=self.parse)
    
    def getData(self,html_object,xPath):
        if xPath=="-":
            return "-"
        data= html_object.xpath('{}/text()'.format(xPath))
        if len(data) >0:
            return data[0]
        return "-"

    def getDataUsingCss(self,html_object,cssPath):
        str_list=html_object.cssselect(cssPath)
        data = [element.text_content().strip() for element in str_list]
        result = ' '.join(data)
        if result:
            return result
        return "-"

    def convert_date(self,input_date):
        if input_date =="-":
            return "-"

        parsed_date = datetime.strptime(input_date, "%b %d, %Y")
        formatted_date = parsed_date.strftime("%d %b %Y")
        return formatted_date

    def parse(self, response):
        base_response=response
        html_content = response.text
        text_response = TextResponse(url=response.url, body=html_content, encoding='utf-8')
        links = response.xpath("//table//tbody//tr//td//a/@href").getall()
        print(len(links),"sddddddddddddddddddddddddddddddddddddddddddddddddddd")
        summaries = text_response.css(".list .item .item-summary::text").getall()
        last_page_url = text_response.css(".pager__item--last a::attr(href)").get()

        if last_page_url:
            self.last_page = int(last_page_url.split("=")[1])

        results = [
            {'link': f"https://www.caribank.org{link}"}
            for link in links
        ]

        for result in results:
            response = requests.get(result["link"])
            if response.status_code == 200:
                html_object = html.fromstring(response.content)


                address_lins=html_object.xpath('//*[@id="block-cdb-content"]/article/div[2]/div[3]/div/div/div/div/div[1]/div[1]/div[1]//text()')
                address = ' '.join([text.strip() for text in address_lins if text.strip()])

                address_str_list=html_object.cssselect("#block-cdb-content > article > div.paragraph-list > div:nth-child(3) > div > div > div > div > div:nth-child(1) > div > div.field--wrap.field--paragraph--field-cdb-department.field-display--hidden")
                data = [element.text_content().strip() for element in address_str_list]
                address = ' '.join(data)
                if not address:
                    address_str_list=html_object.cssselect("#block-cdb-content > article > div.paragraph-list > div:nth-child(3) > div > div > div > div > div:nth-child(1) > div > div.field--wrap.field--paragraph.field--paragraph--first-level.field--paragraph--field-cdb-location p")
                    data = [element.text_content().strip() for element in address_str_list]
                    address = ' '.join(data)

                # print(telephone,"sdfsfds")

                # email_str_list=html_object.cssselect("#block-cdb-content > article > div.paragraph-list > div:nth-child(3) > div > div > div > div > div:nth-child(1) > div > div.node--field-icon > a")
                # data = [element.text_content().strip() for element in email_str_list]
                # email = ' '.join(data)

                # country_str_list=html_object.cssselect("#block-cdb-content > article > div.bg-blue-prussian > div > div > div.col-md-3 > div > div.field--wrap.field--node--field-cdb-country-tag.field-display--above > div:nth-child(2) > div > a")
                # data = [element.text_content().strip() for element in country_str_list]
                # country = ' '.join(data)
               

                self.scraped_data.append({
                    'authority_name': "-",
                    'address': address,
                    'telephone': self.getDataUsingCss(html_object,"#block-cdb-content > article > div.paragraph-list > div:nth-child(3) > div > div > div > div > div:nth-child(1) > div > div.field--wrap.field--paragraph.field--paragraph--field-cdb-phone-text"),
                    # 'telephone': self.getData(html_object,'//*[@id="block-cdb-content"]/article/div[2]/div[3]/div/div/div/div/div[1]/div/div[3]'),
                    'fax_number': self.getData(html_object,'-'),
                    'Email': self.getDataUsingCss(html_object,"#block-cdb-content > article > div.paragraph-list > div:nth-child(3) > div > div > div > div > div:nth-child(1) > div > div.node--field-icon > a"),
                    'contact_person': self.getData(html_object,'//*[@id="block-cdb-content"]/article/div[2]/div[3]/div/div/div/div/div[1]/div/h5[1]'),
                    'tender_title': self.getData(html_object,'//*[@id="block-cdb-content"]/article/div[1]/div/div/div[2]/div/div[2]/div[2]'),
                    'description': self.getData(html_object,'//*[@id="block-cdb-content"]/article/div[1]/div/div/div[3]/div/div/p[6]'),
                    'tender_type': "-",
                    'tender_no': self.getData(html_object,'-'),
                    'tender_competition': "-",
                    'published_date': "-",
                    'closing_date': self.convert_date(self.getData(html_object,'//*[@id="block-cdb-content"]/article/div[1]/div/div/div[2]/div/div[4]/div[1]/div[2]/time')),
                    'country': self.getDataUsingCss(html_object,"#block-cdb-content > article > div.bg-blue-prussian > div > div > div.col-md-3 > div > div.field--wrap.field--node--field-cdb-country-tag.field-display--above > div:nth-child(2) > div > a"),
                    'emd': "-",
                    'estimated_cost': self.getData(html_object,'-'),
                    'documents': result["link"],
                    'sectors': "-",
                    'cpv_codes': "-",
                    'regions': "-",
                    'funding_agency': "-",
                    'big_ref_no': "-",
                    
                })
              
               
                print(html_object.xpath('//*[@id="block-cdb-content"]/article/div[2]/div[3]/div/div/div/div/div[1]/div/div[1]/div/text()'), "****************************************************cls")
            

        self.is_end = True
        self.crawler.engine.close_spider(self, reason='Data older than one day found.')

    def close(self, reason):
        with open(self.output_file, 'w', newline='', encoding='utf-8') as file:
            try:
                writer = csv.DictWriter(file, fieldnames=list(self.scraped_data[0].keys()))
                writer.writeheader()
                writer.writerows(self.scraped_data)
            except:
                print("something wentwrong during csv file creation. ")
        self.logger.info(f'Spider closed: {reason}')

if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess

    process = CrawlerProcess({
        'LOG_LEVEL': 'INFO',
    })
    process.crawl(AdbOrgSpider)
    process.start()
