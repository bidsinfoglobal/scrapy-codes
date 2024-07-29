import scrapy
from scrapy.http import TextResponse
from lxml import html
from datetime import datetime, timedelta
import time
import csv
import requests
import re
from lxml import etree
import json
from Biginfoglobal.translator import translate


class oportunidadesSpider(scrapy.Spider):
    name = "oportunidades"
    page_no = 0
    last_page = 0
    is_end = False
    output_file = 'oportunidades_tender_data.csv'
    scraped_data = []
    start_url = "https://www.oportunidades.onu.org.bo/convocatorias/bids_dt"
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,  # Optional: Add a delay to avoid being blocked
    }

    def start_requests(self):
        yield scrapy.Request(url=f"{self.start_url}?_=1719201566431", callback=self.parse)
    
    def getData(self,html_object,xPath):
        data= html_object.xpath('{}/text()'.format(xPath))
        if len(data) >0:
            return data[0]
        return "-"

    def getDataUsingCss(self,html_object,cssPath):
        str_list=html_object.cssselect(cssPath)

        data = [element.text_content().strip() for element in str_list]
        result = ' '.join(data)
        if result:
            print(translate(result),"REerewrwer")
            return translate(result)
        return "-" 

    def convert_date(self,input_string):
        if input_string =="-":
            return "-"
        try:
            date_pattern = r"(\w+ \d{1,2}, \d{4})"
            dates = re.findall(date_pattern, input_string)
            def parse_and_format_date(date_str):
                # Parse the date string into a datetime object
                date_obj = datetime.strptime(date_str, '%B %d, %Y')
                # Format the datetime object into the desired format
                return date_obj.strftime('%d %b %Y')

            post_date,closing_date = [parse_and_format_date(date) for date in dates]
            
            return post_date,closing_date
        except:
            return "-","-"

    def parse(self, response):
        data=json.loads(response.body)["data"]
        results=[]
        for i in data:

            if len(i) >0:
                html_object=html.fromstring(i[0])
                link= html_object.xpath("//a/@href")[0]
                post_date_elements= html_object.cssselect(".recuadro_convocatoria> div> div.col-md-10> div.vigencia")
                for element in post_date_elements:
                    post_date_str=element.text_content()
                    post_date,closing_date=self.convert_date(translate(post_date_str))
                    results.append( {'link': link, 'post_date': post_date,"closing_date":closing_date})

        for result in results:
            post_date = datetime.strptime(result["post_date"], '%d %b %Y').date()
            # if post_date == datetime.now().date() - timedelta(days=1):
            if post_date >= datetime.now().date() - timedelta(days=3):
            # if True:
                response = requests.get(result["link"])
                if response.status_code == 200:
                    html_object = html.fromstring(response.content)
                    self.scraped_data.append({
                        'authority_name': self.getDataUsingCss(html_object,'body > div.container-body > div > div > div.recuadro_convocatoria > div > div.col-md-10 > h5:nth-child(5)').split(" by ")[1],
                        'address': self.getDataUsingCss(html_object,'body > div.container-body > div > div > div.recuadro_convocatoria_detalles > div.row > div.col-md-8 > div:nth-child(10) > p:nth-child(3)'),
                        'telephone': self.getDataUsingCss(html_object,'body > div.container-body > div > div > div.recuadro_convocatoria_detalles > div.row > div.col-md-8 > div:nth-child(10) > p:nth-child(3)'),
                        'fax_number': "-",
                        'Email': self.getDataUsingCss(html_object,'body > div.container-body > div > div > div.recuadro_convocatoria_detalles > div.row > div.col-md-8 > div:nth-child(10) > p:nth-child(4) > a'),
                        'contact_person': "-",
                        'tender_title': self.getDataUsingCss(html_object,'body > div.container-body > div > div > div.recuadro_convocatoria > div > div.col-md-10 > h3'),
                        'description': self.getDataUsingCss(html_object,'body > div.container-body > div > div > div.recuadro_convocatoria_detalles > div.row > div.col-md-8 > p'),
                        'tender_type': "-",
                        'tender_no': self.getDataUsingCss(html_object,'body > div.container-body > div > div > div.recuadro_convocatoria > div > div.col-md-10 > h5:nth-child(3)'),
                        'tender_competition': "-",
                        'published_date': result["post_date"],
                        'closing_date': result["closing_date"],
                        'country': "Bolivia",
                        'emd': "-",
                        'estimated_cost': "-",
                        'documents': result["link"],
                        'sectors': "-",
                        'cpv_codes': "-",
                        'regions': "-",
                        'funding_agency': "-",
                        'big_ref_no': "-",
                       
                    })
                    print(self.scraped_data[0])
                    print(translate( self.getDataUsingCss(html_object,'body > div.container-body > div > div > div.recuadro_convocatoria > div > div.col-md-10 > h5:nth-child(5)')).split(" by ")[1], "****************************************************cls")
            elif post_date < datetime.now().date() - timedelta(days=1):
                print("Data older than one day found. Stopping spider.")
                self.is_end = True
                break

        if not self.is_end and self.page_no < self.last_page:
            self.page_no += 1
            time.sleep(5)  # Avoid being blocked by adding delay
            yield scrapy.Request(url=f"{self.start_url}?page={self.page_no}", callback=self.parse)
        else:
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
    process.crawl(oportunidadesSpider)
    process.start()
