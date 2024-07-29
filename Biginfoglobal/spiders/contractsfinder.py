import scrapy
from scrapy.http import TextResponse
from urllib.parse import urlencode
from lxml import html
from datetime import datetime, timedelta
import time
import csv
import requests
import re

class ContractsfinderSpider(scrapy.Spider):
    name = "contractsfinder"
    page_no = 1
    last_page = 0
    is_end = False
    output_file = 'contractsfinder_data.csv'
    scraped_data = []
    start_url = "https://www.contractsfinder.service.gov.uk/Search/Results"
   
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,  # Optional: Add a delay to avoid being blocked
    }

    def start_requests(self):
        yield scrapy.Request(url=f"{self.start_url}?&page={self.page_no}#dashboard_notices", callback=self.parse)
    
    def getData(self,html_object,xPath):
        if xPath=="-":
            return "-"
        data= html_object.xpath('{}/text()'.format(xPath))
        if len(data) >0:
            return data[0]
        return "-"

    def getDataUsingCss(self,html_object,cssPath):
        if cssPath== "-":
            return "-"
        str_list=html_object.cssselect(cssPath)
        data = [element.text_content().strip()for element in str_list]
        print(data)
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
        html_object = html.fromstring(html_content)
        text_response = TextResponse(url=response.url, body=html_content, encoding='utf-8')
        links = text_response.css('#dashboard_notices> div.gadget-body> div.search-result div.search-result-header> h2> a::attr(href)').getall()
        post_dates = text_response.css('#dashboard_notices> div.gadget-body> div.search-result> div:nth-child(11)::text').getall()
        closing_dates = text_response.css('#dashboard_notices> div.gadget-body> div.search-result> div:nth-child(8)::text').getall()
        last_pag = text_response.css('#dashboard_notices > div.gadget-footer > ul > li.standard-paginate-next-box.standard-paginate-next-icon > a > span::text').getall()
        # links = html_object.cssselect('#block-theme-content > div > div > div > div.view-content.clearfix')
        print(last_pag[0].split(" of ")[1],"sddddddddddddddddddddddddddddddddddddddddddddddddddd")
        self.last_page = int(last_pag[0].split(" of ")[1])
        print(links,"sddddddddddddddddddddddddddddddddddddddddddddddddddd")
        print(post_dates,"sddddddddddddddddddddddddddddddddddddddddddddddddddd")
       



        results = [
            {'link': link,
            'post_date':post_date.split(",")[0].strip(),
            'closing_date':closing_date.split(",")[0].strip()}
            for link, post_date, closing_date  in zip(links, post_dates,closing_dates)
        ]
        print(results)

        for result in results:
            post_date = datetime.strptime(result["post_date"].lower(), '%d %B %Y').date()
            # if post_date == datetime.now().date() - timedelta(days=1):
            if post_date >= datetime.now().date() - timedelta(days=4):


                response = requests.get(result["link"])
                if response.status_code == 200:
                    html_object = html.fromstring(response.content)

                    address_str_list=html_object.cssselect("#content-holder-left > div:nth-child(7) > p:nth-child(7)")
                    data = [' '.join(element.itertext())for element in address_str_list]
                    address = ' '.join(data)
                

                    self.scraped_data.append({
                        'authority_name':self.getDataUsingCss(html_object,'#home-breadcrumb-description > h2'),
                        'address': address,
                        'telephone':self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(7) > p:nth-child(9)'),
                        'fax_number': self.getDataUsingCss(html_object,'-'),
                        'Email': self.getDataUsingCss(html_object,"#content-holder-left > div:nth-child(7) > p:nth-child(11)> a"),
                        'contact_person': self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(7) > p:nth-child(5)'),
                        'tender_title': self.getDataUsingCss(html_object,'#all-content-wrapper > h1'),
                        'description': self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(4) > p:nth-child(4)'),
                        'tender_type': self.getDataUsingCss(html_object,'-'),
                        'tender_no': self.getDataUsingCss(html_object,'-'),
                        'tender_competition': self.getDataUsingCss(html_object,'-'),
                        'published_date': result["post_date"],
                        'closing_date': result["closing_date"],
                        'country': "United Kingdom",
                        'emd': self.getDataUsingCss(html_object,'-'),
                        'estimated_cost': self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(3) > p:nth-child(8)'),
                        'documents': result["link"],
                        'sectors': self.getDataUsingCss(html_object,'-'),
                        'cpv_codes':self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(3) > ul > li > p').split(" - ")[1],
                        'regions': self.getDataUsingCss(html_object,'-'),
                        'funding_agency': self.getDataUsingCss(html_object,'-'),
                        'big_ref_no': self.getDataUsingCss(html_object,'-'),
                        
                    })
                    print({
                        'authority_name':self.getDataUsingCss(html_object,'#home-breadcrumb-description > h2'),
                        'address': address,
                        'telephone':self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(7) > p:nth-child(9)'),
                        'fax_number': self.getDataUsingCss(html_object,'-'),
                        'Email': self.getDataUsingCss(html_object,"#content-holder-left > div:nth-child(7) > p:nth-child(11)> a"),
                        'contact_person': self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(7) > p:nth-child(5)'),
                        'tender_title': self.getDataUsingCss(html_object,'#all-content-wrapper > h1'),
                        'description': self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(4) > p:nth-child(4)'),
                        'tender_type': self.getDataUsingCss(html_object,'-'),
                        'tender_no': self.getDataUsingCss(html_object,'-'),
                        'tender_competition': self.getDataUsingCss(html_object,'-'),
                        'published_date': result["post_date"],
                        'closing_date': result["closing_date"],
                        'country': "United Kingdom",
                        'emd': self.getDataUsingCss(html_object,'-'),
                        'estimated_cost': self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(3) > p:nth-child(8)'),
                        'documents': result["link"],
                        'sectors': self.getDataUsingCss(html_object,'-'),
                        'cpv_codes':self.getDataUsingCss(html_object,'#content-holder-left > div:nth-child(3) > ul > li > p').split(" - ")[1],
                        'regions': self.getDataUsingCss(html_object,'-'),
                        'funding_agency': self.getDataUsingCss(html_object,'-'),
                        'big_ref_no': self.getDataUsingCss(html_object,'-'),
                        
                    })
                
                
                    print(html_object.xpath('//*[@id="block-cdb-content"]/article/div[2]/div[3]/div/div/div/div/div[1]/div/div[1]/div/text()'), "****************************************************cls")
            elif post_date < datetime.now().date() - timedelta(days=2):
                print("****************************Data older than one day found. Stopping spider.***********************************")
                self.is_end = True
                break  

        if not self.is_end and self.page_no < self.last_page:
            self.page_no += 1
            time.sleep(5)  # Avoid being blocked by adding delay
            yield scrapy.Request(url=f"{self.start_url}?page={self.page_no}", callback=self.parse)
        else:
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
    process.crawl(ContractsfinderSpider)
    process.start()
