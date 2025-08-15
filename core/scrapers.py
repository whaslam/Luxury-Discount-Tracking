from scraper_tools import LoggingRequests, DatabaseHandler
import pandas as pd
import time
from curl_cffi import requests
import json
from models import OutNetModel

class WebScraper:
    def __init__(self, project_name, scraper_name, proxies=None, timeout=None):
        self.logger = LoggingRequests(project_name, scraper_name, proxies, timeout)

    def fetch_content(self, url, method="GET", **kwargs):
        response = None
        if method == "GET":
            response = self.logger.get(url, **kwargs)
        elif method == "POST":
            response = self.logger.post(url, **kwargs)

        if response and response.status_code == 200:
            return response
        return None

class OutNetScraper(WebScraper):
    def __init__(self, project_name: str, scraper_name: str):
        super().__init__(project_name, scraper_name)
        self.db_handler = DatabaseHandler()
        self.base_url = 'https://www.theoutnet.com/en-gb/shop/designers/{}?pageNumber={}'
        self.results = []
        self.session = requests.Session(impersonate='chrome')
        self.brands = {
            'Brunello Cucinelli': ['brunello-cucinelli'],
            'Burberry': ['burberry'],
            'Coach': ['coach'],
            'Versace': ['versace'],
            'Michael Kors': ['michael-michael-kors', 'michael-kors-collection'],
            'Jimmy Choo': ['jimmy-choo'],
            'Ferragamo': ['salvatore-ferragamo', 'ferragamo'],
            'Hermes': ['hermes'],
            'Hugo Boss': ['hugo-boss'],
            'Gucci': ['gucci'],
            'Bottega Veneta': ['bottega-veneta'],
            'Balenciaga': ['balenciaga'],
            'Alexander McQueen': ['mcqueen'],
            'Berluti': ['berluti'],
            'Celine': ['celine'],
            'Fendi': ['fendi'],
            'Kenzo': ['kenzo'],
            'Loro Piana': ['loro-piana'],
            'Marc Jacobs': ['marc-jacobs'],
            'Dior': ['dior'],
            'Loewe': ['loewe'],
            'Givenchy': ['givenchy'],
            'Moncler': ['moncler'],
            'Stone Island': ['stone-island'],
            'Prada': ['prada'],
        }

    def scrape(self):
        self.get_products()
        self.df = self.format_df()
        self.write_to_db()

    def get_products(self):
        for brand, urls in self.brands.items():
            for url in urls:
                total_pages = None
                page_num = 1
                while True:
                    r = self.session.get(self.base_url.format(url, page_num))
                    if r.status_code != 200:
                        break
                    page_data = json.loads(r.text.split('<script>window.state=')[1].split('</script>')[0].strip())
                    if not total_pages:
                        total_pages = page_data['plp']['listing']['response']['body']['totalPages']
                        print(f'Total pages for {brand}: {total_pages}')
                    found_products = page_data['plp']['listing']['visibleProducts'][0]['products']
                    for product in found_products:
                        product['brand'] = brand
                    self.results += found_products
                    if page_num == total_pages:
                        break
                    page_num += 1

    def format_df(self):    
        df = pd.json_normalize(self.results)
        df = df[df['inv_local_92003'].notna()]
        df['inventory'] = df['inv_local_92003'].astype(int)

        df['price'] = df['price.sellingPrice.amount'] / df['price.sellingPrice.divisor']
        df['rrp'] = df['price.wasPrice.amount'] / df['price.wasPrice.divisor']
        df['discount'] = (df['rrp'] - df['price']) / df['rrp']
        df['inventory_value'] = df['inventory'] * df['price']

        summary_df = df.pivot_table(index=['brand'], 
                                    values=['productId', 'price', 'rrp', 'discount', 'inventory_value'], 
                                    aggfunc={'productId': 'count', 'price': 'mean', 'rrp': 'mean', 'discount': 'mean', 'inventory_value': 'sum'})

        summary_df = summary_df.rename(columns={
                        'productId': 'product_count',
                        'price': 'avg_price',
                        'rrp': 'avg_rrp',
                        'discount': 'avg_discount'}).reset_index()
        summary_df['date'] = time.strftime('%Y-%m-%d')    
        return summary_df
    
    def write_to_db(self):
        self.db_handler.write_df(df=self.df, table_name=OutNetModel.__tablename__, model=OutNetModel, if_exists='append')

