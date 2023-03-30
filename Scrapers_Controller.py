import os
import sys
import json
import glob
import datetime
from datetime import datetime
import chromedriver_autoinstaller
import pandas as pd

from models.store import Store
from models.brand import Brand
from models.product import Product
from models.variant import Variant
from models.metafields import Metafields

from modules.query_processor import Query_Processor


from scrapers.digitalhub import Digitalhub_Scraper
from scrapers.safilo import Safilo_Scraper
from scrapers.keringeyewear import Keringeyewear_Scraper
from scrapers.rudyproject import Rudyproject_Scraper
from scrapers.luxottica import Luxottica_Scraper

from database.digitalhub import Digitalhub_Mongodb
from database.safilo import Safilo_Mongodb
from database.keringeyewear import Keringeyewear_Mongodb
from database.rudyproject import Rudyproject_Mongodb
from database.luxottica import Luxottica_Mongodb

class Controller:
    def __init__(self, DEBUG: bool, path: str) -> None:
        self.DEBUG = DEBUG
        self.store: Store = None
        self.path: str = path
        self.config_file: str = f'{self.path}/files/config.json'
        self.results_foldername: str = ''
        self.logs_folder_path: str = ''
        self.result_filename: str = ''
        self.logs_filename: str = ''
        pass

    def scrape_by_store_controller(self) -> None:
        try:
            
            # getting all stores from database
            query_processor = Query_Processor(self.DEBUG, self.config_file, '')
            stores = query_processor.get_stores()

            for store in self.get_store_to_update(stores):
                self.store = store

                query_processor.database_name = str(self.store.name).lower()
                self.logs_folder_path = f'{self.path}/Logs/{self.store.name}/'

                if not os.path.exists('Logs'): os.makedirs('Logs')
                if not os.path.exists(self.logs_folder_path): os.makedirs(self.logs_folder_path)
                if not self.logs_filename: self.create_logs_filename()

                query_processor.logs_filename = self.logs_filename

                # getting all brands of store from database
                self.store.brands = query_processor.get_brands()

                if self.store.brands:
                    
                    self.results_foldername = f'{self.path}/scraped_data/{self.store.name}/'
                    
                    if not os.path.exists('scraped_data'): os.makedirs('scraped_data')
                    if not os.path.exists(self.results_foldername): os.makedirs(self.results_foldername)
                    self.remove_extra_scraped_files()
                    self.create_result_filename()

                    print('\n')

                    
                    if self.store.name in ['Digitalhub', 'Safilo', 'Keringeyewear', 'Luxottica']:
                        # download chromedriver.exe with same version and get its path
                        chromedriver_autoinstaller.install(self.path)
                        if self.store.name == 'Digitalhub': Digitalhub_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)
                        elif self.store.name == 'Safilo': Safilo_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)
                        elif self.store.name == 'Keringeyewear': Keringeyewear_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)
                        elif self.store.name == 'Luxottica': Luxottica_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)
                    elif self.store.name == 'Rudyproject': Rudyproject_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)


                    if self.store.name == 'Digitalhub': Digitalhub_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                    elif self.store.name == 'Safilo': Safilo_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                    elif self.store.name == 'Keringeyewear': Keringeyewear_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                    elif self.store.name == 'Rudyproject': Rudyproject_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                    elif self.store.name == 'Luxottica': Luxottica_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                else: print('No brand selected to scrape and update') 

        except Exception as e:
            if self.DEBUG: print(f'Exception in scrape_by_store_controller: {e}')
            self.print_logs(f'Exception in scrape_by_store_controller: {e}')

    def scrape_by_brand_controller(self) -> None:
        try:
            # getting all stores from database
            query_processor = Query_Processor(self.DEBUG, self.config_file, '')
            stores = query_processor.get_stores()

            for store in self.get_store_to_update(stores):
                self.store = store
                query_processor.database_name = str(self.store.name).lower()
                self.logs_folder_path = f'{self.path}/Logs/{self.store.name}/'

                if not os.path.exists('Logs'): os.makedirs('Logs')
                if not os.path.exists(self.logs_folder_path): os.makedirs(self.logs_folder_path)
                if not self.logs_filename: self.create_logs_filename()

                query_processor.logs_filename = self.logs_filename

                # getting all brands of store from database
                all_brands = query_processor.get_brands()

                # getting user selected brands to scrape and update
                self.store.brands = self.get_brands_to_update(all_brands)

                if self.store.brands:
                    for brand in self.store.brands:
                        # getting user selected product type for each brand 
                        selected_product_types = self.get_product_type_to_update(brand, brand.product_types)
                        if selected_product_types: brand.product_types = selected_product_types
                        else: print(f'No product type selected for {brand.name}')

                    
                    self.results_foldername = f'{self.path}/scraped_data/{self.store.name}/'
                    
                    if not os.path.exists('scraped_data'): os.makedirs('scraped_data')
                    if not os.path.exists(self.results_foldername): os.makedirs(self.results_foldername)
                    self.remove_extra_scraped_files()
                    self.create_result_filename()

                    print('\n')

                    
                    if self.store.name in ['Digitalhub', 'Safilo', 'Keringeyewear', 'Luxottica']:
                        # download chromedriver.exe with same version and get its path
                        chromedriver_autoinstaller.install(self.path)
                        if self.store.name == 'Digitalhub': Digitalhub_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)
                        elif self.store.name == 'Safilo': Safilo_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)
                        elif self.store.name == 'Keringeyewear': Keringeyewear_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)
                        elif self.store.name == 'Luxottica': Luxottica_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)
                    elif self.store.name == 'Rudyproject': Rudyproject_Scraper(self.DEBUG, self.result_filename, self.logs_filename).controller(self.store)


                    if self.store.name == 'Digitalhub': Digitalhub_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                    elif self.store.name == 'Safilo': Safilo_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                    elif self.store.name == 'Keringeyewear': Keringeyewear_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                    elif self.store.name == 'Rudyproject': Rudyproject_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                    elif self.store.name == 'Luxottica': Luxottica_Mongodb(self.DEBUG, self.results_foldername, self.logs_filename, query_processor).controller(self.store)
                else: print('No brand selected to scrape and update') 

        except Exception as e:
            if self.DEBUG: print(f'Exception in scrape_by_brand_controller: {e}')
            self.print_logs(f'Exception in scrape_by_brand_controller: {e}')

    # create logs filename
    def create_logs_filename(self) -> None:
        try:
            scrape_time = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
            self.logs_filename = f'{self.logs_folder_path}Logs {scrape_time}.txt'
        except Exception as e:
            self.print_logs(f'Exception in create_logs_filename: {str(e)}')
            if self.DEBUG: print(f'Exception in create_logs_filename: {e}')
            else: pass

    # create result filename
    def create_result_filename(self) -> None:
        try:
            if not self.result_filename:
                scrape_time = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
                self.result_filename = f'{self.results_foldername}Results {scrape_time}.json'
        except Exception as e:
            self.print_logs(f'Exception in create_result_filename: {str(e)}')
            if self.DEBUG: print(f'Exception in create_result_filename: {e}')
            else: pass

    # remove extra scraped files and keep latest 6 files 
    def remove_extra_scraped_files(self) -> None:
        try:
            files = glob.glob(f'{self.results_foldername}*.json')
            while len(files) > 5:
                oldest_file = min(files, key=os.path.getctime)
                os.remove(oldest_file)
                files = glob.glob(f'{self.results_foldername}*.json')

            files = glob.glob(f'{self.logs_folder_path}*.txt')
            while len(files) > 5:
                oldest_file = min(files, key=os.path.getctime)
                os.remove(oldest_file)
                files = glob.glob(f'{self.logs_folder_path}*.txt')
        except Exception as e:
            self.print_logs(f'Exception in remove_extra_scraped_files: {str(e)}')
            if self.DEBUG: print(f'Exception in remove_extra_scraped_files: {e}')
            else: pass

    # get store of user choice
    def get_store_to_update(self, stores: list[Store]) -> list[Store]:
        selected_stores = []
        try:
            print('Select any store to update:')
            for store_index, store in enumerate(stores):
                print(store_index + 1, store.name)

            while True:
                store_choices = ''
                try:
                    store_choices = input('Choice: ')
                    if store_choices:
                        for store_choice in store_choices.split(','):
                            selected_stores.append(stores[int(str(store_choice).strip()) - 1])
                        break
                    else: 
                        selected_stores = []
                        print(f'Please enter number from 1 to {len(stores)}')
                except: 
                    selected_stores = []
                    print(f'Please enter number from 1 to {len(stores)}')
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_store_to_update: {e}')
            else: pass
        finally: return selected_stores

    # get brands of user choice
    def get_brands_to_update(self, brands: list[Brand]) -> list[Brand]:
        selected_brands = []
        try:
            print('\nSelect brands to scrape and update:')
            for brand_index, brand in enumerate(brands):
                print(brand_index + 1, brand.name)


            while True:
                brand_choices = ''
                try:
                    brand_choices = input('Choice: ')
                    if brand_choices:
                        for brand_choice in brand_choices.split(','):
                            selected_brands.append(brands[int(str(brand_choice).strip()) - 1])
                        break
                    else:
                        selected_brands = [] 
                        print(f'Please enter number from 1 to {len(brands)}')
                except Exception as e:
                    selected_brands = []
                    print(f'Please enter number from 1 to {len(brands)}')
        except Exception as e:
            if self.DEBUG: print(f'Exception in get_brands_to_update: {e}')
            else: pass
        finally: return selected_brands

    # get brands product types of user choice
    def get_product_type_to_update(self, brand: Brand, product_types: list[str]) -> list[str]:
        selected_product_types = []
        try:
            print(f'\nSelect product type to update for {brand.name}:')
            for product_type_index, product_type in enumerate(product_types):
                print(product_type_index + 1, str(product_type).title())


            while True:
                product_type_choices = ''
                try:
                    product_type_choices = input('Choice: ')
                    if product_type_choices:
                        for product_type_choice in product_type_choices.split(','):
                            selected_product_types.append(product_types[int(str(product_type_choice).strip()) - 1])
                        break
                    else: 
                        selected_product_types = []
                        print(f'Product type cannot be empty')
                except Exception as e:
                    if self.DEBUG: print(e) 
                    selected_product_types = []
                    print(f'Please enter number from 1 to {len(product_types)}')

        except Exception as e:
            if self.DEBUG: print(f'Exception in get_product_type_to_update: {e}')
            else: pass
        finally: return selected_product_types

    # print logs to the log file
    def print_logs(self, log: str):
        try:
            with open(self.logs_filename, 'a') as f:
                f.write(f'\n{log}')
        except: pass

DEBUG = True
try:
    pathofpyfolder = os.path.realpath(sys.argv[0])
    # get path of Exe folder
    path = pathofpyfolder.replace(pathofpyfolder.split('\\')[-1], '')
    
    if '.exe' in pathofpyfolder.split('\\')[-1]: DEBUG = False

    print('1. Scrape by store\n2. Scrape by brand\n0. Exit')
    while True:
        try:
            choice = int(input('Choice: '))
            print()
            if choice == 1: Controller(DEBUG, path).scrape_by_store_controller()
            elif choice == 2: Controller(DEBUG, path).scrape_by_brand_controller()
            elif choice == 0: break
        except: print('Enter 0, 1 or 2')

except Exception as e:
    if DEBUG: print('Exception: '+str(e))
    else: pass