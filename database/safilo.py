import os
import json
import glob
from datetime import datetime

from modules.query_processor import Query_Processor

from models.store import Store
from models.brand import Brand
from models.product import Product
from models.variant import Variant
from models.metafields import Metafields

class Safilo_Mongodb:
    def __init__(self, DEBUG: bool, results_foldername: str, logs_filename: str, query_processor: Query_Processor) -> None:
        self.DEBUG: bool = DEBUG
        self.results_foldername = results_foldername
        self.logs_filename = logs_filename
        self.query_processor = query_processor
        pass

    def controller(self, store: Store) -> None:
        try:
            print('Updating database...')

            self.read_data_from_json_file(store.brands)

            for brand in store.brands:
                
                print(f'Brand: {brand.name}: Products: {len(brand.products)}')

                self.printProgressBar(1, len(brand.products) + 1, prefix = 'Progress:', suffix = 'Complete', length = 50)

                db_products = self.query_processor.get_products_by_brand(brand.name)

                for index, product in enumerate(brand.products):

                    matched_db_product = self.get_matched_product(product, db_products)
                    
                    if matched_db_product:

                        self.check_product_feilds(product, matched_db_product)
                        self.check_product_metafeilds(product.metafields, matched_db_product['metafields'], matched_db_product["_id"])

                        self.query_processor.update_variant({"product_id": matched_db_product["_id"]}, {"$set": {"found_status": 0, "inventory_quantity": 0} })
                        
                        db_variants = self.query_processor.get_variants_by_product_id(product.id)

                        for variant in product.variants:
                            matched_db_variant = self.get_matched_variant(variant, db_variants)

                            if matched_db_variant:
                                self.check_variant_fields(variant, matched_db_variant)
                            else: 
                                self.add_new_product(variant, matched_db_product['_id'])
                    else: 
                        self.add_new_product(product)

                    self.printProgressBar(index + 2, len(brand.products) + 1, prefix = 'Progress:', suffix = 'Complete', length = 50)

                if db_products:
                    print('\nSetting found status for not found products')
                    self.printProgressBar(1, len(db_products) + 1, prefix = 'Progress:', suffix = 'Complete', length = 50)
                    for index, db_product in enumerate(db_products):
                        self.printProgressBar(index + 2, len(db_products) + 1, prefix = 'Progress:', suffix = 'Complete', length = 50)
                        self.query_processor.update_variant({"product_id": db_product.id}, {"$set": {"found_status": 0, "inventory_quantity": 0} })
                    print()

        except Exception as e:
            if self.DEBUG: print(f'Exception in Safilo_Mongodb: controller: {e}')
            self.print_logs(f'Exception in Safilo_Mongodb: controller: {e}')

    # read data from json file
    def read_data_from_json_file(self, brands: list[Brand]):
        try:
            files = glob.glob(f'{self.results_foldername}*.json')
            if files:
                latest_file = max(files, key=os.path.getctime)
                
                f = open(latest_file)
                json_data = json.loads(f.read())
                f.close()
                products: list[Product] = []

                for json_d in json_data:
                    product = Product()
                    product.id = str(json_d['_id']).strip()
                    product.number = str(json_d['number']).strip().upper()
                    product.name = str(json_d['name']).strip()
                    product.brand = str(json_d['brand']).strip()
                    product.frame_code = str(json_d['frame_code']).strip().upper()
                    product.lens_code = str(json_d['lens_code']).strip().upper()
                    product.type = str(json_d['type']).strip().title()
                    product.bridge = str(json_d['bridge']).strip()
                    product.template = str(json_d['template']).strip()
                    product.image = str(json_d['image']).strip()
                    product.images_360 = json_d['images_360']

                    product.metafields.for_who = str(json_d['metafields']['for_who']).strip().title()
                    product.metafields.lens_material = str(json_d['metafields']['lens_material']).strip().title()
                    product.metafields.lens_technology = str(json_d['metafields']['lens_technology']).strip().title()
                    product.metafields.lens_color = str(json_d['metafields']['lens_color']).strip().title()
                    product.metafields.frame_shape = str(json_d['metafields']['frame_shape']).strip().title()
                    product.metafields.frame_material = str(json_d['metafields']['frame_material']).strip().title()
                    product.metafields.frame_color = str(json_d['metafields']['frame_color']).strip().title()
                    product.metafields.size_bridge_template = str(json_d['metafields']['size-bridge-template']).strip()
                    product.metafields.gtin1 = str(json_d['metafields']['gtin1']).strip()
                    
                    variants = []
                    for json_variant in json_d['variants']:
                        variant = Variant()
                        variant.id = str(json_variant['_id']).strip()
                        variant.product_id = str(json_variant['product_id']).strip()
                        variant.title = str(json_variant['title']).strip()
                        variant.sku = str(json_variant['sku']).strip().upper()
                        variant.inventory_quantity = int(json_variant['inventory_quantity'])
                        variant.found_status = int(json_variant['found_status'])
                        variant.wholesale_price = float(json_variant['wholesale_price'])
                        variant.listing_price = float(json_variant['listing_price'])
                        variant.barcode_or_gtin = str(json_variant['barcode_or_gtin']).strip()
                        
                        variants.append(variant)
                    product.variants = variants 
                    products.append(product)

                for brand in brands:
                    brand_products = []
                    for product in products:
                        if product.brand == brand.name:
                            brand_products.append(product)
                    brand.products = brand_products
        except Exception as e:
            self.print_logs(f'Exception in read_data_from_json_file: {str(e)}')
            if self.DEBUG: print(f'Exception in read_data_from_json_file: {e}')
            else: pass

    def get_matched_product(self, product: Product, db_products: list[dict]) -> dict:
        matched_db_product: dict = {}
        try:
            matched_index = -1
            for index, db_product in enumerate(db_products):
                if product.id == db_product['_id']:
                    matched_index = index
                    break

            if matched_index != -1:
                matched_db_product = db_products.pop(index)
        except Exception as e:
            self.print_logs(f'Exception in get_matched_product: {e}')
            if self.DEBUG: print(f'Exception in get_matched_product: {e}')
        finally: return matched_db_product

    def get_matched_variant(self, variant: Variant, db_variants: list[dict]) -> dict:
        matched_db_variant: dict = {}
        try:
            matched_index = -1
            for index, db_variant in enumerate(db_variants):
                if variant.id == db_variant['_id']:
                    matched_index = index
                    break

            if matched_index != -1:
                matched_db_variant = db_variants.pop(index)

        except Exception as e:
            self.print_logs(f'Exception in get_matched_variant: {e}')
            if self.DEBUG: print(f'Exception in get_matched_variant: {e}')
        finally: return matched_db_variant

    def check_product_feilds(self, product: Product, matched_db_product: dict) -> None:
        try:
            if product.name and product.name != matched_db_product['name']:
                self.query_processor.update_product({'_id': product.id}, {"$set": {"name": product.name, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update name from {matched_db_product["name"]} to {product.name} for product: {matched_db_product["_id"]}')

            if product.type and product.type != matched_db_product['type']:
                self.query_processor.update_product({'_id': product.id}, {"$set": {"type": product.type, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update name from {matched_db_product["name"]} to {product.name} for product: {matched_db_product["_id"]}')

            if product.bridge and product.bridge != matched_db_product['bridge']:
                self.query_processor.update_product({'_id': product.id}, {"$set": {"bridge": product.bridge, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update bridge from {matched_db_product["bridge"]} to {product.bridge} for product: {matched_db_product["_id"]}')
            
            if product.template and product.template != matched_db_product['template']:
                self.query_processor.update_product({'_id': product.id}, {"$set": {"template": product.template, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update template from {matched_db_product["template"]} to {product.template} for product: {matched_db_product["_id"]}')

            if product.image and product.image != matched_db_product['image']:
                self.query_processor.update_product({'_id': product.id}, {"$set": {"image": product.image, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update image from {matched_db_product["image"]} to {product.image} for product: {matched_db_product["_id"]}')

            if len(product.images_360) != 0 and product.images_360 != matched_db_product['images_360']:
                self.query_processor.update_product({'_id': product.id}, {"$set": {"images_360": product.images_360, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update images_360 from {matched_db_product["images_360"]} to {product.images_360} for product: {matched_db_product["_id"]}')

        except Exception as e:
            if self.DEBUG: print(f'Exception in check_product_feilds: {e}')
            self.print_logs(f'Exception in check_product_feilds: {e} {matched_db_product}')

    def check_product_metafeilds(self, metafields: Metafields, matched_db_product_metafields: dict, product_id: str) -> None:
        try:
            if metafields.for_who and metafields.for_who != matched_db_product_metafields['for_who']:
                self.query_processor.update_product({'_id': product_id}, {"$set": {"metafields.for_who": metafields.for_who}})
                # self.print_logs(f'Update for_who metafield from {matched_db_product_metafields["for_who"]["value"]} to {metafields.for_who} for product: {product_id}')

            if metafields.lens_material and metafields.lens_material != matched_db_product_metafields['lens_material']:
                self.query_processor.update_product({'_id': product_id}, {"$set": {"metafields.lens_material": metafields.lens_material}})
                # self.print_logs(f'Update lens_material metafield from {matched_db_product_metafields["lens_material"]["value"]} to {metafields.lens_material} for product: {product_id}')

            if metafields.lens_technology and metafields.lens_technology != matched_db_product_metafields['lens_technology']:
                self.query_processor.update_product({'_id': product_id}, {"$set": {"metafields.lens_technology": metafields.lens_technology}})
                # self.print_logs(f'Update lens_technology metafield from {matched_db_product_metafields["lens_technology"]["value"]} to {metafields.lens_technology} for product: {product_id}')

            if metafields.lens_color and metafields.lens_color != matched_db_product_metafields['lens_color']:
                self.query_processor.update_product({'_id': product_id}, {"$set": {"metafields.lens_color": metafields.lens_color}})
                # self.print_logs(f'Update lens_color metafield from {matched_db_product_metafields["lens_color"]["value"]} to {metafields.lens_color} for product: {product_id}')

            if metafields.frame_shape and metafields.frame_shape != matched_db_product_metafields['frame_shape']:
                self.query_processor.update_product({'_id': product_id}, {"$set": {"metafields.frame_shape": metafields.frame_shape}})
                # self.print_logs(f'Update frame_shape metafield from {matched_db_product_metafields["frame_shape"]["value"]} to {metafields.frame_shape} for product: {product_id}')

            if metafields.frame_material and metafields.frame_material != matched_db_product_metafields['frame_material']:
                self.query_processor.update_product({'_id': product_id}, {"$set": {"metafields.frame_material": metafields.frame_material}})
                # self.print_logs(f'Update frame_material metafield from {matched_db_product_metafields["frame_material"]["value"]} to {metafields.frame_material} for product: {product_id}')

            if metafields.frame_color and metafields.frame_color != matched_db_product_metafields['frame_color']:
                self.query_processor.update_product({'_id': product_id}, {"$set": {"metafields.frame_color": metafields.frame_color}})
                # self.print_logs(f'Update frame_color metafield from {matched_db_product_metafields["frame_color"]["value"]} to {metafields.frame_color} for product: {product_id}')

            if metafields.size_bridge_template and metafields.size_bridge_template != matched_db_product_metafields['size-bridge-template']:
                self.query_processor.update_product({'_id': product_id}, {"$set": {"metafields.size-bridge-template": metafields.size_bridge_template}})
                # self.print_logs(f'Update size-bridge-template metafield from {matched_db_product_metafields["size-bridge-template"]["value"]} to {metafields.size_bridge_template} for product: {product_id}')

            if metafields.gtin1 and metafields.gtin1 != matched_db_product_metafields['gtin1']:
                self.query_processor.update_product({'_id': product_id}, {"$set": {"metafields.gtin1": metafields.gtin1}})
                # self.print_logs(f'Update gtin1 metafield from {matched_db_product_metafields["gtin1"]["value"]} to {metafields.gtin1} for product: {product_id}')


        except Exception as e:
            if self.DEBUG: print(f'Exception in check_product_metafeilds: {e}')
            self.print_logs(f'Exception in check_product_metafeilds: {e}')

    def check_variant_fields(self, variant: Variant, matched_db_variant: dict) -> None:
        try:
            if matched_db_variant['found_status'] == 0:
                self.query_processor.update_variant({'_id': variant.id}, {"$set": {"found_status": 1, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update found_status from {matched_db_variant["found_status"]} to 1 for variant: {variant.id}')

            if variant.inventory_quantity != matched_db_variant['inventory_quantity']:
                self.query_processor.update_variant({'_id': variant.id}, {"$set": {"inventory_quantity": variant.inventory_quantity, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update inventory_quantity from {matched_db_variant["inventory_quantity"]} to {variant.inventory_quantity} for variant: {variant.id}')


            if variant.wholesale_price != 0.0 and variant.wholesale_price != matched_db_variant['wholesale_price']:
                self.query_processor.update_variant({'_id': variant.id}, {"$set": {"wholesale_price": variant.wholesale_price, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update wholesale_price from {matched_db_variant["wholesale_price"]} to {variant.wholesale_price} for variant: {variant.id}')

            if variant.listing_price != 0.0 and variant.listing_price != matched_db_variant['listing_price']:
                self.query_processor.update_variant({'_id': variant.id}, {"$set": {"listing_price": variant.listing_price, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update listing_price from {matched_db_variant["listing_price"]} to {variant.listing_price} for variant: {variant.id}')

            if variant.barcode_or_gtin and variant.barcode_or_gtin != matched_db_variant['barcode_or_gtin']:
                self.query_processor.update_variant({'_id': variant.id}, {"$set": {"barcode_or_gtin": variant.barcode_or_gtin, "updated_at": datetime.utcnow()}})
                # self.print_logs(f'Update barcode_or_gtin from {matched_db_variant["barcode_or_gtin"]} to {variant.barcode_or_gtin} for variant: {variant.id}')

        except Exception as e:
            if self.DEBUG: print(f'Exception in check_variant_fields: {e}')
            self.print_logs(f'Exception in check_variant_fields: {e}')

    def add_new_product(self, product: Product) -> None:
        try:
            # first add new product to shopify then to database
            json_product = {
                "_id": product.id,
                'number': product.number,
                'name': product.name,
                'brand': product.brand,
                'frame_code': product.frame_code,
                'lens_code': product.lens_code,
                'type': product.type,
                'bridge': product.bridge,
                'template': product.template,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                'shopify_id': product.shopify_id,
                'metafields': {
                    'for_who': product.metafields.for_who,
                    'lens_material': product.metafields.lens_material,
                    'lens_technology': product.metafields.lens_technology,
                    'lens_color': product.metafields.lens_color,
                    'frame_shape': product.metafields.frame_shape,
                    'frame_material': product.metafields.frame_material,
                    'frame_color': product.metafields.frame_color,
                    'size-bridge-template': product.metafields.size_bridge_template,
                    'gtin1': product.metafields.gtin1
                },
                'image': product.image,
                'images_360': product.images_360
            }

            new_json_product = self.query_processor.insert_product(json_product)
            
            if new_json_product:
                self.print_logs(f'New product added _id {new_json_product.inserted_id}')
                for variant in product.variants:
                    self.add_new_variant(variant, product.id)
        
        except Exception as e:
            if self.DEBUG: print(f'Exception in add_new_product: {e}')
            self.print_logs(f'Exception in add_new_product: {e}')

    def add_new_variant(self, variant: Variant, product_id: str) -> None:
        try:
            json_variant = {
                '_id': variant.id,
                'product_id': product_id,
                'title': variant.title,
                'sku': variant.sku,
                'inventory_quantity': variant.inventory_quantity,
                'found_status': variant.found_status,
                'wholesale_price': variant.wholesale_price,
                'listing_price': variant.listing_price,
                'barcode_or_gtin': variant.barcode_or_gtin,
                'shopify_id': variant.shopify_id,
                'inventory_item_id': variant.inventory_item_id,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }

            new_json_variant = self.query_processor.insert_variant(json_variant)
            if new_json_variant:
                self.print_logs(f'New variant added _id {new_json_variant.inserted_id}')
        except Exception as e:
            if self.DEBUG: print(f'Exception in add_new_variant: {e}')
            self.print_logs(f'Exception in add_new_variant: {e}')

    # def update_not_found_variant(self, db_variant: dict) -> None:
    #     try:
    #         if db_variant['found_status'] == 1:
    #             self.query_processor.update_variant({'_id': db_variant['_id']}, {"$set": {"found_status": 0, "updated_at": datetime.utcnow()}})
    #             # self.print_logs(f'Variant {db_variant["_id"]} not found. Changing status to 0')
    #         if db_variant['inventory_quantity'] == 1:
    #             self.query_processor.update_variant({'_id': db_variant['_id']}, {"$set": {"inventory_quantity": 0, "updated_at": datetime.utcnow()}})
    #             # self.print_logs(f'Variant {db_variant["_id"]} not found. Changing inventory to 0')
    #     except Exception as e:
    #         if self.DEBUG: print(f'Exception in update_not_found_variant: {e}')
    #         self.print_logs(f'Exception in update_not_found_variant: {e}')

    # print logs to the log file
    def print_logs(self, log: str):
        try:
            with open(self.logs_filename, 'a') as f:
                f.write(f'\n{log}')
        except: pass

    def printProgressBar(self, iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r") -> None:
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
        # Print New Line on Complete
        if iteration == total: 
            print()
