# -*- coding: utf-8 -*-
import json
from dakhno.items import PostItem
from dakhno.spiders.base import BaseSpider
from dakhno import storage
from pdb import set_trace as tr

class CianSpider(BaseSpider):
    name = 'cian'

    def generate_page_body(self, page=1, kind='apartment', price_min=None, price_max=None):
        if kind == 'apartment':
            # flat with such number of rooms
            val = [1,2,3,4,5,6,7,9]
            #price = 20000
        else:
            # just a room
            val = [0]
            price_min = 9000
            price_max = None
        query = {
            "jsonQuery": {
                "_type": "flatrent",
                "room": {
                    "type": "terms",
                    "value": val
                },
                "for_day": {
                    "type": "term", "value":"!1"
                },
                "region": {
                    "type": "terms", "value":[1]
                },
                "engine_version": {
                    "type": "term",
                    "value": 2
                },
                "page": {
                    "type": "term",
                    "value": page
                }
            }
        }
        if price_min or price_max:
            o = {
                "type": "range",
                "value": {}
            }
            if price_min:
                o["value"]["gte"] = price_min

            if price_max:
                o["value"]["lte"] = price_max

            query["jsonQuery"]["price"] = o

        return query

    def init_queue(self, kind='apartment'):
        lst = []
        price_ranges = [(20000,60000), (60001, 85000), (85001, 110000)]
        for prc in price_ranges:
            for i in range(1, 61):
                pttp = {
                    'url': 'https://api.cian.ru/search-offers/v2/search-offers-desktop/',
                    'method': 'post',
                    'cb': 'list'
                }
                pttp['meta'] = {'page': i}
                pttp['body'] = self.generate_page_body(page=i, kind=kind,
                    price_min=prc[0], price_max=prc[1])
                lst.append(pttp)
        return lst

    def get_post_urls(self):
        ids = self.get_post_ids()
        for post_id in ids:
            url = 'https://www.cian.ru/rent/flat/%d/' % post_id
            url_o = {
                'url': url,
                'cb': 'expired',
                'meta': {
                    'post_id': post_id
                }
            }
            self.urls_total += 1
            yield self.url_to_request(url_o)

    def extract_data(self, post):
        def _extract_phones(phones):
            phones_list = []
            for phone in phones:
                phones_list.append('+7%s' % phone.get('number'))
            return ', '.join(phones_list)

        def _extract_address(geo):
            address = geo.get('address', [])
            addr_dict = {}
            for line in address:
                kind = line.get('type', 'other')
                if kind == 'metro':
                    addr_dict['station_id'] = line.get('id')
                else:
                    addr_dict[kind] = line.get('shortName')
            # probably do smth with addr_dict in the future
            addr_dict['lat'] = geo.get('coordinates', {}).get('lat')
            addr_dict['lng'] = geo.get('coordinates', {}).get('lng')
            return addr_dict

        phones_str = _extract_phones(post.get('phones', []))
        address_dict = _extract_address(post.get('geo', {}))

        address_comp = []
        if address_dict.get('street'):
            address_comp.append(address_dict.get('street'))
        if address_dict.get('house'):
            address_comp.append(address_dict.get('house'))
        address_str = ', '.join(address_comp)

        is_agency = post['isByHomeowner'] != True or post['bargainTerms']['clientFee'] > 0

        data = {
            'address': address_str,
            'lat': address_dict.get('lat'),
            'lng': address_dict.get('lng'),
            'station_id': address_dict.get('station_id'),
            'price': post.get('bargainTerms', {}).get('price'),
            'currency': post.get('bargainTerms', {}).get('currency'),
            'description': post.get('description'),
            'floors_total': post.get('building', {}).get('floorsCount'),
            'floor': post.get('floorNumber'),
            'rooms': post.get('roomsCount'),
            'area': post.get('totalArea'),
            'origin': self.name,
            'origin_id': post.get('id'),
            'name': post.get('user', {}).get('agencyName', ''),
            'phones': phones_str,
            'kind': self.kind,
            #'creation_date': post.get('creationDate'),
            'added': post.get('addedTimestamp'),
            'is_agency': is_agency,
        }

        itm = PostItem(**data)
        if not storage.check_if_post_already_exists(itm):
            self.init_bookkeep_files(post.get('id'), len(post.get('photos', [])))
            yield from self.extract_images_urls(post)
            yield itm

    def handle_list(self, response):
        req = self.handle_app_errors(response)
        if req:
            yield req
            return
        try:
            res = json.loads(response.text)
        except:
            url = response.request.meta.get('url', {})
            self.failures += 1
            self.logger.error('JSON parse error retrying')
            yield self.url_to_request(url)
            return
        data = res['data']
        ag_count = data['aggregatedCount']
        of_count = data['offerCount']
        records = data['offersSerialized']
        count = of_count
        for post in records:
            yield from self.extract_data(post)
        self.urls_crawled += 1
        self.logger.info('crawled %d out of %d', self.urls_crawled, self.urls_total)

    def extract_images_urls(self, data):
        for item in data.get('photos'):
            url = {
                'url': item.get('fullUrl', ''),
                'cb': 'image',
                'meta': {
                    'post_id': data['id']
                },
            }
            self.urls_total += 1
            yield self.url_to_request(url)

    def contains_captcha(self, res):
        errors = ['302 Found', 'form_captcha']
        for error in errors:
            if error in res.text:
                return True
        return False

    def handle_expired(self, response):
        if self.contains_captcha(response):
            self.logger.error('hit a captcha')
            self.refresh_proxy()
            url = response.request.meta.get('url', {})
            yield self.url_to_request(url)
            return
        if response.status == 404 or (response.status == 200 and \
            'Объявление снято с публикации' in response.text):
            post_id = response.request.meta.get('url', {}).get('meta', {}).get('post_id')
            if post_id:
                self.clean_images(post_id)
                yield {'id': post_id}
