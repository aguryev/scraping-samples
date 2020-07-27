# -*- coding: utf-8 -*-
import json
from pdb import set_trace as tr
from dakhno.items import PostItem
from dakhno.spiders.base import BaseSpider
from dakhno import storage

AVITO_KEY = os.getenv('AVITO_KEY')

class AvitoSpider(BaseSpider):
    name = 'avito'

    def generate_page_body(self, page=1, kind='apartment', owner=True, agency=True):
        if kind == 'apartment':
            prs = {
                'categoryId': 24,
                'params[201]': 1060,
                'params[504]': 5256,
                'priceMin': 20000,
            }
        else:
            prs = {
                'categoryId': 23,
                'params[200]': 1055,
                'params[596]': 6203,
                'priceMin': 9000,
            }
        prs.update({
            'key': AVITO_KEY,
            'user': 1,
            'locationId': 637640,
            'page': page,
            'display': 'list',
            'limit': 30,
        })

        if owner:
            prs.update({'owner[]': 'private'})

        if agency:
            prs.update({'owner[]': 'company'})

        return prs

    def init_queue(self, kind='apartment'):
        lst = []
        for i in range(1, 11):
            pttp = {
                'url': 'https://m.avito.ru/api/9/items',
                'params': self.generate_page_body(page=i, kind=kind),
                'cb': 'list' 
            }
            pttp['meta'] = {'page': i}
            lst.append(pttp)
        return lst

    #def test_post(self):
    #    post_id = 1413371531
    #    url = 'https://m.avito.ru/api/10/items/%d' % post_id
    #    url_o = {
    #        'url': url,
    #        'params': {
    #            'key': AVITO_KEY
    #        },
    #        'cb': 'expired',
    #        'cb': 'single',
    #    }
    #    return self.url_to_request(url_o)

    def get_post_urls(self):
        ids = self.get_post_ids()
        for post_id in ids:
            url = 'https://m.avito.ru/api/10/items/%d' % post_id
            self.logger.info('Expired link %s', url)
            url_o = {
                'url': url,
                'params': {
                    'key': AVITO_KEY
                },
                'cb': 'expired',
                'meta': {
                    'post_id': post_id
                }
            }
            self.urls_total += 1
            yield self.url_to_request(url_o)

    def extract_data(self, post):
        def _extract_phones():
            lst = post['contacts']['list']
            phones_list = []
            for item in lst:
                if item['type'] == 'phone':
                    phone_uri = item['value']['uri']
                    phone = phone_uri.split('number=')[-1].replace('%2B', '+')
                    phones_list.append(phone)
            return ', '.join(phones_list)

        def _extract_address():
            # a hack to merge two stations
            m_id = post.get('metroId')
            if m_id == 2163:
                station = 2146
            else:
                station = m_id
            addr_dict = {
                'address': post['address'].replace('Москва, ', ''),
                'metro_id': station,
                'coords': post['coords'],
            }
            # probably do smth with addr_dict in the future
            return addr_dict

        def _parse_apt_title():
            values = post['title'].split(', ')
            if values[0] == 'Студия':
                rooms = '1'
            else:
                rooms = values[0].split(' ')[0][0:1]
            area = values[1].split(' ')[0]
            floor, floors_total = values[2].split(' ')[0].split('/')
            return {
                'rooms': rooms,
                'area': area,
                'floor': floor,
                'floors_total': floors_total,
            }

        def _parse_room_title():
            vals = post['title'].split(' ')[1:]
            values = []
            for val in vals:
                if val != '>':
                    values.append(val)
            rooms = values[3][0:-3]
            area = values[0]
            floor, floors_total = values[4].split('/')
            return {
                'rooms': rooms,
                'area': area,
                'floor': floor,
                'floors_total': floors_total,
            }

        def _parse_currency():
            values = post['price'].get('metric', '').split(' ')
            if values:
                currency = values[0]
                return currency

        def _parse_is_agency():
            return post['firebaseParams']['commission'] != 'Собственник'

        try:
            phones_str = _extract_phones()
            address_dict = _extract_address()
            if self.kind == 'apartment':
                parsed = _parse_apt_title()
            else:
                parsed = _parse_room_title()
            currency = _parse_currency()
            is_agency = _parse_is_agency()
        except:
            tr()

        data = {
            'address': address_dict.get('address'),
            'station_id': address_dict.get('metro_id'),
            'lat': address_dict.get('coords', {}).get('lat'),
            'lng': address_dict.get('coords', {}).get('lng'),
            'price': post['price']['value'],
            'currency': currency,
            'description': post['description'],
            'origin': self.name,
            'origin_id': post['id'],
            'phones': phones_str,
            'name': post['seller']['name'],
            'added': post['time'],
            'kind': self.kind,
            'is_agency': is_agency,
        }
        data['price'] = "".join(data['price'].split())
        data.update(parsed)
        itm = PostItem(**data)
        if not storage.check_if_post_already_exists(itm):
            self.init_bookkeep_files(post.get('id'), len(post.get('images', [])))
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
        data = res['result']
        count = data['count']
        records = data['items']
        yield from self.extract_posts_urls(records)
        self.urls_crawled += 1
        self.logger.info('crawled %d out of %d', self.urls_crawled, self.urls_total)

    def extract_posts_urls(self, posts):
        for post in posts:
            value = post['value']
            lst = value.get('list')
            if lst:
                post_id = lst[0]['value']['id']
            else:
                post_id = value['id']
            url = 'https://m.avito.ru/api/10/items/%d' % post_id
            url_o = {
                'url': url,
                'params': {
                    'key': AVITO_KEY
                },
                'cb': 'single',
            }
            self.urls_total += 1
            yield self.url_to_request(url_o)

    def handle_post(self, response):
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
        yield from self.extract_data(res)
        self.urls_crawled += 1
        self.logger.info('crawled %d out of %d', self.urls_crawled, self.urls_total)

    def extract_images_urls(self, data):
        for item in data.get('images', []):
            url = item['640x480']
            url_o = {
                'url': url,
                'cb': 'image',
                'meta': {
                    'post_id': data['id']
                },
            }
            self.urls_total += 1
            yield self.url_to_request(url_o)

    def handle_expired(self, response):
        if response.status == 302 or response.status == 404:
            post_id = response.request.meta.get('url', {}).get('meta', {}).get('post_id')
            self.logger.info('Expired %d', post_id)
            if post_id:
                self.clean_images(post_id)
                yield {'id': post_id}
