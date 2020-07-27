import os
from shutil import rmtree
from pathlib import Path
from hashlib import sha256
import requests
import scrapy
import json
import csv
import twisted.internet.error as tw_errors
from io import BytesIO
from PIL import Image
from pdb import set_trace as tr
from pprint import pprint
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode
from dakhno.items import PostItem
from dakhno import storage
from dakhno import settings

_proxy = None

def refresh_proxy():
    global _proxy
    _proxy = None
    get_proxy()

def get_proxy():
    global _proxy
    if _proxy:
        return _proxy
    addr = agent = None
    url = 'http://falcon.proxyrotator.com:51337'
    proxy_key = os.getenv('PROXY_KEY')
    data = {}
    res = requests.get(url, params={
        'apiKey': proxy_key,
        'country': 'RU',
        'city': 'Moscow',
    })
    if res.status_code == requests.codes.ok:
        data = res.json()
    addr = data.get('proxy')
    agent = data.get('randomUserAgent')
    if addr and agent:
         _proxy = {'addr': addr, 'agent': agent}
         return _proxy

def contains_captcha(res):
    errors = ['302 Found', 'form_captcha']
    for error in errors:
        if error in res.text:
            return True
    return False

class BaseSpider(scrapy.Spider):

    urls_total = 0
    urls_crawled = 0

    failures = 0
    failures_allowed = 5

    INFO_NAME = '.info'
    FL_NAME = '.fl'

    def __init__(self, **kwargs):
        self.mode = kwargs.get('mode', 'crawl')
        self.kind = kwargs.get('kind', 'apartment')
        super().__init__(**kwargs)

    def start_requests(self):
        if self.mode == 'crawl':
            urls = self.init_queue(self.kind)
            for url in urls:
                self.urls_total += 1
                yield self.url_to_request(url)
        else:
            yield from self.get_post_urls()

    def get_post_ids(self):
        ids = storage.Post.objects.filter(origin=self.name,
            kind=self.kind).values_list('origin_id', flat=True)
        return ids

    def url_to_request(self, url_o):
        url = url_o['url']
        params = url_o.get('params')
        if params:
            parts = urlparse(url)
            qs = parse_qs(parts.query)
            qs.update(params)
            prts = parts._replace(query=urlencode(qs, doseq=True))
            url = urlunparse(prts)
        body = url_o.get('body')
        body_str = None
        if body:
            body_str = json.dumps(body)

        proxy = get_proxy()

        headers = {
            'user-agent': proxy['agent'],
        }
        meta = {
            'url': url_o,
            #'dont_merge_cookies': True
            # handle retrying reqs manually
            'dont_retry': True,
            # handle errors in a callback too
            'handle_httpstatus_all': True,
            'download_timeout': 5,
            'proxy': proxy['addr']
        }

        priority = 10

        cb_key = url_o.get('cb')
        if cb_key == 'single':
            cb = self.handle_post
        elif cb_key == 'image':
            cb = self.handle_image
            priority = 1
        elif cb_key == 'expired':
            cb = self.handle_expired
        else:
            cb = self.handle_list

        # TODO set a referrer
        return scrapy.Request(url=url, priority=priority,
            method=url_o.get('method', 'get'), body=body_str,
            #            allow duplicate requests
            callback=cb, dont_filter=True,
            meta=meta, errback=self.handle_twstd_error)

    def refresh_proxy(self):
        self.logger.info('refreshing proxy')
        refresh_proxy()
        self.failures = 0

    def handle_twstd_error(self, failure):
        url = failure.request.meta.get('url', {})
        msg = failure.getErrorMessage()
        self.logger.error(msg)
        if self.failures > self.failures_allowed:
            self.refresh_proxy()
        else:
            self.failures += 1

        yield self.url_to_request(url)

    def handle_app_errors(self, response):
        try:
            response.text
        except AttributeError:
            return
        url = response.request.meta.get('url', {})
        restart = False
        if response.status > 200:
            self.logger.error('got a non 200 response')
            if 'image-temp' in response.url:
                self.logger.error('image-temp cian 301')
                _url = response.headers.get('location', '').decode()
                if _url:
                    url['url'] = _url
                restart = True
            elif 'Объявление не найдено' in response.text or 'Объявление закрыто' in response.text:
                self.logger.error('avito ad not found')
                self.urls_crawled += 1
                pass
            else:
                restart = True
        if self.name == 'avito':
            res = json.loads(response.text)
            if res.get('error', {}).get('code'):
                restart = True
        if contains_captcha(response):
            restart = True
            self.logger.error('hit a captcha')
            self.refresh_proxy()
        if restart:
            if self.failures > self.failures_allowed:
                self.refresh_proxy()
            else:
                self.failures += 1
            return self.url_to_request(url)

    def get_images_path(self, post_id):
        base = settings.MEDIA_PATH
        kind = '%ss' % self.kind
        pth = os.path.join(base, kind, self.name, str(post_id))
        return pth

    def init_bookkeep_files(self, post_id, total):
        base_path = self.get_images_path(post_id)
        path = Path(base_path)
        path.mkdir(parents=True, exist_ok=True)
        with open(os.path.join(base_path, self.INFO_NAME), 'w') as f:
            info = '0/%d' % total
            f.write(info)
        with open(os.path.join(base_path, self.FL_NAME), 'w') as f:
            f.write('[]')

    def increment_info_file(self, post_id):
        base_path = self.get_images_path(post_id)
        INFO_NAME = '.info'
        pth = os.path.join(base_path, INFO_NAME)
        info = ''
        with open(pth) as f:
            info = f.read()
        try:
            current, total = info.split('/')
        except:
            pass
            #tr()
        with open(pth, 'w+') as f:
            info = '%d/%d' % (int(current) + 1, int(total))
            f.write(info)

    def handle_image(self, response):
        req = self.handle_app_errors(response)
        if req:
            yield req
            return
        post_id = response.request.meta.get('url', {}).get('meta', {}).get('post_id')
        o_url = response.request.url
        ext = o_url.split('.')[-1]
        hsh = sha256(o_url.encode()).hexdigest()
        title = '%s.%s' % (hsh, ext)
        base_path = self.get_images_path(post_id)
        fl_path = os.path.join(base_path, title)
        try:
            cropped = self.crop(BytesIO(response.body))
            cropped.save(fl_path, subsampling=0, quality=95)
        except:
            #tr()
            self.urls_crawled += 1
            self.increment_info_file(post_id)
            self.logger.info('there was an error with the cropping')
            self.logger.info('crawled %d out of %d', self.urls_crawled, self.urls_total)
            return
        fl = open(os.path.join(base_path, self.FL_NAME))
        names = json.loads(fl.read())
        fl.close()
        names.append(title)
        with open(os.path.join(base_path, self.FL_NAME), 'w') as f:
            res = json.dumps(names)
            f.write(res)
        self.urls_crawled += 1
        self.increment_info_file(post_id)
        self.logger.info('crawled %d out of %d', self.urls_crawled, self.urls_total)

    def crop(self, image):
        image_obj = Image.open(image)
        width, height = image_obj.size
        ln = 155
        if self.name == 'avito':
            ln = 40
        new_ln = height - ln
        if new_ln < 0:
            new_ln = ln
        cropped = image_obj.crop((0, 0, width, new_ln))
        return cropped

    def clean_images(self, post_id):
        pth = self.get_images_path(post_id)
        try:
            self.logger.info('cleaning images: %s', pth)
            rmtree(pth)
        except FileNotFoundError:
            self.logger.info('tried deleting %d post images but there are none',
                post_id)
