import bs4 as bs
import requests, re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from models import base, ProxyItem

class ProxyList():

	def __init__(self, db_url, url='https://free-proxy-list.net/', ):
		# set up url and header
		self.url = url
		self.set_header()
		self.request_time = None
		# set up db
		self.engine = create_engine(db_url)
		# create tables if not exist
		base.metadata.create_all(self.engine)

	def set_header(self, headers=None):
		if headers:
			self.headers = headers
		else:
			self.headers = {
				'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
			}

	def last_checked(self, status):
		parse_status = re.match(r'(\d{1,2}) (\w+) ago', status)
		if parse_status:
			# get amount an units
			amount = int(parse_status.group(1))
			units = parse_status.group(2)
			# transform units to plural
			if units[-1] != 's':
				units = f'{units}s'

			# get last check time
			if units == 'seconds':
				return self.request_time - timedelta(seconds=amount)
			if units == 'minutes':
				return self.request_time - timedelta(minutes=amount)
			if units == 'hours':
				return self.request_time - timedelta(hours=amount)

		# default time
		return self.request_time



	def parse(self):
		# get response and timestamp
		response = requests.get(self.url, headers=self.headers)
		# >>>>> DEBUG
		#with open('proxy-list.html', 'w') as f:
		#	f.write(response.text)
		# <<<<< DEBUG
		self.request_time = datetime.utcnow()

		# get proxy table
		soup = bs.BeautifulSoup(response.text,'lxml')
		proxy_table = soup.find('tbody')
		
		# extract data to db
		db = sessionmaker(bind=self.engine)()
		for row in proxy_table.find_all('tr'):
			# extract row data
			data = [td.text for td in row.find_all('td')]
			print(data)
			# check if the proxy exists
			item = db.query(ProxyItem).filter_by(ip=data[0]).filter_by(port=int(data[1])).first()
			if item:
				item.country = data[2]
				item.anonimity = data[4]
				item.https_support = (data[6]== 'yes')
				item.last_checked = self.last_checked(data[7])
			else:
				item = ProxyItem(
					ip=data[0],
					port=int(data[1]),
					country=data[2],
					anonimity=data[4],
					https_support= (data[6]== 'yes'),
					last_checked=self.last_checked(data[7]),
				)
				db.add(item)
		db.commit()



