from models import ProxyItem
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import requests
from datetime import datetime

engine = create_engine(datetime.now().strftime("sqlite:///proxy_%Y%m%d.db"))
session = sessionmaker(bind=engine)()

plist = session.query(ProxyItem).all()

test_url = 'https://www.google.com/search?client=ubuntu&channel=fs&q=where+am++i&ie=utf-8&oe=utf-8'
headers = {
	'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
}
for p in plist:
	proxies = {h: f'{h}://{p}' for h in ['http', 'https']}
	try:
		r = requests.get(test_url, proxies=proxies, verify=False, timeout=10)
		print(f'\n{p}\t{r.status_code}')
	except Exception as e:
		print(f"\n{'*'*30}\n{p}\n{e}\n{'*'*30}")
