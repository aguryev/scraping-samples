from sqlalchemy import Column, String, Integer, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

base = declarative_base()

class ProxyItem(base):
	#
	# Proxy Server Item
	#

	__tablename__ = 'proxies'
	# column 
	id = Column(Integer, primary_key=True)
	ip = Column(String(16), nullable=False)
	port = Column(Integer, nullable=False)
	country = Column(String(2), nullable=False)
	anonimity = Column(String(16), nullable=False)
	https_support = Column(Boolean, nullable=False)
	last_checked = Column(DateTime, nullable=False)

	def __str__(self):
		return f'{self.ip}:{self.port}'

