from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

base = declarative_base()

def date_to_str(date):
	if date:
		return date.strftime("%m/%d/%Y")
	return None

class Company(base):
	#
	# Company Items
	#

	__tablename__ = 'companies'
	# columns
	id = Column(Integer, primary_key=True)
	title = Column(String, nullable=False)
	status = Column(String, nullable=True)
	registration_date = Column(DateTime, nullable=True)
	withdrawal_date = Column(DateTime, nullable=True)
	# relationships
	annual_reports = relationship('AnnualReport')

	def __str__(self):
		return self.title

	def to_json(self):
		return {
			'Company Title': self.title,
			'Registration Status': self.status,
			'Registration Date': date_to_str(self.registration_date),
			'Withdrawal Date': date_to_str(self.withdrawal_date),
			'Annual Reports': [r.to_json() for r in self.annual_reports],
		}


class AnnualReport(base):
	#
	# Annual Report Items
	#

	__tablename__ = 'annual_reports'
	# columns
	id = Column(Integer, primary_key=True)
	form = Column(String, nullable=False)
	filed_date = Column(DateTime, nullable=True)
	company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
	# relationships
	company = relationship('Company', foreign_keys=[company_id])

	def __str__(self):
		return self.title

	def to_json(self):
		return {
			'Form': self.form,
			'Filed Date': date_to_str(self.filed_date),
		}

