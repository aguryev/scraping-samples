from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import time, json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import base, Company, AnnualReport

class PCAOB:
	url = 'https://rasr.pcaobus.org/Search/Search.aspx'
	path_to_driver = 'webdriver/geckodriver'
	items_per_page = 25

	def __init__(self, page=1, index=0):
		self.set_options()
		self.driver = webdriver.Firefox(executable_path=self.path_to_driver, options=self.options)
		self.page = page
		self.index = index
		self.results = []


	def set_options(self):
		#
		# set up web-driver options
		#

		self.options = Options()
		self.options.headless = True


	@classmethod
	def db_to_json(self, db_url, path_to_json):
		#
		# dump db to json
		#

		# set up db
		engine = create_engine(db_url)
		db = sessionmaker(bind=engine)()

		# get companies
		companies = db.query(Company).all()

		# build json
		data = []
		for c in companies:
			data.append(c.to_json())

		# dump json
		with open(path_to_json, 'w') as f:
			json.dump(data, f)

		print(f'DB imported to json: {path_to_json}')


	@classmethod
	def str_to_date(self, date):
		try:
			return datetime.strptime(date, "%m/%d/%Y")
		except Exception:
			return None


	def stop_item(self):
		return self.page, self.index


	def to_json(self, path_to_json):
		#
		# dump results to json
		#

		try:
			with open(path_to_json, 'r') as f:
				results = json.load(f)
			results.update(self.results)
		except Exception:
			results = self.results

		with open(path_to_json, 'w') as f:
			json.dump(results, f)

		print(f'Results imported to json: {path_to_json}')


	def to_db(self, db_url):
		#
		# dump results to db
		#

		# create tables if not exist
		engine = create_engine(db_url)
		base.metadata.create_all(engine)

		# dump data
		db = sessionmaker(bind=engine)()
		for record in self.results:
			# create item
			company = Company(
				title=record.get('Company Title'),
				status=record.get('Registration Status'),
				registration_date=self.str_to_date(record.get('Registration Date')),
				withdrawal_date=self.str_to_date(record.get('Withdrawal Date')),
			)
			db.add(company)

			reports = record.get('Annual Reports')
			if reports:
				for r in reports:
					report = AnnualReport(
						form=r.get('Form'),
						filed_date=self.str_to_date(r.get('Filed Date')),
						company=company,
					)
					db.add(report)

		db.commit()
		db.close()
		print(f'Results stored to DB: {db_url}')


	def loading(self, xpath):
		#
		# check for page loading
		#

		# wait for loading
		time.sleep(0.5)
		
		attempts = 0
		item = None
		while not item and attempts < 10:
			try:
				item = self.driver.find_element_by_xpath(xpath)
			except Exception:
				time.sleep(0.5)
				attempts += 1
		
		if not item:
			raise Exception('Unable to get the source')

		return item


	def get_list(self):
		# get url
		self.driver.get(self.url)
		time.sleep(0.5)
		# set flag
		self.driver.find_element_by_xpath("//input[@id='ctl00_MainBody_ucSearch_Response713']").click()
		# get serch results
		self.driver.find_element_by_xpath("//input[@id='ctl00_MainBody_ucSearch_btnSearchFirmName']").click()
		# get current page
		print(f'Page: {self.page}')
		if self.page > 1:
			page = self.loading("//input[@id='ctl00_MainBody_ucSearch_FirmResultsPager_PageInput']")
			page.send_keys(f'{self.page}')
			self.driver.find_element_by_xpath("//input[@id='ctl00_MainBody_ucSearch_FirmResultsPager_SelectNewPage']").click()


	def get_companies(self):
		table = self.loading("//table[@id='ctl00_MainBody_ucSearch_gvFirmResults']")
		return table.find_elements_by_xpath(".//a")


	def get_details(self):
		title_span = self.loading("//span[@id='ctl00_MainBody_FirmHeader1_lbFirmTitle']")
		title = title_span.get_attribute('innerHTML').strip()
		print(f'Title:\t{title}')

		#get registration status
		try:
			reg_status_span = self.driver.find_element_by_xpath("//span[@id='ctl00_MainBody_FirmHeader1_lblStatus']")
			reg_status = reg_status_span.get_attribute('innerHTML').strip()
		except Exception:
			reg_status = None
		print(f'Registration status:\t{reg_status}')

		# get registration date
		try:
			reg_date_span = self.driver.find_element_by_xpath("//span[@id='ctl00_MainBody_FirmHeader1_lblRegistrationDate']")
			reg_date = reg_date_span.get_attribute('innerHTML').strip()
		except Exception:
			reg_date = None		
		print(f'Registration Date:\t{reg_date}')

		# get withdrawal
		try:
			wd_date_span = self.driver.find_element_by_xpath("//span[@id='ctl00_MainBody_FirmHeader1_lblWithdrawalDate']")
			wd_date = wd_date_span.get_attribute('innerHTML').strip()
		except Exception:
			wd_date = None
		print(f'Withdrawal Date:\t{wd_date}')

		# get annual reports
		reports = self.driver.find_elements_by_xpath("//table[@id='ctl00_MainBody_PublicForms_gvForms']//tr")
		report_records = []
		if len(reports) > 1:
			print(f'Annual Reports:')
			for report in reports[1:]:
				data = report.find_elements_by_xpath(".//td")
				form = data[0].find_element_by_xpath(".//a").get_attribute('innerHTML').strip()
				if 'form 2' in form.lower():
					filed_date = data[1].find_element_by_xpath(".//span").get_attribute('innerHTML').strip()
					print(f'{filed_date}\t{form}')
					report_records.append({
						'Form': form,
						'Filed Date': filed_date,
					})

		self.results.append({
			'Company Title': title,
			'Registration Status': reg_status,
			'Registration Date': reg_date,
			'Withdrawal Date': wd_date,
			'Annual Reports': report_records or None,
		})


	def parse(self):
		try:
			while self.page <= 4:
				companies = None
				while not companies or self.index < len(companies):
					print(f'\nParsing company {self.items_per_page*(self.page-1) + self.index + 1}')
					self.get_list()
					companies = self.get_companies()
					companies[self.index].click()
					self.get_details()
					self.index += 1
				
				self.page += 1
				self.index = 0
		except Exception as e:
			print(f'Exception raised: {e}')

		# quit the browser
		self.driver.quit()



