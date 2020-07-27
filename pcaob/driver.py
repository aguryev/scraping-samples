from pcaob import PCAOB

path_to_results = 'results.json'
db_url = 'sqlite:///pcaob.db'

# dump db to json
#PCAOB.db_to_json(db_url, path_to_results)
#exit(0)

page = 3
index = 17

while page < 5:
	p = PCAOB(page=page, index=index)
	p.parse()
	page, index = p.stop_item()
	p.to_json(path_to_results)
	p.to_db(db_url)
	print(f'\n************ NEXT ATTEMPT: page={page}, index={index}')