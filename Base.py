import sys
import os
import requests
import time
import argparse
import json
from datetime import datetime, timedelta
import gzip
import shutil
import csv



START_DATE = "1900-01-01"

class Base():

    
	#путь до родительской папки
	def get_parent_path(self):
		"""
		
		"""
		path = os.path.dirname(self.path)
		path = os.path.split(path)
		return path[0]
	
	#чтение данных из конфига
	def read_config(self, path):
		with open(path, 'r', encoding='utf-8') as f:
			return json.load(f)

	def __init__(self, path):
		'''
		Инициация загрузчика.
		Определяет параметры, передаваемые через  get_args:
		период - date_from, date_to,
		конфигурационный файл - config (по умолчанию - conf/config.json),
		имя файла - file_name

		Если в конфигурационном файле указан сдвиг в днях daydiff, а период не передавался, то
		date_from, date_to определяются как текущий день минус daydiff

		"""
		# пусть вызываемого файла. Того, который наследует этот класс '''
		self.path = path

		self.date_from = datetime.now().date() - timedelta(days=1)
		self.date_to = self.date_from
		self.config = self.read_config(os.path.join(self.get_parent_path(),
                                                           "conf",
                                                           "config.json"))
		self.file_name = self.config["file_name"]
		self.success_codes = self.config["success_codes"]
		print("Инициация загрузчика завешена.")

	def exp_delay(self, retry_counter, start_delay=20):
		"""
		Функция вычисления экспоненциальной задержки
		:return: int
		"""
		if retry_counter == 0:
			sleep_time_source = 0
		else:
			sleep_time_source = int(start_delay * 3 ** retry_counter)
		time.sleep(sleep_time_source)

	
	#Запись в файл
	def write_csv(self, data):
		date = datetime.now()
		date = datetime.strftime(date, "%Y%m%d")
		columns_names = self.config["csv_headers"]
		self.path_csv = self.get_parent_path()+"/"+"DATA/"+self.file_name+date+".csv"
		with open (self.path_csv, 'w', newline='') as f:
			wr = csv.writer(f)
			wr.writerow(self.config["csv_headers"])
			wr.writerows(data)
		print("Запись завершена в ", self.path_csv)
	
	def to_gzip(self, path):
		with open (path, 'rb') as f_in:
			with gzip.open(path+".gz", "wb") as f_out:
				shutil.copyfileobj(f_in, f_out)

	#формирование и отправка get запроса
	def load_request(self, params):
		if "n_tries" in self.config:
			n_tries = self.config["n_tries"]
		else:
			n_tries = 1

		if 'request_params' not in params:
			raise Exception("params must contain the 'request_params' element")
		

		retry_counter = 0

		while retry_counter < n_tries:
			self.exp_delay(retry_counter)
			retry_counter += 1

			try:
				response = requests.get(url = params["url"], headers = params["headers"], params = params["request_params"]) # Загрузка данных из API
				print(response.url)
				if response.status_code in self.success_codes:
					u'Запрос завершился успешно, инициируем поток загрузки. Код: {code}'.format(code=response.status_code)
					data = response.json()
					return data
				else:
					print(u'Ошибка в ответе сервера, код: %i, текст: %s' % (
						response.status_code, response.text))
			except Exception as e:
				u'Cервер не ответил в заданное время: {} секунд'.format(self.timeout)
				print(response.url)
		
		raise Exception(
			u'После {} запросов источник {} не вернул никаких данных'.format(n_tries, params['url']))

		

		

	