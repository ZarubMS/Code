import sys
import os
import json
from datetime import datetime, timedelta
import csv
import requests
from Base import Base





path_file = os.path.realpath(__file__)

class mostrans_appmetrica(Base):
	
	#получение параметров из конфига
	def get_params_loader(self, request_params, **kwargs):
		headers = self.config["headers"]
		url = self.config["url"]
		self.timeout = self.config["timeout"]
		request_params["date1"] = self.date_from
		request_params["date2"] = self.date_to

		for key in kwargs:
			if key != "add_url":
				request_params[key] = kwargs[key]
			elif key == "add_url":
				url += kwargs[key]
		print(url)
		main_params = dict(url = url, headers = headers, request_params = request_params)
		return main_params 
			
	#парсим JSON
	def read_json(self, load_data):
		print("Парсинг:")
		a = []
		a.append(load_data['query']['date1'])
		a.append(load_data['query']['date2'])
		a.append(load_data['time_intervals'][0][0])
		a.append(load_data['query']['ids'][0])
		a.append(load_data['query']['metrics'][0])
		a.append(load_data['totals'][0][0])
		print("Парсинг завершен")		
		return a
	
	def read_json2(self, load_data, count_metrics):
		print("Парсинг:")
		data = []
		for i in range(count_metrics):
			for j in range(24):
				a = []
				a.append(load_data['query']['date1'])
				a.append(load_data['query']['date2'])
				a.append(load_data['time_intervals'][j][0])
				a.append(load_data['query']['ids'][0])
				a.append(load_data['data'][i]['dimensions'][0]['name'])
				a.append(load_data['data'][i]['metrics'][0][j])
				data.append(a)
		print("Парсинг завершен")
		return data
	
	def request_to_list(self, params, **kwargs):
		all_params = self.get_params_loader(params, **kwargs)
		load_data = self.load_request(all_params)
		return load_data


	def run(self):
		df = []

		#запросы для каждого id и метрик "ym:s:avgSessionDuration", "ym:s:sessionsPerUser"
		for i in range(3):
			for j in range(2):

				load_data = self.request_to_list(self.config["first_request_params"]["const_params"],
									id = self.config["first_request_params"]["variable_params"]["ids"][i],
									metrics = self.config["first_request_params"]["variable_params"]["metrics"][j])
				data = self.read_json(load_data)
				df.append(data)
		#запросы для каждого id и всяких разных метрик
		for i in range(3):
			for j in range(2):
				row_ids_list = self.config["second_request_params"]["row_ids"][j]
				row_id1 = str(row_ids_list)
				if j == 0:
					row_id2 = row_id1[:1] + "[null]," + row_id1[1:]
				else:
					row_id2 = row_id1
				row_ids = row_id2.replace(" ", "").replace("\'", "\"")
				count_metrics = len(row_ids_list)
				load_data = self.request_to_list(self.config["second_request_params"]["const_params"],
										id = self.config["second_request_params"]["variable_params"]["ids"][i],
										row_ids = row_ids,
									headers = self.config["headers"])
				data = self.read_json2(load_data, count_metrics - 1)
				df = df + data
		
		self.write_csv(df)
		self.to_gzip(self.path_csv)

		

 

		

if __name__ == '__main__':
    Main = mostrans_appmetrica(path_file)
    Main.run()