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
		a.append(load_data['totals'][0])
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
	
	def request_toDF(self, params, **kwargs):
		all_params = self.get_params_loader(params, **kwargs)
		load_data = self.load_request(all_params)
		return load_data

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

	def run(self):
		df = []
		#запросы для каждого id и метрик "ym:s:avgSessionDuration", "ym:s:sessionsPerUser"
		for i in range(3):
			for j in range(2):

				load_data = self.request_toDF(self.config["first_request_params"]["const_params"],
									id = self.config["first_request_params"]["variable_params"]["ids"][i],
									metrics = self.config["first_request_params"]["variable_params"]["metrics"][j])
				data = self.read_json(load_data)
				df.append(data)
		#запросы для каждого id и первых 30 метрик
		for i in range(3):
			count_metrics = 29
			load_data = self.request_toDF(self.config["second_request_params"]["const_params"],
									id = self.config["second_request_params"]["variable_params"]["ids"][i],
									add_url = '?row_ids=[[null]%2C["_taxi"]%2C["_scooter"]%2C["_bike"]%2C["_bike_scooter"]%2C["_taxi_scooter"]%2C' +
				 '["_taxi_bike"]%2C["_taxi_bike_scooter"]%2C["carsharing"]%2C["carshering_"]%2C["carsharing_taxi"]%2C["carsharing_scooter"]%2C' +
				 '["carsharing_bike"]%2C["carsharing__taxi"]%2C["carsharing__scooter"]%2C["carsharing__bike"]%2C["carsharing_bike_scooter"]%2C'+
				 '["carsharing_taxi_bike"]%2C["carsharing_bike_scooter"]%2C["carsharing__taxi_scooter"]%2C["carsharing__taxi_bike"]%2C' +
				 '["carsharing__bike_scooter"]%2C["carsharing_taxi_bike_scooter"]%2C["carsharing__taxi_bike_scooter"]%2C["taxi"]%2C' +
				 '["taxi_"]%2C["taxi_scooter"]%2C["taxi_bike"]%2C["taxi__scooter"]%2C["taxi__bike"]]',
			  					headers = self.config["headers"])
			data = self.read_json2(load_data, count_metrics)
			df = df + data
		#запросы для каждого id и ещё 11 метрик
		for i in range(3):
			count_metrics = 11
			load_data = self.request_toDF(self.config["second_request_params"]["const_params"],
									id = self.config["second_request_params"]["variable_params"]["ids"][i],
									add_url = "?row_ids=%5B%5B%22taxi_carsharing_%22%5D%2C%5B%22taxi_carsharing_scooter%22%5D%2C%5B%22taxi_bike_scooter%22%5D%2C%"+
			   "5B%22taxi__bike_scooter%22%5D%2C%5B%22taxi_carsharing__scooter%22%5D%2C%5B%22taxi_carsharing__bike%22%5D%2C%"+
			   "5B%22taxi_carsharing_bike_scooter%22%5D%2C%5B%22taxi_carsharing__bike_scooter%22%5D%2C%5B%22scooter%22%5D%2C%"+
			   "5B%22bike%22%5D%2C%5B%22bike_scooter%22%5D%2C%5B%22notMultiModalRoute%22%5D%5D", headers=self.config["headers"])
			data = self.read_json2(load_data, count_metrics)
			df = df + data
		
		self.write_csv(df)
		self.to_gzip(self.path_csv)

 

		

if __name__ == '__main__':
    Main = mostrans_appmetrica(path_file)
    Main.run()
