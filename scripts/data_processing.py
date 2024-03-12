
import json
import csv

class FileTypeNotExists(Exception):
    pass

class Data:

    def __init__(self, data):
        self.data = data
        self.columns = self._get_columns()
        self.data_size = self._size_data()
    
    def __load_json(path):
        with open(path, 'r') as file:
            json_data = json.load(file)
        return json_data

    def __load_csv(path):
        csv_data = []
        with open(path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                csv_data.append(dict(row))

        return csv_data

    @classmethod
    def load_data(cls, path, type):
        if type == 'csv':
            data = cls.__load_csv(path)
        elif type == 'json':
            data = cls.__load_json(path)
        else:
            raise FileTypeNotExists("The file type of data doesn't exists in code.")
        return cls(data)
    
    def _get_columns(self):
        return list(self.data[-1].keys())

    def rename_columns(self, key_mapping):
        new_data = []

        for old_dict in self.data:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_data.append(dict_temp)
        
        self.data = new_data
        self.columns = self._get_columns()
    
    def _size_data(self):
        return len(self.data)

    def join_data(list_of_data):
        combined_list = []
        for data in list_of_data:
            combined_list.extend(data.data)
        return Data(combined_list)

    def _transform_data_to_table(self):
        combined_data_table = [self.columns]
        for row in self.data:
            single_row = []
            [single_row.append(row.get(column, 'Indisponivel')) for column in self.columns]
            combined_data_table.append(single_row)
        
        return combined_data_table

    def save_data_to_csv(self, path):
        data_transformed = self._transform_data_to_table()
        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(data_transformed)
        return None