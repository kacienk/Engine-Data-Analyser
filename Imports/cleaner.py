from copy import deepcopy
from itertools import repeat
from typing import Any, List, Dict, Optional
import json
import time
from multiprocessing import Pool
import pandas as pd


class Cleaner:
    """
        A class to clean data collected from the scada(?).

        Atributes
        ---------
        path : str
            path to the spreadsheet with data
        year : int
            the year in which data was collected
            IMPORTANT: if data was collected over two years pass the latest year
        month : List[str]
            list of months in the same format as they are in the spreadsheet for example:
            ['styczeń', 'luty', 'marzec', ... , 'listopad', 'grudzień']
            months should be in the same order as they are in the spreadsheet
            if the first month is 'wrzesień' then list shoud look like this
            ['wrzesień', 'październik', 'listopad', 'grudzień', 'styczeń', ... , 'sierpień']
        months_converted : List[str]
            month list converted to formt
            ['sty', 'lut', ... , 'gru']
            using month.json file
        _new_year : int
            index of 'sty' month in list months
        errors : List[List[List[int]]]
            UNDER DEVELOPMENT
            indicies of the cells in which errors occured
            first dimension defines month
            second dimension defines column
            third dimension contains indicies of the cells

        Methods
        -------
        __init__(path: str, year: int, months: List[str]):
            initialize a new Cleaner for the specified path, year, and months list
        _convert_months(months: List[str]) -> List[str]:
            returns converts names of months in the given list of months
        _clean_sheet(self, month: str, path_to_output_folder: str) -> int:
            cleans single sheet applying is_number() map 
            and returns number of columns in that sheet
        clean_data(self, new_column_names: Dict[int, str]={}, path_to_output_folder: str='', *, processes: int=6) -> Optional[int]:
            cleans data and saves clean spreadsheets in the specified folder 
            implements multiprocessing (default: creates 6 processes)
            (default: folder where is the code executed)
            checks if number of columns cleaned is the same in every sheet cleaned
            if it is returns number of columns (useful for Analyser)
            else returns None
        print_errors():
            prints errors list 
        errors_to_files(path = ''):
            saves errors list in errors.txt file at given path        
    """ 
    path: str
    year: int
    months: List[str]
    months_converted: Dict[str, str]
    _new_year: int
    errors: List[List[List[int]]]


    def __init__(self, path: str, year: int, months: List[str]):
        self.months = months
        self.months_converted = Cleaner._convert_months(months)
        self.year = year
        self._new_year = self.months.index(self.return_key_of_value(self.months_converted, 'sty'))
        self.path = path
        self.errors = []

        for i in range(12):
            self.errors.append([])


    @classmethod
    def _convert_months(cls, months: List[str]) -> Dict[str, str]:
        result_months: Dict[str, str] = {}

        with open("./Imports/months.json") as json_file:
            data = json.load(json_file)
            for month in months:
                result_months[month] = data[month]
        
        return result_months


    @staticmethod
    def return_key_of_value(dict_: dict, value: Any):
        if value in dict_.values():
            reverse_dict = {dvalue: key for key, dvalue in dict_.items()}
            return reverse_dict[value]
        
        return None

    @staticmethod
    def is_number(x: Any) -> bool:
        try:
            float(x)
            return True
        except:
            return False


    def _clean_sheet(self, month: str, path_to_output_folder: str) -> int:
        start = time.perf_counter()
        if self.months.index(month) < self._new_year:
            df = pd.read_excel(self.path, sheet_name=f'{month} {self.year - 1}', header=0)
        else:
            df = pd.read_excel(self.path, sheet_name=f'{month} {self.year}', header=0)

        time_column = deepcopy(df['Czas'])
        df.drop('Czas')

        df = df[df.applymap(self.is_number)]

        df['Czas'] = time_column

        df.to_excel(f'{path_to_output_folder}Sheets/Clean/clean_{self.months_converted[month]}.xlsx')

        end = time.perf_counter()
        print(f'{month} cleaned in {end - start:.2f} s')
        
        return len(df.columns)


    def clean_data(self, new_column_names: Dict[int, str]={}, path_to_output_folder: str='', *, processes: int=6) -> Optional[int]:
        with Pool(processes) as pool:
            column_numbers = pool.starmap(self._clean_sheet, zip(self.months, repeat(path_to_output_folder)))
        
        if column_numbers and all([val == column_numbers[0] for val in column_numbers]):
                return column_numbers[0]

        return None


    def print_errors(self) -> None:
        for i in range(12):
            print(self.months_converted[i])

            for i, val_i in enumerate(self.errors[i]):
                print(f"{i}: ", end="")        
                for val_j in val_i:
                    print(f"{val_j}, ", end="")
                
            print()


    def errors_to_file(self, path = '') -> None:
        if path == '':
            path = 'errors.txt'
        
        with open(path, 'w') as file:
            pass

        with open(path, 'a') as file:
            for i in range(12):
                file.write(f"{self.months_converted[i]}\n")
                for i, val_i in enumerate(self.errors[i]):
                    file.write(f"{i}: ")

                    for j, val_j in enumerate(val_i):
                        file.write(f"{val_j}, ")
                    
                    file.write('\n')

    