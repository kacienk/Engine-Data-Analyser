from cmath import nan
from copy import deepcopy
from typing import List, Dict
import json
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
            indicies of the cells in which errors occured
            first dimension defines month
            second dimension defines column
            third dimension contains indicies of the cells
            UNDER DEVELOPMENT

        Methods
        -------
        __init__(path: str, year: int, months: List[str]):
            constructs a new Cleaner for the specified path, year, and months list
        _convert_months(months: List[str]) -> List[str]:
            returns converts names of months in the given list of months
        clean_data(path_to_output_folder = '') -> int:
            cleans data and saves clean spreadsheets in the specified folder 
            (default: folder where is the code in which Cleaner was constructed)
            returns number of columns (useful for Analyser)
        _clean_data(column_names:Dict[int, string], path_to_output_folder = '') -> int:
            as above. Prototype function also changing names of columns.
        print_errors():
            prints errors list 
        errors_to_files(path = ''):
            saves errors list in errors.txt file at given path        
    """ 
    path: str
    year: int
    months: List[str]
    months_converted: List[str]
    _new_year: int
    errors: List[List[List[int]]]


    def __init__(self, path: str, year: int, months: List[str]):
        self.months = months
        self.months_converted = Cleaner._convert_months(months)
        self.year = year
        self._new_year = self.months_converted.index('sty')
        self.path = path
        self.errors = []

        for i in range(12):
            self.errors.append([])


    @classmethod
    def _convert_months(cls, months: List[str]) -> List[str]:
        result_months = []

        with open("Imports/months.json") as json_file:
            data = json.load(json_file)
            for month in months:
                result_months.append(data[month])
        
        return result_months


    def _isnumber(self, x) -> bool:
        try:
            float(x)
            return True
        except:
            return False


    def clean_data(self,path_to_output_folder = '') -> int:
        for month_index, month in enumerate(self.months):
            if self.months.index(month) < self._new_year:
                df = pd.read_excel(self.path, sheet_name=f'{month} {self.year - 1}', header=0)
            else:
                df = pd.read_excel(self.path, sheet_name=f'{month} {self.year}', header=0)

            for _ in df.columns:
                self.errors[month_index].append([])

            for column in df.columns:
                if column == 'Czas':
                    continue

                for index in df.index:
                    try:
                        df.loc[index, column] = float(df.loc[index, column])
                    except:
                        df.loc[index, column] = nan
                        self.errors[month_index][column].append(index)

            df.to_excel(f'{path_to_output_folder}Sheets/Clean/clean_{self.months_converted[month_index]}.xlsx')
            
        return len(df.columns)


    def _clean_data(self, new_column_names:Dict[int, str]={}, path_to_output_folder='') -> int:
        for month_index, month in enumerate(self.months):
            if self.months.index(month) < self._new_year:
                df = pd.read_excel(self.path, sheet_name=f'{month} {self.year - 1}', header=0)
            else:
                df = pd.read_excel(self.path, sheet_name=f'{month} {self.year}', header=0)

            for _ in df.columns:
                self.errors[month_index].append([])

            time_column = deepcopy(df['Czas'])

            df = df[df.applymap(self._isnumber)]

            df['Czas'] = time_column

            df.rename(columns=new_column_names, inplace=True)

            df.to_excel(f'{path_to_output_folder}Sheets/Clean/cleannew_{self.months_converted[month_index]}.xlsx')

            print(f'{month} cleaned')
            
        return len(df.columns)


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

    