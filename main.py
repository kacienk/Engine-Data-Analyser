from Imports.cleaner import Cleaner
from Imports.analyser import Analyser
from typing import Dict
import time
import pandas as pd


path_to_folder = './'
headers = {
                "first_engine" : 10,
                "second_engine" : 11,
                "heat_meter": 8,
                "gas_usage" : 1
              }


def create_output_df(analysis_results: dict, months: list) -> pd.DataFrame:
    output_df = pd.DataFrame(analysis_results)

    indexes = pd.Index(months)
    output_df.set_index(indexes, inplace=True)

    return output_df


def cleaning(column_names: Dict[int, str] = {}) -> int:
    path_to_data = f'{path_to_folder}dane_zbiorcze.xlsx'
    months = ['styczeń', 'luty', 'marzec', 'kwiecień', 'maj', 'czerwiec', 'lipiec', 'sierpień', 'wrzesień', 'październik', 'listopad', 'grudzień']

    data_cleaner = Cleaner(path_to_data, 2021, months)

    start = time.perf_counter()
    num_of_columns = data_cleaner._clean_data(new_column_names=column_names)
    end = time.perf_counter()
    duration = end - start

    print(f'Cleaning took {duration:.2f} s')
    #data_cleaner.print_errors()
    #data_cleaner.errors_to_file()

    return num_of_columns


def analysis(num_of_columns: int) -> pd.DataFrame:
    min_power_chp = 1990
    WD = 9.8
    months = ['sty', 'lut', 'mar', 'kwi', 'maj', 'cze', 'lip', 'sie', 'wrz', 'paz', 'lis', 'gru']
    analysis_results = {
                        'WD [kWh/m3]': [],
                        'Total gas usage [Nm3]': [],
                        "Power fuel [GJ]": [],
                        "Total electricity production [GJ]": [],
                        "Total heat production [GJ]": [],
                        "Electric efficency [%]": [],
                        "Heat efficency [%]": [],
                        "Total efficency [%]": []     
                   }

    for month in months:
        data_analyser = Analyser(WD, min_power_chp, headers, month)

        data_analyser.df = Analyser.load_df(month, path_to_folder, num_of_columns)

        month_analysis_result = data_analyser.analyse()

        for key in data_analyser.analyse():
            analysis_results[key].append(month_analysis_result[key])

        print(data_analyser)
        print()

    output_df = create_output_df(analysis_results, months)

    return output_df

        
def main():
    start = time.perf_counter()

    new_columns = {v: k for k, v in headers.items()}
    num_of_columns = cleaning(new_columns)
    num_of_columns = 42

    end = time.perf_counter()
    print(end - start)

    analisis_results_df = analysis(num_of_columns)

    analisis_results_df.to_excel('analysis_results.xlsx')


if __name__ == '__main__':
    cleaning()
