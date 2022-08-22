from cmath import isnan
from typing import List, Dict
import numpy as np
import pandas as pd
import os


class Analyser:
    """
        A class to analyse data cleaned by object of class Cleaner

        Atributes
        ---------
        month : str
            name of the month in format
            ['sty', 'lut', ... , 'gru']

        path_to_folder : str
            path to the project folder
            (default: foldere where is the code in which Analyser was constructed)

        df : pd.DataFrame
            DataFrame containing data from clean spreadsheet with additional columns

        headers : dict
            dictionary containing essential names of columns, for example
            {"first_engine" : 10, "second_engine" : 11, "heat_meter": 8, "gas_usage" : 1}

        min_power_chp : float
            minimal power needed to count the engine

        WD : float
            calorific value


        Methods
        -------
        __init__(WD: float, min_power_chp: float, headers: dict, month: str):
            constructs a new Analyser for the specified WD, min_power_chp, headers, and month
            to do proper anylysis 

        @classmethod
        load_df(month, path_to_folder = './', num_of_columns = None) -> pd.DataFrame:
            from specified path_to_folder loads excel representing specified month and returns DataFrame 

        _engines_working_corectly() -> np.ndarray:
            adds three columns to df DataFrame containing information whether respective condition is satisfied
            also returns np.ndarray containing information whether at least one of conditioin was satisfide in respective column

        _rows_without_nan() -> None:
            adds column containing information if an error did not occurd in the respective row in significant column

        _engines_off():
            adds column to df DataFrame containing information if engines are not working

        _gas_usage_Nm3():
            adds column to df DataFrame containing gas usage converted to Nm3

        _check_dependencies() -> None:
            checks whether required columns exist and creates them if they do not

        total_gas_usage() -> float:
            returns total gas usage in Nm3

        power_fuel() -> float:
            returns power fuel (?)

        total_electricity_prod() -> float:
            returns total electricity production in GJ

        total_heat_prod() -> float:
            returns total heat production in GJ

        electric_efficiency() -> float:
            returns electric efficiency

        heat_efficiency() -> float:
            returns heat efficiency

        total_efficiency() -> float:
            returns sum of electric efficiency and heat efficiency

        bad_gas_usage() -> List[str]:
            returns list of indexes of rows in which gas is used and engines are not working

        analyse():
            analyses data in df DataFrame and returns dictionary storing analysis result
        
         
    """
    month: str
    df: pd.DataFrame
    headers: Dict[str, int]
    min_power_chp: float
    WD: float


    def __init__(self, WD: float, min_power_chp: float, headers: Dict[str, int], month: str = ''):
        self.month = month
        self.WD = WD
        self.min_power_chp = min_power_chp
        self.headers = headers
        self.df = None


    def __str__(self) -> str:
        self._check_dependencies()

        analiser_str = f"Month: {self.month}\n"
        analiser_str += f"Gas usage: {self.total_gas_usage()} Nm3\n"
        analiser_str += f"WD: {self.WD} kWh/m3\n"
        analiser_str += f"Power fuel: {self.power_fuel()} GJ\n"
        analiser_str += f"Electricity production: {self.total_electricity_prod()} GJ\n"
        analiser_str += f"Heat production: {self.total_heat_prod()} GJ\n"
        analiser_str += f"Electric efficiency: {self.electric_efficiency()}%\n"
        analiser_str += f"Heat efficiency: {self.heat_efficiency()}%\n"
        analiser_str += f"Total efficiency: {self.total_efficiency()}%"

        return analiser_str

    
    @classmethod
    def load_df(cls, month, path_to_folder = './', num_of_columns = None) -> pd.DataFrame:
        if not os.path.isdir(path_to_folder):
            raise Exception("Invalid path")

        try:
            if num_of_columns is not None:
                dtype = {i: np.float128 for i in range(1, num_of_columns)}

                return pd.read_excel(f'{path_to_folder}Sheets/Clean/clean_{month}.xlsx', index_col=0, dtype=dtype)

            return pd.read_excel(f'{path_to_folder}Sheets/Clean/clean_{month}.xlsx', index_col=0)
        except Exception as e:
            print(e)
            

    def _engines_working_corectly(self) -> None:
        condition1 = np.logical_and(self.df[self.headers["first_engine"]] > self.min_power_chp, self.df[self.headers["second_engine"]] == 0)
        condition2 = np.logical_and(self.df[self.headers["second_engine"]] > self.min_power_chp, self.df[self.headers["first_engine"]] == 0)
        condition3 = np.logical_and(self.df[self.headers["first_engine"]] > self.min_power_chp, self.df[self.headers["second_engine"]] > self.min_power_chp)

        self.df['engine1_condition'] = condition1
        self.df['engine2_condition'] = condition2
        self.df['both_engines_condition'] = condition3

        return np.logical_or(np.logical_or(condition1, condition2), condition3)


    def _engines_off(self) -> None:
        self.df["engines_off"] = np.logical_and(self.df["first_engine"] == 0, self.df["second_engine"] == 0)


    def _rows_without_nan(self) -> None:
        for index in self.df.index:
            for key in self.headers:
                self.df.loc[index, 'row_without_nan'] = not isnan(self.df.loc[index, self.headers[key]])


    def _create_is_counted(self) -> None:
        self.df['is_counted'] = self._engines_working_corectly()
        self._rows_without_nan()
        self.df['is_counted'] = np.logical_and(self.df['is_counted'], self.df['row_without_nan'])


    def _gas_usage_Nm3(self) -> None:
        self.df['gas_usage_Nm3'] = self.df[self.headers["gas_usage"]] / 60


    def _check_dependencies(self) -> None:
        if self.df is None:
            raise Exception('No data to anylyse')

        if 'is_counted' not in self.df.columns:
            self._create_is_counted()

        if 'engines_off' not in self.df.columns:
            self._engines_off()

        if 'gas_usage_Nm3' not in self.df.columns:
            self._gas_usage_Nm3()


    def total_gas_usage(self) -> float:
        self._check_dependencies()

        filtered_df = self.df[self.df['is_counted']]

        return filtered_df['gas_usage_Nm3'].sum()


    def power_fuel(self) -> float:
        self._check_dependencies()

        return self.total_gas_usage() * self.WD * (3.6/1000)


    def total_electricity_prod(self) -> float:
        self._check_dependencies()
        conversion_ratio = 277.78 # 1 [GJ] = 277.78 [kWh]

        filtered_df = self.df[self.df['is_counted']]

        engine1_electricity_prod = filtered_df[self.headers["first_engine"]].sum() / 60
        engine2_electricity_prod = filtered_df[self.headers["second_engine"]].sum() / 60 
        total_electricity_prod = (engine1_electricity_prod + engine2_electricity_prod) / conversion_ratio

        return total_electricity_prod


    def total_heat_prod(self) -> float:
        self._check_dependencies()

        total_heat_production = 0

        for i in self.df.index[1:]:
            if self.df.loc[i, 'is_counted']:
                if not isnan(self.df.loc[i, self.headers['heat_meter']]) and not isnan(self.df.loc[i - 1, self.headers['heat_meter']]):
                    total_heat_production = total_heat_production + (self.df.loc[i, self.headers['heat_meter']] - self.df.loc[i - 1, self.headers['heat_meter']])

        return total_heat_production


    def electric_efficiency(self) -> float:
        self._check_dependencies()

        return (self.total_electricity_prod() / self.power_fuel()) * 100


    def heat_efficiency(self) -> float:
        self._check_dependencies()

        return (self.total_heat_prod() / self.power_fuel()) * 100

    
    def total_efficiency(self) -> float:
        self._check_dependencies()

        return self.heat_efficiency() + self.electric_efficiency()


    def bad_gas_usage(self) -> List[int]:
        self._check_dependencies()

        indexes = []

        for i in self.df.index:
            if self.df.loc[i, 'engines_off'] and self.df.loc[i, self.headers["gas_usage"]] != 0 and not isnan(self.df.loc[i, self.headers["gas_usage"]]):
                indexes.append(i)

        return indexes

    
    def analyse(self) -> Dict[str, int]:
        self._check_dependencies()

        analysis_result = {
                            'WD [kWh/m3]': self.WD,
                            'Total gas usage [Nm3]': self.total_gas_usage(),
                            "Power fuel [GJ]": self.power_fuel(),
                            "Total electricity production [GJ]": self.total_electricity_prod(),
                            "Total heat production [GJ]": self.total_heat_prod(),
                            "Electric efficency [%]": self.electric_efficiency(),
                            "Heat efficency [%]": self.heat_efficiency(),
                            "Total efficency [%]": self.total_efficiency()
                          }

        return analysis_result


    def _concatenate_to_one_row(self, start_point, end_point) -> pd.Series:
        result_row = self.df.loc[start_point]
        rows = self.df.loc[start_point : end_point]

        pass


    def concatenate_data(self, time_delta: np.timedelta64) -> pd.DataFrame:
        self._check_dependencies()

        new_df = pd.DataFrame(columns=self.df.columns)



        



