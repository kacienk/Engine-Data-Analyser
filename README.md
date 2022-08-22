# Engine Data Analyser
The `Engine Data Analyser` project is created to automate the processes of cleaning and analysing data acquiered from SCADA monitorizing cogeneration plants. It is written in Python using Numpy and Pandas libraries. One of the biggest challenges faced during the development process was the variety of data representation. In the future the goal is to create a way to consolidate data into bigger periods of time (hours instead of minutes) and develop a system that is going to rename columns.

## Getting started
To run `Engine Data Analyser` you will need:
 1. Python3 
 2. Numpy
 3. Pandas

After downloading import `Analyser.py` and `Cleaner.py` from Imports folder.

Data file to clean and anylyse must be a Microsoft Excel file and should look like:
| Time | 1 | 2 | 3 | ... |
|------|---|---|---|-----|

Where `Time` column contains timestamps and the rest of the columns contain collected data. You should specify what data given column contains when constructing `Analyser` instance.

There is an example of `Engine Data Analyser` usage presented in `main.py` file.

## License
This project is licensed under MIT license. See the [LICENSE](https://github.com/kacienk/Engine-Data-Analiser/blob/main/LICENSE) file for details. 
