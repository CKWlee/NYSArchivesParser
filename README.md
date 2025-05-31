# NYS Archive Parser

This repository contains a simple Python toolkit for converting New York State inmate records from fixed-width text files into clean, human-readable CSVs. It includes two main scripts—`parse_raw_formatter.py` (which reads the raw `.txt` files, preserves all codes as strings, and pivots dates into ISO format) and `parse_decoded.py` (which applies JSON‐based mappings for institutions, counties, crimes, and other fields to produce a fully decoded output). To get started, install the dependencies (`pandas`, `numpy`) in your virtual environment, drop your `.txt` files into the repo folder, and run:  
```bash
python run_all_parsers.py
