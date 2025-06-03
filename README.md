# NYS Archive Parser

This repository contains a set of Python scripts for processing New York State inmate records: parse_raw_formatter.py converts fixed‐width text files into interim CSVs, parse_decoded.py applies JSON‐ and codebook‐based mappings to produce fully decoded CSVs, and build_histpun.py, build_inst_court.py, and build_inst_county.py generate aggregated Histpun‐style outputs (general tallies, institution×court breakdowns, and institution×county breakdowns, respectively). All decoded files live under csv_decoded/ and the JSON files (e.g., institution_map.json, county_map.json, se x_map.json, etc.) provide human‐readable labels for coded fields. Finally, run_all_parsers.py ties everything together by running each step in sequence so that, with one command, you can produce histpun_output.csv, histpun_inst_court.csv, and histpun_inst_county.csv from raw data.
```bash
python run_all_parsers.py
