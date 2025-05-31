#!/usr/bin/env python3
import pandas as pd
import glob
import os
import numpy as np
import json
import re
import sys

# -----------------------------------------------------------------------------
# LOAD JSON MAPPING FILES (all eight)
# -----------------------------------------------------------------------------
try:
    with open("institution_map.json") as f:
        institution_map = json.load(f)
    with open("county_map.json") as f:
        county_map = json.load(f)
    with open("crime_map.json") as f:
        crime_map = json.load(f)
    with open("country_map.json") as f:
        country_map = json.load(f)
    with open("psych_map.json") as f:
        psych_map = json.load(f)
    with open("religion_map.json") as f:
        religion_map = json.load(f)
    with open("sex_map.json") as f:
        sex_map = json.load(f)
    with open("return_type_map.json") as f:
        return_type_map = json.load(f)
except FileNotFoundError as e:
    print(f"Error: Missing JSON mapping file: {e.filename}")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"Error: Invalid JSON in {e.msg}")
    sys.exit(1)


def clean_unknown(series: pd.Series) -> pd.Series:
    """
    Convert any literal '&', '9', or empty string to NaN,
    so that .map(...).fillna("Unknown") works cleanly.
    """
    return series.replace({"&": np.nan, "9": np.nan, "": np.nan})


def parse_iso_date(raw: str, fmt: str = "MDDYY") -> str:
    """
    Given a fixed-width date string (e.g. '61252' or '121252' for MDDYY),
    return a “yy-MM-DD” or “yy-MM” stub. Full pivot to YYYY is done later.

    Returns "" if raw is invalid or missing.
    """
    if not isinstance(raw, str):
        return ""
    s = re.sub(r"\D", "", raw)

    if fmt == "MDDYY":
        if len(s) == 5:
            mm, dd, yy = s[0], s[1:3], s[3:5]
        elif len(s) == 6:
            mm, dd, yy = s[0:2], s[2:4], s[4:6]
        else:
            return ""
        yy_i = int(yy)
        return f"{yy_i:02d}-{int(mm):02d}-{int(dd):02d}"

    elif fmt == "MYY":
        if len(s) == 3:
            mm, yy = s[0], s[1:3]
        elif len(s) == 4:
            mm, yy = s[0:2], s[2:4]
        else:
            return ""
        yy_i = int(yy)
        return f"{yy_i:02d}-{int(mm):02d}"

    return ""


def pivot_date_received(ysm: str) -> str:
    """
    Convert a “yy-MM-DD” stub into “YYYY-MM-DD”, assuming all receipts are 1900s.
    If ysm is empty or malformed, return "".
    """
    if not ysm or len(ysm) != 8:
        return ""
    yy, mm, dd = ysm.split("-")
    yy_i = int(yy)
    year_full = 1900 + yy_i
    return f"{year_full:04d}-{mm}-{dd}"


def pivot_dob(ysm: str, recv: str) -> str:
    """
    Convert a “yy-MM-DD” stub into “YYYY-MM-DD” by comparing its yy to the
    last two digits of the receipt-year (from recv). If yy > recv_yy → 18yy else 19yy.
    """
    if not isinstance(ysm, str) or len(ysm) != 8:
        return ""
    yy, mm, dd = ysm.split("-")
    yy_i = int(yy)

    if not recv or len(recv) < 4:
        return ""
    recv_year = int(recv[:4])  # e.g. “1952”
    recv_yy = recv_year % 100   # e.g. 52

    if yy_i > recv_yy:
        year_full = 1800 + yy_i
    else:
        year_full = 1900 + yy_i

    return f"{year_full:04d}-{mm}-{dd}"


def pivot_month_year(ym: str) -> str:
    """
    Convert a “yy-MM” stub into “YYYY-MM”, assuming all are 1900s.
    Return "" on failure.
    """
    if not ym or len(ym) != 5:
        return ""
    yy, mm = ym.split("-")
    yy_i = int(yy)
    return f"{1900 + yy_i:04d}-{mm}"


# -----------------------------------------------------------------------------
# Create output folder if it doesn’t exist
# -----------------------------------------------------------------------------
output_folder = "csv_decoded"
os.makedirs(output_folder, exist_ok=True)

# -----------------------------------------------------------------------------
# Loop over every raw-formatted CSV
# -----------------------------------------------------------------------------
for path in glob.glob("csv_raw_formatted/*_rawformatted.csv"):
    # Read all columns as strings
    df = pd.read_csv(path, dtype=str)

    # 1) DECODE ReceivingInstitutionCode → Institution
    df["Institution"] = (
        clean_unknown(df["ReceivingInstitutionCode"])
        .map(institution_map)
        .fillna("Unknown")
    )

    # 2) DECODE CountyCommittedFrom → County
    df["County"] = (
        clean_unknown(df["CountyCommittedFrom"])
        .map(county_map)
        .fillna("Unknown")
    )

    # 3) DECODE CrimeDetails → Crime
    raw_crime = df["CrimeDetails"].fillna("")
    code = raw_crime.str.slice(0, 2).fillna("")
    degree = raw_crime.str.slice(2, 3).fillna("")

    def decode_crime_pair(c: str, d: str) -> str:
        # Any presence of “&” or blank means “Unknown”
        if not c or not d or "&" in c or "&" in d:
            return "Unknown"
        base = crime_map.get(c)
        if base is None:
            return "Unknown"
        # Degree mapping: “0”→3rd, “1”→2nd, “2”→1st, “3”→1st
        deg_map = {"0": "3rd", "1": "2nd", "2": "1st", "3": "1st"}
        deg_label = deg_map.get(d)
        if deg_label is None:
            return base
        return f"{base}, degree {deg_label}"

    df["Crime"] = [decode_crime_pair(c, d) for c, d in zip(code, degree)]

    # 4) PARSE & PIVOT DATES
    #  a) DateReceived → parse to “yy-MM-DD” then pivot to “YYYY-MM-DD”
    df["_RAW_DateReceived"] = df["DateReceived"].apply(lambda x: parse_iso_date(x, "MDDYY"))
    df["DateReceived"] = df["_RAW_DateReceived"].apply(pivot_date_received)

    #  b) DateOfBirth → parse to “yy-MM-DD” then pivot with DateReceived
    df["_RAW_DateOfBirth"] = df["DateOfBirth"].apply(lambda x: parse_iso_date(x, "MDDYY"))
    df["DateOfBirth"] = [
        pivot_dob(dob_raw, rec)
        for dob_raw, rec in zip(df["_RAW_DateOfBirth"], df["DateReceived"])
    ]

    #  c) LatestReleaseDate → parse “MYY” to “yy-MM” then pivot to “YYYY-MM”
    df["_RAW_LatestRelease"] = df["LatestReleaseDate"].apply(lambda x: parse_iso_date(x, "MYY"))
    df["LatestReleaseDate"] = df["_RAW_LatestRelease"].apply(pivot_month_year)

    #  d) LatestReturnDate → parse “MDDYY” to “yy-MM-DD” then pivot to “YYYY-MM-DD”
    df["_RAW_LatestReturn"] = df["LatestReturnDate"].apply(lambda x: parse_iso_date(x, "MDDYY"))
    df["LatestReturnDate"] = df["_RAW_LatestReturn"].apply(pivot_date_received)

    # Drop temporary raw-date columns
    df.drop(columns=["_RAW_DateReceived", "_RAW_DateOfBirth", "_RAW_LatestRelease", "_RAW_LatestReturn"], inplace=True)

    # 5) DECODE CourtCommittedBy → CourtCommittedByName (if needed)
    # If you have a separate JSON for court codes, load & map it similarly. Otherwise skip.
    # Example inline mapping (fill in actual values if required):
    court_map = {
        "1": "Albany County Court",
        "2": "Erie County Court",
        "3": "Onondaga County Court",
        "4": "Westchester County Court",
        "5": "New York County Court",
        "6": "Queens County Court",
        "7": "Kings County Court",
        "8": "Bronx County Court",
        "9": "Suffolk County Court",
        "0": "Unknown"
    }
    # Instead of a fixed "Erie County Court", do:
    df['CourtCommittedByName'] = df['CountyCommittedFrom'].map(lambda c: f"{county_map.get(c, 'Unknown')} County Court")


    # 6) DECODE Race → RaceName
    # Example mapping (fill from your codebook if different):
    race_map = {
        "W": "White",
        "B": "Black",
        "H": "Hispanic",
        "A": "Asian/Pacific Islander",
        "I": "Native American",
        "O": "Other"
    }
    df["RaceName"] = (
        clean_unknown(df["Race"])
        .map(race_map)
        .fillna("Unknown")
    )

    # 7) AgeAtCommitment remains as a string (or convert to int if desired)
    df["AgeAtCommitment"] = df["AgeAtCommitment"].apply(lambda x: x if pd.notna(x) else "")

    # 8) DECODE Religion → ReligionName
    df["ReligionName"] = (
        clean_unknown(df["Religion"])
        .map(religion_map)
        .fillna("Unknown")
    )

    # 9) DECODE Sex → SexName
    df["SexName"] = (
        clean_unknown(df["Sex"])
        .map(sex_map)
        .fillna("Unknown")
    )

    # 10) IdentifierNumber and CheckDigit can remain raw strings
    df["IdentifierNumber"] = df["IdentifierNumber"].fillna("")
    df["CheckDigit"] = df["CheckDigit"].fillna("")

    # 11) DECODE YearsResidenceNY (numeric) → leave as-is or convert
    df["YearsResidenceNY"] = df["YearsResidenceNY"].apply(lambda x: x if pd.notna(x) else "")

    # 12) DECODE MilitaryService → “Yes”/“No”
    df["MilitaryServiceYN"] = (
        df["MilitaryService"]
        .fillna("")
        .map({"Y": "Yes", "N": "No"})
        .fillna("Unknown")
    )

    # 13) DECODE Education → EducationLevel (if you have a JSON, load & map)
    # For now, leave raw or create an edu_map.json and replace:
    df["EducationLevel"] = df["Education"].apply(lambda x: x if pd.notna(x) else "")

    # 14) Occupation (raw code) → OccupationCode
    df["OccupationCode"] = df["Occupation"].apply(lambda x: x if pd.notna(x) else "")

    # 15) NarcoticsUse → “Yes”/“No”
    df["NarcoticsUseYN"] = (
        df["NarcoticsUse"]
        .fillna("")
        .map({"Y": "Yes", "N": "No"})
        .fillna("Unknown")
    )

    # 16) MaritalStatus → MaritalStatusCode (or map if you have a JSON)
    df["MaritalStatusCode"] = df["MaritalStatus"].apply(lambda x: x if pd.notna(x) else "")

    # 17) PrevCriminalRecord → “Yes”/“No”
    df["PrevCriminalRecordYN"] = (
        df["PrevCriminalRecord"]
        .fillna("")
        .map({"Y": "Yes", "N": "No"})
        .fillna("Unknown")
    )

    # 18) CommitmentsProbation (numeric)
    df["CommitmentsProbationNum"] = df["CommitmentsProbation"].apply(lambda x: x if pd.notna(x) else "")

    # 19) FinesSuspensions (numeric)
    df["FinesSuspensionsNum"] = df["FinesSuspensions"].apply(lambda x: x if pd.notna(x) else "")

    # 20) TimeSpanEarliestAdultRecord (numeric)
    df["TimeSpanEarliestAdultRecordNum"] = df["TimeSpanEarliestAdultRecord"].apply(lambda x: x if pd.notna(x) else "")

    # 21) MinorPoliceContacts (numeric)
    df["MinorPoliceContactsNum"] = df["MinorPoliceContacts"].apply(lambda x: x if pd.notna(x) else "")

    # 22) SeriousPoliceContacts (numeric)
    df["SeriousPoliceContactsNum"] = df["SeriousPoliceContacts"].apply(lambda x: x if pd.notna(x) else "")

    # 23) DECODE CountryOfBirth → CountryOfBirthName
    df["CountryOfBirthName"] = (
        clean_unknown(df["CountryOfBirth"])
        .map(country_map)
        .fillna("Unknown")
    )

    # 24) YearEnteredUS (numeric)
    df["YearEnteredUSNum"] = df["YearEnteredUS"].apply(lambda x: x if pd.notna(x) else "")

    # 25) NaturalizationStatus → “Naturalized”/“Not naturalized”
    df["NaturalizationStatusLabel"] = (
        clean_unknown(df["NaturalizationStatus"])
        .map({"Y": "Naturalized", "N": "Not naturalized"})
        .fillna("Unknown")
    )

    # 26) DECODE PsychiatricClassification → PsychiatricClassificationLabel
    df["PsychiatricClassificationLabel"] = (
        clean_unknown(df["PsychiatricClassification"])
        .map(psych_map)
        .fillna("Unknown")
    )

    # 27) DECODE InstitutionOriginal → InstitutionOriginalName
    df["InstitutionOriginalName"] = (
        clean_unknown(df["InstitutionOriginal"])
        .map(institution_map)
        .fillna("Unknown")
    )

    # 28) OriginalMonthYear (MYY) → parse & pivot to “YYYY-MM”
    df["_RAW_OMY"] = df["OriginalMonthYear"].apply(lambda x: parse_iso_date(x, "MYY"))
    df["OriginalMonthYear"] = df["_RAW_OMY"].apply(pivot_month_year)
    df.drop(columns=["_RAW_OMY"], inplace=True)

    # 29) MentalHygieneID (numeric string)
    df["MentalHygieneIDNum"] = df["MentalHygieneID"].apply(lambda x: x if pd.notna(x) else "")

    # 30) DECODE ReturnType → ReturnTypeLabel
    df["ReturnTypeLabel"] = (
        clean_unknown(df["ReturnType"])
        .map(return_type_map)
        .fillna("Unknown")
    )

    # 31) DECODE CurrentInstitution → CurrentInstitutionName
    df["CurrentInstitutionName"] = (
        clean_unknown(df["CurrentInstitution"])
        .map(institution_map)
        .fillna("Unknown")
    )

    # -----------------------------------------------------------------------------
    # FINAL: Assemble a DataFrame containing ONLY the decoded columns, in logical order
    # -----------------------------------------------------------------------------
    decoded = df[
        [
            "Institution",                  # from ReceivingInstitutionCode
            "County",                       # from CountyCommittedFrom
            "CourtCommittedByName",         # from CourtCommittedBy
            "Crime",                        # from CrimeDetails
            "DateOfBirth",                  # pivoted to YYYY-MM-DD
            "DateReceived",                 # pivoted to YYYY-MM-DD
            "MinSentence",                  # raw string (e.g. “000” = 0 months)
            "MaxSentence",                  # raw
            "AgeAtCommitment",              # as string
            "RaceName",                     # decoded
            "ReligionName",                 # decoded
            "SexName",                      # decoded
            "IdentifierNumber",             # raw ID
            "CheckDigit",                   # raw
            "YearsResidenceNY",             # as string
            "MilitaryServiceYN",            # “Yes”/“No”
            "EducationLevel",               # raw (or map with edu_map.json)
            "OccupationCode",               # raw
            "NarcoticsUseYN",               # “Yes”/“No”
            "MaritalStatusCode",            # raw
            "PrevCriminalRecordYN",         # “Yes”/“No”
            "CommitmentsProbationNum",      # as string
            "FinesSuspensionsNum",          # as string
            "TimeSpanEarliestAdultRecordNum", # as string
            "MinorPoliceContactsNum",       # as string
            "SeriousPoliceContactsNum",     # as string
            "CountryOfBirthName",           # decoded
            "YearEnteredUSNum",             # as string
            "NaturalizationStatusLabel",    # decoded
            "PsychiatricClassificationLabel", # decoded
            "InstitutionOriginalName",      # decoded
            "OriginalMonthYear",            # pivoted to YYYY-MM
            "MentalHygieneIDNum",           # as string
            "ReturnTypeLabel",              # decoded
            "LatestReleaseDate",            # YYYY-MM
            "LatestReturnDate",             # YYYY-MM-DD
            "CurrentInstitutionName"        # decoded
        ]
    ]

    # Write out the final decoded CSV
    base = os.path.basename(path).replace("_rawformatted.csv", "")
    out_csv = os.path.join(output_folder, f"{base}_decoded.csv")
    decoded.to_csv(out_csv, index=False)
    print(f"Decoded saved: {out_csv}")

print("All files decoded.")
