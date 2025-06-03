#!/usr/bin/env python3
import pandas as pd
import glob
import os
import numpy as np
import json
import re
import sys

# ---------------------------------------------------------------------------
# LOAD JSON MAPPING FILES (all eight)
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Create output folder if it doesn’t exist
# ---------------------------------------------------------------------------
output_folder = "csv_decoded"
os.makedirs(output_folder, exist_ok=True)

# ---------------------------------------------------------------------------
# Loop over every raw-formatted CSV
# ---------------------------------------------------------------------------
for path in glob.glob("csv_raw_formatted/*_rawformatted.csv"):
    # Read all columns as strings
    df = pd.read_csv(path, dtype=str)

    # 1) DECODE ReceivingInstitutionCode → Institution
    if "ReceivingInstitutionCode" in df.columns:
        df["Institution"] = (
            clean_unknown(df["ReceivingInstitutionCode"])
            .map(institution_map)
            .fillna("Unknown")
        )
    else:
        df["Institution"] = ""

    # 2) DECODE CountyCommittedFrom → County
    if "CountyCommittedFrom" in df.columns:
        df["County"] = (
            clean_unknown(df["CountyCommittedFrom"])
            .map(county_map)
            .fillna("Unknown")
        )
    else:
        df["County"] = ""

    # 3) DECODE CrimeDetails → Crime
    if "CrimeDetails" in df.columns:
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
    else:
        df["Crime"] = ""

    # 4) PARSE & PIVOT DATES
    #  a) DateReceived is already “YYYY-MM-DD” from parse_raw_formatter.py; skip re-parsing.

    #  b) DateOfBirth → parse to “yy-MM-DD” then pivot with DateReceived
    if "DateOfBirth" in df.columns and "DateReceived" in df.columns:
        df["_RAW_DateOfBirth"] = df["DateOfBirth"].apply(lambda x: parse_iso_date(x, "MDDYY"))
        df["DateOfBirth"] = [
            pivot_dob(dob_raw, rec)
            for dob_raw, rec in zip(df["_RAW_DateOfBirth"], df["DateReceived"])
        ]
        df.drop(columns=["_RAW_DateOfBirth"], inplace=True)
    else:
        df["DateOfBirth"] = ""

    #  c) LatestReleaseDate → parse “MYY” to “yy-MM” then pivot to “YYYY-MM”
    if "LatestReleaseDate" in df.columns:
        df["_RAW_LatestRelease"] = df["LatestReleaseDate"].apply(lambda x: parse_iso_date(x, "MYY"))
        df["LatestReleaseDate"] = df["_RAW_LatestRelease"].apply(pivot_month_year)
        df.drop(columns=["_RAW_LatestRelease"], inplace=True)
    else:
        df["LatestReleaseDate"] = ""

    #  d) LatestReturnDate → parse “MDDYY” to “yy-MM-DD” then pivot to “YYYY-MM-DD”
    if "LatestReturnDate" in df.columns:
        df["_RAW_LatestReturn"] = df["LatestReturnDate"].apply(lambda x: parse_iso_date(x, "MDDYY"))
        df["LatestReturnDate"] = df["_RAW_LatestReturn"].apply(pivot_date_received)
        df.drop(columns=["_RAW_LatestReturn"], inplace=True)
    else:
        df["LatestReturnDate"] = ""

    # 5) DECODE CourtCommittedBy → CourtCommittedByName (per codebook)
    if "CourtCommittedBy" in df.columns:
        court_map = {
            "0": "Transfer from Civil Institution",
            "1": "Special Sessions – New York City",
            "2": "County/Supreme Court – General Sessions",
            "5": "Preliminary Court",
            "8": "Children’s Court (Family Court after 9/62)",
            "9": "Court Not Stated"
        }
        df["CourtCommittedByName"] = (
            clean_unknown(df["CourtCommittedBy"])
            .map(court_map)
            .fillna("Unknown")
        )
    else:
        df["CourtCommittedByName"] = ""

    # 6) DECODE Race → RaceName (per codebook)
    if "Race" in df.columns:
        race_map = {
            "1": "White",
            "2": "Black",
            "3": "Oriental",
            "4": "American Indian",
            "5": "Puerto Rican",
            "6": "Puerto Rican"
        }
        df["RaceName"] = (
            clean_unknown(df["Race"])
            .map(race_map)
            .fillna("Unknown")
        )
    else:
        df["RaceName"] = ""

    # 7) AgeAtCommitment remains as a string (or convert to int if desired)
    df["AgeAtCommitment"] = df["AgeAtCommitment"].apply(lambda x: x if "AgeAtCommitment" in df.columns and pd.notna(x) else "")

    # 8) DECODE Religion → ReligionName (via religion_map.json)
    if "Religion" in df.columns:
        df["ReligionName"] = (
            clean_unknown(df["Religion"])
            .map(religion_map)
            .fillna("Unknown")
        )
    else:
        df["ReligionName"] = ""

    # 9) DECODE Sex → SexName (via sex_map.json)
    if "Sex" in df.columns:
        df["SexName"] = (
            clean_unknown(df["Sex"])
            .map(sex_map)
            .fillna("Unknown")
        )
    else:
        df["SexName"] = ""

    # 10) IdentifierNumber and CheckDigit can remain raw strings (if present)
    df["IdentifierNumber"] = df["IdentifierNumber"].fillna("") if "IdentifierNumber" in df.columns else ""
    df["CheckDigit"] = df["CheckDigit"].fillna("") if "CheckDigit" in df.columns else ""

    # 11) DECODE YearsResidenceNY (numeric) → leave as-is or convert
    if "YearsResidenceNY" in df.columns:
        df["YearsResidenceNY"] = df["YearsResidenceNY"].apply(lambda x: x if pd.notna(x) else "")
    else:
        df["YearsResidenceNY"] = ""

    # 12) DECODE MilitaryService → MilitaryServiceLabel (per codebook)
    if "MilitaryService" in df.columns:
        mil_map = {
            "0": "No military service",
            "1": "Military – honorable/general discharge",
            "2": "Military – discharged for mental disability",
            "3": "Military – discharged as undesirable (BCD/BCI)",
            "4": "Military – dishonorable discharge",
            "5": "Military – discharged as minor",
            "6": "Military – type not stated",
            "7": "Military – now in reserves",
            "8": "Military – active/AWOL",
            "9": "Military – not stated"
        }
        df["MilitaryServiceLabel"] = (
            clean_unknown(df["MilitaryService"])
            .map(mil_map)
            .fillna("Unknown")
        )
    else:
        df["MilitaryServiceLabel"] = ""

    # 13) DECODE Education → EducationLevel (per codebook)
    if "Education" in df.columns:
        edu_map = {
            "0": "Not stated",
            "1": "Illiterate/<3rd grade",
            "2": "Special/Remedial classes",
            "3": "3rd grade",
            "4": "4th grade",
            "5": "5th grade",
            "6": "6th grade",
            "7": "7th grade",
            "8": "8th grade",
            "9": "9th grade",
            "A": "10th grade",
            "B": "11th grade",
            "C": "12th grade",
            "E": "High school equivalency",
            "H": "High school graduate",
            "L": "Some college",
            "G": "College graduate",
            "M": "Master’s/Doctorate",
            "P": "Business college",
            "Q": "Technical institution",
            "R": "Other beyond high school"
        }
        df["EducationLevel"] = (
            clean_unknown(df["Education"])
            .map(edu_map)
            .fillna("Unknown")
        )
    else:
        df["EducationLevel"] = ""

    # 14) DECODE Occupation → OccupationName (per codebook)
    if "Occupation" in df.columns:
        occ_map = {
            "0": "Professional",
            "1": "Semi-professional",
            "2": "Manager/Official/Proprietor",
            "3": "Clerical",
            "4": "Sales worker",
            "5": "Craftsman/Foreman",
            "6": "Operative/Mechanic",
            "7": "Service worker",
            "8": "Laborer",
            "9": "Not stated/Unemployed/Housewife/Student"
        }
        df["OccupationName"] = (
            clean_unknown(df["Occupation"])
            .map(occ_map)
            .fillna("Unknown")
        )
    else:
        df["OccupationName"] = ""

    # 15) DECODE NarcoticsUse → NarcoticsUseLabel (per codebook)
    if "NarcoticsUse" in df.columns:
        narc_map = {
            "1": "Uses narcotics",
            "2": "Does not use narcotics",
            "4": "Denies, but suspected",
            "9": "Not stated whether uses"
        }
        df["NarcoticsUseLabel"] = (
            clean_unknown(df["NarcoticsUse"])
            .map(narc_map)
            .fillna("Unknown")
        )
    else:
        df["NarcoticsUseLabel"] = ""

    # 16) DECODE MaritalStatus → MaritalStatusName (per codebook)
    if "MaritalStatus" in df.columns:
        mar_map = {
            "0": "Single",
            "1": "Married",
            "2": "Divorced/Annulled",
            "3": "Widowed",
            "4": "Separated",
            "6": "Common-law",
            "9": "Not stated"
        }
        df["MaritalStatusName"] = (
            clean_unknown(df["MaritalStatus"])
            .map(mar_map)
            .fillna("Unknown")
        )
    else:
        df["MaritalStatusName"] = ""

    # 17) DECODE PrevCriminalRecord → PrevCriminalRecordLabel (per codebook)
    if "PrevCriminalRecord" in df.columns:
        prev_map = {
            "0": "No prior adult record",
            "1": "No prior adult conviction (dismissal)",
            "2": "No prior institutional commitment",
            "3": "Local jail/penitentiary only",
            "4": "State/Federal institution only",
            "5": "State/Federal + probation",
            "6": "Local + State/Federal, no probation",
            "7": "Local + State/Federal + probation",
            "8": "State/Federal + local + probation",
            "9": "Data not available"
        }
        df["PrevCriminalRecordLabel"] = (
            clean_unknown(df["PrevCriminalRecord"])
            .map(prev_map)
            .fillna("Unknown")
        )
    else:
        df["PrevCriminalRecordLabel"] = ""

    # 18) DECODE CountryOfBirth → CountryOfBirthName (via country_map.json)
    if "CountryOfBirth" in df.columns:
        df["CountryOfBirthName"] = (
            clean_unknown(df["CountryOfBirth"])
            .map(country_map)
            .fillna("Unknown")
        )
    else:
        df["CountryOfBirthName"] = ""

    # 19) YearEnteredUS (numeric) → YearEnteredUSNum (already present)
    if "YearEnteredUS" in df.columns:
        df["YearEnteredUSNum"] = df["YearEnteredUS"].apply(lambda x: x if pd.notna(x) else "")
    else:
        df["YearEnteredUSNum"] = ""

    # 20) DECODE NaturalizationStatus → NaturalizationStatusLabel (per codebook)
    if "NaturalizationStatus" in df.columns:
        nat_map = {
            "1": "Alien",
            "5": "First papers only",
            "6": "Naturalized via military service",
            "7": "Naturalized (not via military)",
            "8": "Foreign-born U.S. citizen",
            "9": "Not stated",
            "-": "Not stated"
        }
        df["NaturalizationStatusLabel"] = (
            clean_unknown(df["NaturalizationStatus"])
            .map(nat_map)
            .fillna("Unknown")
        )
    else:
        df["NaturalizationStatusLabel"] = ""

    # 21) DECODE PsychiatricClassification → PsychiatricClassificationLabel (via psych_map.json)
    if "PsychiatricClassification" in df.columns:
        df["PsychiatricClassificationLabel"] = (
            clean_unknown(df["PsychiatricClassification"])
            .map(psych_map)
            .fillna("Unknown")
        )
    else:
        df["PsychiatricClassificationLabel"] = ""

    # 22) DECODE InstitutionOriginal → InstitutionOriginalName (via institution_map.json)
    if "InstitutionOriginal" in df.columns:
        df["InstitutionOriginalName"] = (
            clean_unknown(df["InstitutionOriginal"])
            .map(institution_map)
            .fillna("Unknown")
        )
    else:
        df["InstitutionOriginalName"] = ""

    # 23) OriginalMonthYear (MYY) → parse & pivot to “YYYY-MM”
    if "OriginalMonthYear" in df.columns:
        df["_RAW_OMY"] = df["OriginalMonthYear"].apply(lambda x: parse_iso_date(x, "MYY"))
        df["OriginalMonthYear"] = df["_RAW_OMY"].apply(pivot_month_year)
        df.drop(columns=["_RAW_OMY"], inplace=True)
    else:
        df["OriginalMonthYear"] = ""

    # 24) MentalHygieneID (numeric string) → MentalHygieneIDNum
    if "MentalHygieneID" in df.columns:
        df["MentalHygieneIDNum"] = df["MentalHygieneID"].apply(lambda x: x if pd.notna(x) else "")
    else:
        df["MentalHygieneIDNum"] = ""

    # 25) DECODE ReturnType → ReturnTypeLabel (via return_type_map.json)
    if "ReturnType" in df.columns:
        df["ReturnTypeLabel"] = (
            clean_unknown(df["ReturnType"])
            .map(return_type_map)
            .fillna("Unknown")
        )
    else:
        df["ReturnTypeLabel"] = ""

    # 26) DECODE CurrentInstitution → CurrentInstitutionName (via institution_map.json)
    if "CurrentInstitution" in df.columns:
        df["CurrentInstitutionName"] = (
            clean_unknown(df["CurrentInstitution"])
            .map(institution_map)
            .fillna("Unknown")
        )
    else:
        df["CurrentInstitutionName"] = ""

    # 27) DECODE CrimeAttempted → CrimeAttemptedLabel (0 = Completed, 1 = Attempted)
    if "CrimeAttempted" in df.columns:
        attempt_map = {"0": "Completed", "1": "Attempted"}
        df["CrimeAttemptedLabel"] = (
            clean_unknown(df["CrimeAttempted"])
            .map(attempt_map)
            .fillna("Unknown")
        )
    else:
        df["CrimeAttemptedLabel"] = ""

    # 28) DECODE MinSentence & MaxSentence if needed (combine years/months or special codes)
    def decode_sentence_years_months(y: str, m: str) -> str:
        # y and m are raw strings; handle special codes first
        if not isinstance(y, str) or not isinstance(m, str):
            return ""
        # Death/indeterminate if month part is '&&&'
        if m == "&&&":
            return "Death/Indeterminate"
        # 100+ years if y == '999'
        if y == "999":
            return "100+ years"
        # Transfer/Indeterminate if y starts with '92' or '95'
        if y.startswith("92") or y.startswith("95"):
            return "Transfer/Indeterminate"
        try:
            yy = int(y)
        except:
            return ""
        if m == "T":
            mm = 10
        elif m == "E":
            mm = 11
        else:
            try:
                mm = int(m)
            except:
                mm = 0
        parts = []
        if yy > 0:
            parts.append(f"{yy} yr{'s' if yy != 1 else ''}")
        if mm > 0:
            parts.append(f"{mm} mo{'s' if mm != 1 else ''}")
        return ", ".join(parts) or "0 months"

    if "MinSentence" in df.columns or "MinSentenceYears" in df.columns:
        # If numeric years/months fields exist, map accordingly; else fallback
        def get_min_sen_label(row):
            if "MinSentenceYears" in df.columns and "MinSentenceMonths" in df.columns:
                return decode_sentence_years_months(
                    row["MinSentenceYears"], row["MinSentenceMonths"]
                )
            elif "MinSentence" in df.columns:
                return decode_sentence_years_months(row["MinSentence"], "")
            else:
                return ""

        df["MinSentenceLabel"] = df.apply(get_min_sen_label, axis=1)
    else:
        df["MinSentenceLabel"] = ""

    if "MaxSentence" in df.columns or "MaxSentenceYears" in df.columns:
        def get_max_sen_label(row):
            if "MaxSentenceYears" in df.columns and "MaxSentenceMonths" in df.columns:
                return decode_sentence_years_months(
                    row["MaxSentenceYears"], row["MaxSentenceMonths"]
                )
            elif "MaxSentence" in df.columns:
                return decode_sentence_years_months(row["MaxSentence"], "")
            else:
                return ""

        df["MaxSentenceLabel"] = df.apply(get_max_sen_label, axis=1)
    else:
        df["MaxSentenceLabel"] = ""

    # -------------------------------------------------------------------------
    # FINAL: Assemble a DataFrame containing ONLY the decoded columns, in logical order
    # -------------------------------------------------------------------------
    decoded = df[
        [
            "Institution",                    # from ReceivingInstitutionCode
            "County",                         # from CountyCommittedFrom
            "CourtCommittedByName",           # per codebook
            "Crime",                          # from CrimeDetails
            "CrimeAttemptedLabel",            # per codebook (if present)
            "DateOfBirth",                    # pivoted to YYYY-MM-DD
            "DateReceived",                   # kept from raw formatter
            "MinSentenceLabel",               # decoded if available
            "MaxSentenceLabel",               # decoded if available
            "AgeAtCommitment",                # as string
            "RaceName",                       # per codebook
            "ReligionName",                   # via religion_map.json
            "SexName",                        # via sex_map.json
            "IdentifierNumber",               # raw ID
            "CheckDigit",                     # raw
            "YearsResidenceNY",               # as string
            "MilitaryServiceLabel",           # per codebook
            "EducationLevel",                 # per codebook
            "OccupationName",                 # per codebook
            "NarcoticsUseLabel",              # per codebook
            "MaritalStatusName",              # per codebook
            "PrevCriminalRecordLabel",        # per codebook
            "CountryOfBirthName",             # via country_map.json
            "YearEnteredUSNum",               # as string
            "NaturalizationStatusLabel",      # per codebook
            "PsychiatricClassificationLabel", # via psych_map.json
            "InstitutionOriginalName",        # via institution_map.json
            "OriginalMonthYear",              # pivoted to YYYY-MM
            "MentalHygieneIDNum",             # as string
            "ReturnTypeLabel",                # via return_type_map.json
            "LatestReleaseDate",              # YYYY-MM
            "LatestReturnDate",               # YYYY-MM-DD
            "CurrentInstitutionName"          # via institution_map.json
        ]
    ]

    # Write out the final decoded CSV
    base = os.path.basename(path).replace("_rawformatted.csv", "")
    out_csv = os.path.join(output_folder, f"{base}_decoded.csv")
    decoded.to_csv(out_csv, index=False)
    print(f"Decoded saved: {out_csv}")

print("All files decoded.")
