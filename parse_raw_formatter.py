#!/usr/bin/env python3
import pandas as pd
import glob
import os
import re
import numpy as np
import sys

# -----------------------------------------------------------------------------
# 1) Define your fixed-width slices (zero-based) and column names exactly per the codebook.
# -----------------------------------------------------------------------------
colspecs = [
    (0, 2),   # ReceivingInstitutionCode
    (2, 8),   # InmateNumber
    (8, 13),  # DateReceived    ← columns 9–13 (1-based)
    (13, 14), # CrimeCategory
    (14, 15), # SentenceType
    (15, 20), # DateOfBirth     ← columns 16–20 (1-based)
    (20, 24), # CrimeDetails
    (24, 27), # MinSentence
    (27, 30), # MaxSentence
    (30, 32), # CountyCommittedFrom
    (32, 33), # CourtCommittedBy
    (33, 34), # Race
    (34, 36), # AgeAtCommitment
    (36, 37), # Religion
    (37, 38), # Sex
    (38, 44), # IdentifierNumber
    (44, 45), # CheckDigit
    (45, 47), # YearsResidenceNY
    (47, 48), # MilitaryService
    (48, 49), # Education
    (49, 50), # Occupation
    (50, 51), # NarcoticsUse
    (51, 52), # MaritalStatus
    (52, 54), # PrevCriminalRecord
    (54, 55), # CommitmentsProbation
    (55, 56), # FinesSuspensions
    (56, 57), # TimeSpanEarliestAdultRecord
    (57, 58), # MinorPoliceContacts
    (58, 59), # SeriousPoliceContacts
    (59, 61), # CountryOfBirth
    (61, 63), # YearEnteredUS
    (63, 64), # NaturalizationStatus
    (64, 66), # PsychiatricClassification
    (66, 68), # InstitutionOriginal
    (68, 70), # OriginalMonthYear
    (70, 71), # MentalHygieneID
    (71, 72), # ReturnType
    (72, 75), # LatestReleaseDate
    (75, 78), # LatestReturnDate
    (78, 80), # CurrentInstitution
]

colnames = [
    'ReceivingInstitutionCode', 'InmateNumber',    'DateReceived',
    'CrimeCategory',             'SentenceType',    'DateOfBirth',
    'CrimeDetails',              'MinSentence',     'MaxSentence',
    'CountyCommittedFrom',       'CourtCommittedBy','Race',
    'AgeAtCommitment',           'Religion',        'Sex',
    'IdentifierNumber',          'CheckDigit',      'YearsResidenceNY',
    'MilitaryService',           'Education',       'Occupation',
    'NarcoticsUse',              'MaritalStatus',   'PrevCriminalRecord',
    'CommitmentsProbation',      'FinesSuspensions','TimeSpanEarliestAdultRecord',
    'MinorPoliceContacts',       'SeriousPoliceContacts','CountryOfBirth',
    'YearEnteredUS',             'NaturalizationStatus','PsychiatricClassification',
    'InstitutionOriginal',       'OriginalMonthYear','MentalHygieneID',
    'ReturnType',                'LatestReleaseDate','LatestReturnDate',
    'CurrentInstitution'
]

# -----------------------------------------------------------------------------
# 2) A helper to clean any “&”, “9”, or blank as missing.
# -----------------------------------------------------------------------------
def clean_marker(x: str) -> str:
    if pd.isna(x):
        return ""
    s = x.strip()
    if s in {"&", "9", ""}:
        return ""
    return s

# -----------------------------------------------------------------------------
# 3) parse_iso_date: turn a raw MDDYY or MYY string into a “yy-MM-DD” or “yy-MM” stub.
# -----------------------------------------------------------------------------
def parse_iso_date(raw: str, fmt: str = 'MDDYY') -> str:
    if not isinstance(raw, str):
        return ""
    s = re.sub(r'\D', '', raw)
    if fmt == 'MDDYY':
        if len(s) == 5:
            mm, dd, yy = s[0], s[1:3], s[3:5]
        elif len(s) == 6:
            mm, dd, yy = s[0:2], s[2:4], s[4:6]
        else:
            return ""
        return f"{int(yy):02d}-{int(mm):02d}-{int(dd):02d}"
    elif fmt == 'MYY':
        if len(s) == 3:
            mm, yy = s[0], s[1:3]
        elif len(s) == 4:
            mm, yy = s[0:2], s[2:4]
        else:
            return ""
        return f"{int(yy):02d}-{int(mm):02d}"
    return ""

# -----------------------------------------------------------------------------
# 4) pivot_date_received: “yy-MM-DD” → “YYYY-MM-DD” (all receipts are 1900s)
# -----------------------------------------------------------------------------
def pivot_date_received(ysm: str) -> str:
    if not isinstance(ysm, str) or len(ysm) != 8:
        return ""
    yy, mm, dd = ysm.split("-")
    return f"{1900 + int(yy):04d}-{mm}-{dd}"

# -----------------------------------------------------------------------------
# 5) pivot_dob: “yy-MM-DD” + receipt-year “YYYY-MM-DD” → “YYYY-MM-DD”
# -----------------------------------------------------------------------------
def pivot_dob(ysm: str, recv_full: str) -> str:
    if not (isinstance(ysm, str) and len(ysm) == 8 and isinstance(recv_full, str) and len(recv_full) >= 4):
        return ""
    yy, mm, dd = ysm.split("-")
    yy_i = int(yy)
    recv_year_full = int(recv_full[:4])  # e.g. “1952”
    recv_yy = recv_year_full % 100       # e.g. 52
    if yy_i > recv_yy:
        return f"{1800 + yy_i:04d}-{mm}-{dd}"
    else:
        return f"{1900 + yy_i:04d}-{mm}-{dd}"

# -----------------------------------------------------------------------------
# 6) pivot_month_year: “yy-MM” → “YYYY-MM” (all in 1900s)
# -----------------------------------------------------------------------------
def pivot_month_year(ym: str) -> str:
    if not isinstance(ym, str) or len(ym) != 5:
        return ""
    yy, mm = ym.split("-")
    return f"{1900 + int(yy):04d}-{mm}"

# -----------------------------------------------------------------------------
# 7) Main loop: read every .txt and write its “_rawformatted.csv”
# -----------------------------------------------------------------------------
output_folder = "csv_raw_formatted"
os.makedirs(output_folder, exist_ok=True)

for txt_file in glob.glob("*.txt"):
    try:
        # a) Read fixed-width as strings to preserve every code exactly
        df = pd.read_fwf(
            txt_file,
            colspecs=colspecs,
            names=colnames,
            dtype=str,
            encoding='latin1',
        )

        # b) Clean each code-column from stray whitespace (but keep “&”, “9” here)
        for col in colnames:
            df[col] = df[col].astype(str)

        # c) Parse and pivot DateReceived
        df["_STUB_DateReceived"] = df["DateReceived"].apply(lambda x: parse_iso_date(clean_marker(x), "MDDYY"))
        df["DateReceived"] = df["_STUB_DateReceived"].apply(pivot_date_received)

        # d) Parse then pivot DateOfBirth (using the newly-pivoted DateReceived)
        df["_STUB_DateOfBirth"] = df["DateOfBirth"].apply(lambda x: parse_iso_date(clean_marker(x), "MDDYY"))
        df["DateOfBirth"] = [
            pivot_dob(dob_stub, recv_full)
            for dob_stub, recv_full in zip(df["_STUB_DateOfBirth"], df["DateReceived"])
        ]

        # e) Parse & pivot LatestReleaseDate (MYY)
        df["_STUB_LatestRelease"] = df["LatestReleaseDate"].apply(lambda x: parse_iso_date(clean_marker(x), "MYY"))
        df["LatestReleaseDate"] = df["_STUB_LatestRelease"].apply(pivot_month_year)

        # f) Parse & pivot LatestReturnDate (MDDYY)
        df["_STUB_LatestReturn"] = df["LatestReturnDate"].apply(lambda x: parse_iso_date(clean_marker(x), "MDDYY"))
        df["LatestReturnDate"] = df["_STUB_LatestReturn"].apply(pivot_date_received)

        # g) Drop temporary stub columns
        df.drop(columns=["_STUB_DateReceived", "_STUB_DateOfBirth", "_STUB_LatestRelease", "_STUB_LatestReturn"], inplace=True)

        # h) Write to CSV
        base = os.path.splitext(os.path.basename(txt_file))[0]
        out_csv = os.path.join(output_folder, f"{base}_rawformatted.csv")
        df.to_csv(out_csv, index=False, encoding='utf-8')
        print(f"Saved: {out_csv}")

    except Exception as e:
        print(f"Error processing {txt_file}: {e}", file=sys.stderr)

print("Raw formatting done.")
