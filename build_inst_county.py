import pandas as pd
import glob
import os

# ─── (1) Locate all decoded CSVs ───────────────────────────────────────────────
decoded_folder = "csv_decoded"
decoded_files  = glob.glob(os.path.join(decoded_folder, "*_decoded.csv"))

if not decoded_files:
    raise FileNotFoundError(f"No decoded CSV files found in '{decoded_folder}'")

# ─── (2) Read & concatenate all decoded CSVs ───────────────────────────────────
df_list = [pd.read_csv(fp, dtype=str) for fp in decoded_files]
df_all  = pd.concat(df_list, ignore_index=True)

# ─── (3) Parse `DateReceived` → extract `Year` ─────────────────────────────────
df_all["DateReceived"] = pd.to_datetime(df_all["DateReceived"], errors="coerce")
df_all = df_all[df_all["DateReceived"].notna()].copy()
df_all["Year"] = df_all["DateReceived"].dt.year

# ─── (4) Compute `Age` + `AgeCategory` ─────────────────────────────────────────
df_all["DateOfBirth"] = pd.to_datetime(df_all["DateOfBirth"], errors="coerce")
df_all["Age"] = ((df_all["DateReceived"] - df_all["DateOfBirth"]).dt.days // 365).astype("Int64")

def classify_age(age):
    if pd.isna(age):
        return ""
    return "juvenile" if age < 18 else "adult"

df_all["AgeCategory"] = df_all["Age"].apply(classify_age)

# ─── (5) Helper: build a consistent source key from year ────────────────────────
def source_key(y):
    return f"NYInmateRecords{y}"

# ─── (6) Build one row per (Year + various qualifiers) ─────────────────────────
histpun_rows = []
years = sorted(df_all["Year"].dropna().unique())

for year in years:
    sub = df_all[df_all["Year"] == year].copy()
    src = source_key(int(year))

    # — (6A) TOTAL PRISONERS (no qualifiers) —
    total_n = len(sub)
    histpun_rows.append({
        "Country":    "United States",
        "Year":        int(year),
        "Statistic":  "prisoners",
        "Value":       int(total_n),
        "Source":      src,
        "State":       "New York",
        "Race":        "",
        "Gender":      "",
        "Age":         "",
        "Crime":       "",
        "Institution": "",
        "County":      "",
        "Complete":    ""
    })

    # — (6B) BY RACE × GENDER (Complete = "race,gender") —
    if "RaceName" in sub.columns and "SexName" in sub.columns:
        rg = sub.groupby(["RaceName", "SexName"]).size().reset_index(name="Count")
        for _, row in rg.iterrows():
            histpun_rows.append({
                "Country":    "United States",
                "Year":        int(year),
                "Statistic":  "prisoners",
                "Value":       int(row["Count"]),
                "Source":      src,
                "State":       "New York",
                "Race":        row["RaceName"].strip().lower(),
                "Gender":      row["SexName"].strip().lower(),
                "Age":         "",
                "Crime":       "",
                "Institution": "",
                "County":      "",
                "Complete":    "race,gender"
            })

    # — (6C) BY RACE ONLY (Complete = "race") —
    if "RaceName" in sub.columns:
        rgrp = sub.groupby("RaceName").size().reset_index(name="Count")
        for _, row in rgrp.iterrows():
            name = row["RaceName"]
            if name:
                histpun_rows.append({
                    "Country":    "United States",
                    "Year":        int(year),
                    "Statistic":  "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        name.strip().lower(),
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       "",
                    "Institution": "",
                    "County":      "",
                    "Complete":    "race"
                })

    # — (6D) BY GENDER ONLY (Complete = "gender") —
    if "SexName" in sub.columns:
        ggrp = sub.groupby("SexName").size().reset_index(name="Count")
        for _, row in ggrp.iterrows():
            name = row["SexName"]
            if name:
                histpun_rows.append({
                    "Country":    "United States",
                    "Year":        int(year),
                    "Statistic":  "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      name.strip().lower(),
                    "Age":         "",
                    "Crime":       "",
                    "Institution": "",
                    "County":      "",
                    "Complete":    "gender"
                })

    # — (6E) BY RELIGION ONLY (Complete = "religion") —
    if "ReligionName" in sub.columns:
        relg = sub.groupby("ReligionName").size().reset_index(name="Count")
        for _, row in relg.iterrows():
            name = row["ReligionName"]
            if name:
                histpun_rows.append({
                    "Country":    "United States",
                    "Year":        int(year),
                    "Statistic":  "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       "",
                    "Institution": "",
                    "County":      "",
                    "Complete":    "religion"
                })

    # — (6F) BY AGE CATEGORY (Complete="age" if both juvenile+adult) —
    if "AgeCategory" in sub.columns:
        agrp = sub.groupby("AgeCategory").size().reset_index(name="Count")
        cats = set(agrp["AgeCategory"])
        flag = "age" if {"juvenile", "adult"} <= cats else ""
        for _, row in agrp.iterrows():
            cat = row["AgeCategory"]
            if cat:
                histpun_rows.append({
                    "Country":    "United States",
                    "Year":        int(year),
                    "Statistic":  "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         cat,
                    "Crime":       "",
                    "Institution": "",
                    "County":      "",
                    "Complete":    flag
                })

    # — (6G) BY INSTITUTION × COUNTY (dedicated columns) —
    if "Institution" in sub.columns and "County" in sub.columns:
        ic = sub.groupby(["Institution", "County"]).size().reset_index(name="Count")
        for _, row in ic.iterrows():
            inst = row["Institution"].strip().lower()
            cnty = row["County"].strip().lower()
            if inst and cnty:
                histpun_rows.append({
                    "Country":    "United States",
                    "Year":        int(year),
                    "Statistic":  "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       "",
                    "Institution": inst,
                    "County":      cnty,
                    "Complete":    ""
                })

    # — (6H) BY INSTITUTION ONLY (no Complete, no County) —
    if "Institution" in sub.columns:
        igrp = sub.groupby("Institution").size().reset_index(name="Count")
        for _, row in igrp.iterrows():
            inst = row["Institution"].strip().lower()
            if inst:
                histpun_rows.append({
                    "Country":    "United States",
                    "Year":        int(year),
                    "Statistic":  "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       "",
                    "Institution": inst,
                    "County":      "",
                    "Complete":    ""
                })

# ─── (7) Assemble final DataFrame with “County” column ─────────────────────────
histpun_df = pd.DataFrame(histpun_rows)
histpun_cols = [
    "Country", "Year", "Statistic", "Value", "Source", "State",
    "Race", "Gender", "Age", "Crime", "Institution", "County", "Complete"
]

if histpun_df.shape[0] == 0:
    histpun_df = pd.DataFrame(columns=histpun_cols)
else:
    for c in histpun_cols:
        if c not in histpun_df.columns:
            histpun_df[c] = ""
    histpun_df = histpun_df[histpun_cols]

# ─── (8) Write out the CSV ─────────────────────────────────────────────────────
output_path = "histpun_inst_county.csv"
histpun_df.to_csv(output_path, index=False, encoding="utf-8")
print(f"✔ Wrote {len(histpun_df)} rows to '{output_path}'")
