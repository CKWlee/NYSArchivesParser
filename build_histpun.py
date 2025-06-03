import pandas as pd
import glob
import os

# ─── 1) FOLDERS AND FILE‐PATS ────────────────────────────────────────────────────
decoded_folder = "csv_decoded"
decoded_files = glob.glob(os.path.join(decoded_folder, "*_decoded.csv"))

if len(decoded_files) == 0:
    raise FileNotFoundError(f"No files matching '*_decoded.csv' in {decoded_folder!r}.")

# ─── 2) READ + CONCAT ALL DECODED CSVs ───────────────────────────────────────────
df_list = []
for f in decoded_files:
    df = pd.read_csv(f, dtype=str)
    df_list.append(df)

df_all = pd.concat(df_list, ignore_index=True)

# ─── 3) PARSE DATE‐FIELDS INTO DATETIME, EXTRACT YEAR ───────────────────────────
df_all["DateReceived"] = pd.to_datetime(df_all["DateReceived"], errors="coerce")
df_all = df_all[df_all["DateReceived"].notna()].copy()
df_all["Year"] = df_all["DateReceived"].dt.year

# ─── 4) COMPUTE AGE (YEARS) + AGE CATEGORY ─────────────────────────────────────
df_all["DateOfBirth"] = pd.to_datetime(df_all["DateOfBirth"], errors="coerce")
df_all["Age"] = ((df_all["DateReceived"] - df_all["DateOfBirth"]).dt.days // 365).astype("Int64")

def classify_age(val):
    if pd.isna(val):
        return ""
    return "juvenile" if val < 18 else "adult"

df_all["AgeCategory"] = df_all["Age"].apply(classify_age)

# ─── 5) SIMPLIFY CRIME INTO CrimeCategory ────────────────────────────────────────
df_all["CrimeCategory"] = (
    df_all["Crime"]
      .fillna("")
      .apply(lambda s: s.split(",", 1)[0].strip().lower())
)

# ─── 6) SET UP A SOURCE‐KEY HELPER ───────────────────────────────────────────────
def source_key(year):
    return f"NYInmateRecords{year}"

# ─── 7) AGGREGATE FOR EACH YEAR ─────────────────────────────────────────────────
histpun_rows = []
years = sorted(df_all["Year"].dropna().unique())

for year in years:
    year_df = df_all[df_all["Year"] == year].copy()
    src = source_key(int(year))

    # --- 7A) TOTAL PRISONER COUNT FOR THE YEAR (no qualifiers) ---
    total_count = len(year_df)
    histpun_rows.append({
        "Country":     "United States",
        "Year":        int(year),
        "Statistic":   "prisoners",
        "Value":       int(total_count),
        "Source":      src,
        "State":       "New York",
        "Race":        "",
        "Gender":      "",
        "Age":         "",
        "Crime":       "",
        "Institution": "",
        "Complete":    ""
    })

    # --- 7B) BY RACE × GENDER (Complete = "race,gender") ---
    if "RaceName" in year_df.columns and "SexName" in year_df.columns:
        rg = (
            year_df.groupby(["RaceName", "SexName"])
                   .size()
                   .reset_index(name="Count")
        )
        for _, row in rg.iterrows():
            histpun_rows.append({
                "Country":     "United States",
                "Year":        int(year),
                "Statistic":   "prisoners",
                "Value":       int(row["Count"]),
                "Source":      src,
                "State":       "New York",
                "Race":        row["RaceName"].strip().lower(),
                "Gender":      row["SexName"].strip().lower(),
                "Age":         "",
                "Crime":       "",
                "Institution": "",
                "Complete":    "race,gender"
            })

    # --- 7C) BY RACE ONLY (Complete = "race") ---
    if "RaceName" in year_df.columns:
        race_grp = (
            year_df.groupby("RaceName")
                   .size()
                   .reset_index(name="Count")
        )
        # We set Complete = "race" because this is a pure‐race breakdown
        for _, row in race_grp.iterrows():
            if row["RaceName"]:
                histpun_rows.append({
                    "Country":     "United States",
                    "Year":        int(year),
                    "Statistic":   "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        row["RaceName"].strip().lower(),
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       "",
                    "Institution": "",
                    "Complete":    "race"
                })

    # --- 7D) BY GENDER ONLY (Complete = "gender") ---
    if "SexName" in year_df.columns:
        gender_grp = (
            year_df.groupby("SexName")
                   .size()
                   .reset_index(name="Count")
        )
        for _, row in gender_grp.iterrows():
            if row["SexName"]:
                histpun_rows.append({
                    "Country":     "United States",
                    "Year":        int(year),
                    "Statistic":   "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      row["SexName"].strip().lower(),
                    "Age":         "",
                    "Crime":       "",
                    "Institution": "",
                    "Complete":    "gender"
                })

    # --- 7E) BY RELIGION ONLY (Complete = "religion") ---
    if "ReligionName" in year_df.columns:
        rel_grp = (
            year_df.groupby("ReligionName")
                   .size()
                   .reset_index(name="Count")
        )
        for _, row in rel_grp.iterrows():
            if row["ReligionName"]:
                histpun_rows.append({
                    "Country":     "United States",
                    "Year":        int(year),
                    "Statistic":   "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       "",
                    "Institution": "",
                    "Complete":    "religion"
                })

    # --- 7F) BY AGE CATEGORY (Complete = "age" if juvenile & adult appear) ---
    if "AgeCategory" in year_df.columns:
        age_grp = (
            year_df.groupby("AgeCategory")
                   .size()
                   .reset_index(name="Count")
        )
        cat_set = set(age_grp["AgeCategory"])
        age_complete = "age" if {"juvenile", "adult"}.issubset(cat_set) else ""
        for _, row in age_grp.iterrows():
            if row["AgeCategory"]:
                histpun_rows.append({
                    "Country":     "United States",
                    "Year":        int(year),
                    "Statistic":   "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         row["AgeCategory"],
                    "Crime":       "",
                    "Institution": "",
                    "Complete":    age_complete
                })

    # --- 7G) BY CRIME CATEGORY (no Complete flagged) ---
    if "CrimeCategory" in year_df.columns:
        crime_grp = (
            year_df.groupby("CrimeCategory")
                   .size()
                   .reset_index(name="Count")
        )
        for _, row in crime_grp.iterrows():
            if row["CrimeCategory"]:
                histpun_rows.append({
                    "Country":     "United States",
                    "Year":        int(year),
                    "Statistic":   "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       row["CrimeCategory"],
                    "Institution": "",
                    "Complete":    ""
                })

    # --- 7H) BY INSTITUTION (no Complete flagged) ---
    if "Institution" in year_df.columns:
        inst_grp = (
            year_df.groupby("Institution")
                   .size()
                   .reset_index(name="Count")
        )
        for _, row in inst_grp.iterrows():
            inst_name = row["Institution"]
            if pd.notna(inst_name) and inst_name != "":
                histpun_rows.append({
                    "Country":     "United States",
                    "Year":        int(year),
                    "Statistic":   "prisoners",
                    "Value":       int(row["Count"]),
                    "Source":      src,
                    "State":       "New York",
                    "Race":        "",
                    "Gender":      "",
                    "Age":         "",
                    "Crime":       "",
                    "Institution": inst_name.strip().lower(),
                    "Complete":    ""
                })

# ─── 8) BUILD THE FINAL DATAFRAME IN HISTPUN COLUMN ORDER ──────────────────────
histpun_df = pd.DataFrame(histpun_rows)
histpun_columns = [
    "Country",
    "Year",
    "Statistic",
    "Value",
    "Source",
    "State",
    "Race",
    "Gender",
    "Age",
    "Crime",
    "Institution",
    "Complete"
]

if histpun_df.shape[0] == 0:
    histpun_df = pd.DataFrame(columns=histpun_columns)
else:
    for col in histpun_columns:
        if col not in histpun_df.columns:
            histpun_df[col] = ""
    histpun_df = histpun_df[histpun_columns]

# ─── 9) WRITE THE FINAL CSV ─────────────────────────────────────────────────────
output_path = "histpun_output.csv"
histpun_df.to_csv(output_path, index=False, encoding="utf-8")
print(f"✔️  Wrote {len(histpun_df)} rows to '{output_path}'")
