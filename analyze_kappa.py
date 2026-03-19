import csv
import re
from pathlib import Path
from typing import Optional

import pandas as pd
from sklearn.metrics import accuracy_score, cohen_kappa_score
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support


AUTO_CSV = Path("observations_raw_dp3.csv")
MANUAL_CSV = Path("manual_10032026-14032026.csv")
DOMAIN_RE = re.compile(r"([a-z0-9.-]+\.[a-z]{2,})$", re.IGNORECASE)


def load_csv_resilient(path: Path) -> pd.DataFrame:
    rows = []
    recovered_rows = 0
    skipped_rows = 0

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        expected_len = len(header)

        for line_no, row in enumerate(reader, start=2):
            if len(row) == expected_len:
                rows.append(row)
                continue

            if len(row) > expected_len:
                primary = row[:expected_len]
                rows.append(primary)

                overflow = row[expected_len:]
                domain_match = DOMAIN_RE.search(primary[-1].strip())
                if domain_match and len(overflow) == expected_len - 1:
                    primary[-1] = primary[-1][:domain_match.start()].rstrip(" ;")
                    rows[-1] = primary
                    rows.append([domain_match.group(1), *overflow])
                    recovered_rows += 1
                    print(
                        f"⚠️ ซ่อมแถวที่ข้อมูลต่อกันที่บรรทัด {line_no}: "
                        f"กู้คืนเพิ่ม 1 แถว"
                    )
                else:
                    skipped_rows += 1
                    print(
                        f"⚠️ พบแถวคอลัมน์เกินที่บรรทัด {line_no} "
                        f"({len(row)} คอลัมน์, คาดว่า {expected_len}) "
                        f"เก็บเฉพาะช่วงแรก"
                    )
                continue

            skipped_rows += 1
            print(
                f"⚠️ ข้ามบรรทัด {line_no} เพราะคอลัมน์ไม่ครบ "
                f"({len(row)} คอลัมน์, คาดว่า {expected_len})"
            )

    df = pd.DataFrame(rows, columns=header)
    print(
        f"✅ โหลดข้อมูล {path.name} สำเร็จ: {len(df)} แถว "
        f"(กู้คืน {recovered_rows} แถว, ข้าม {skipped_rows} แถว)"
    )
    return df


def normalize_binary(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.where(numeric.isin([0, 1]))


def build_domain_level_auto(df_auto: pd.DataFrame) -> pd.DataFrame:
    dp_auto_cols = [
        "dp1_flag",
        "dp2_flag",
        "dp3_flag",
        "dp4_flag",
        "dp5_flag",
        "dp6_flag",
        "dp7_flag",
        "dp8_flag",
        "dp3_flag_adv",
    ]
    available_cols = ["domain", *[c for c in dp_auto_cols if c in df_auto.columns]]
    df_auto = df_auto[available_cols].copy()

    for col in available_cols:
        if col != "domain":
            df_auto[col] = normalize_binary(df_auto[col])

    return df_auto.groupby("domain", as_index=False).max()


def resolve_column(df: pd.DataFrame, base_name: Optional[str], suffix: str) -> Optional[str]:
    if not base_name:
        return None
    suffixed = f"{base_name}_{suffix}"
    if suffixed in df.columns:
        return suffixed
    if base_name in df.columns:
        return base_name
    return None


def main() -> None:
    try:
        df_auto = load_csv_resilient(AUTO_CSV)
        df_manual = pd.read_csv(MANUAL_CSV, encoding="utf-8-sig")
        print(f"✅ โหลดข้อมูล Manual สำเร็จ: {len(df_manual)} แถว")
    except FileNotFoundError:
        print("❌ ไม่พบไฟล์")
        print("กรุณาวางไฟล์ในโฟลเดอร์เดียวกับสคริปต์นี้")
        raise SystemExit(1)

    df_auto = build_domain_level_auto(df_auto)
    print(f"✅ สรุปผล AUTO ระดับโดเมน: {len(df_auto)} แถว")
    print(f"คอลัมน์ที่มี: {df_auto.columns.tolist()} vs {df_manual.columns.tolist()}")

    df_merged = pd.merge(df_auto, df_manual, on="domain", suffixes=("_auto", "_manual"))
    print(f"✅ merge สำเร็จ: {len(df_merged)} โดเมน")

    column_map = {
        "dp1": ("dp1_flag", "dp1_flag"),
        "dp2": ("dp2_flag", "dp2_flag"),
        "dp3": ("dp3_flag", "dp3_flag"),
        "dp4": ("dp4_flag", "dp4_flag"),
        "dp5": ("dp5_flag", "dp5_flag"),
        "dp6": ("dp6_flag", "dp6_flag"),
        "dp7": ("dp7_flag", "dp7_flag"),
        "dp8": ("dp8_flag", "dp8_flag"),
        "dp11": ("dp3_flag_adv", "dp3_flag_adv / dp11"),
    }
    results = []

    for dp, (auto_base, manual_base) in column_map.items():
        auto_col = resolve_column(df_merged, auto_base, "auto")
        manual_col = resolve_column(df_merged, manual_base, "manual")

        if not auto_col or not manual_col:
            results.append(
                {
                    "DP": dp.upper(),
                    "Pairs_Used": 0,
                    "Skipped": len(df_merged),
                    "Note": "missing column in one of the files",
                }
            )
            continue

        y_auto = normalize_binary(df_merged[auto_col])
        y_manual = normalize_binary(df_merged[manual_col])
        valid_mask = y_auto.notna() & y_manual.notna()
        pairs_used = int(valid_mask.sum())

        if pairs_used == 0:
            results.append(
                {
                    "DP": dp.upper(),
                    "Pairs_Used": 0,
                    "Skipped": len(df_merged),
                    "Note": "no valid 0/1 pairs after filtering blanks/invalid values",
                }
            )
            continue

        y_auto_valid = y_auto[valid_mask].astype(int)
        y_manual_valid = y_manual[valid_mask].astype(int)

        kappa = cohen_kappa_score(y_manual_valid, y_auto_valid)
        acc = accuracy_score(y_manual_valid, y_auto_valid)
        prec, rec, f1, _ = precision_recall_fscore_support(
            y_manual_valid,
            y_auto_valid,
            average="binary",
            zero_division=0,
            pos_label=1,
        )
        tn, fp, fn, tp = confusion_matrix(y_manual_valid, y_auto_valid, labels=[0, 1]).ravel()

        results.append(
            {
                "DP": dp.upper(),
                "Pairs_Used": pairs_used,
                "Skipped": int((~valid_mask).sum()),
                "Kappa": round(kappa, 3),
                "Accuracy": round(acc, 3),
                "Precision": round(prec, 3),
                "Recall": round(rec, 3),
                "F1": round(f1, 3),
                "TP": tp,
                "FP": fp,
                "FN": fn,
                "TN": tn,
                "Auto_Prev": round(y_auto_valid.mean() * 100, 1),
                "Manual_Prev": round(y_manual_valid.mean() * 100, 1),
                "Note": "",
            }
        )

    df_results = pd.DataFrame(results)
    print(df_results.to_string(index=False))
    df_results.to_csv("automated_vs_manual_agreement.csv", index=False)


if __name__ == "__main__":
    main()



# Result :
# ⚠️ ซ่อมแถวที่ข้อมูลต่อกันที่บรรทัด 1185: กู้คืนเพิ่ม 1 แถว
# ✅ โหลดข้อมูล observations_raw_dp3.csv สำเร็จ: 1556 แถว (กู้คืน 1 แถว, ข้าม 0 แถว)
# ✅ โหลดข้อมูล Manual สำเร็จ: 390 แถว
# ✅ สรุปผล AUTO ระดับโดเมน: 389 แถว
# คอลัมน์ที่มี: ['domain', 'dp1_flag', 'dp2_flag', 'dp3_flag', 'dp4_flag', 'dp5_flag', 'dp6_flag', 'dp7_flag', 'dp8_flag', 'dp3_flag_adv'] vs ['domain', 'stratum', 'Accessed', 'dp9', 'banner_present', 'banner_type', 'cms_cmp_vendor', 'language_detected', 'accept_all_first_layer', 'reject_all_first_layer', 'manage_first_layer', 'close_button_present', 'clicks_accept_all', 'clicks_reject_all', 'delta_clicks', 'steps_reject_description', 'reject_success', 'dp1_flag', 'dp1_reason', 'dp2_flag', 'has_toggle_analytics', 'has_toggle_ads', 'default_on_analytics', 'default_on_ads', 'dp3_flag', 'dp4_flag', 'dp5_flag', 'dp6_flag', 'dp7_flag', 'dp8_flag', 'dp8_del_req', 'dp3_flag_adv / dp11', 'notes', 'dp10', 'dp12', 'Unnamed: 35']
# ✅ merge สำเร็จ: 372 โดเมน
#   DP  Pairs_Used  Skipped  Kappa  Accuracy  Precision  Recall    F1    TP   FP    FN    TN  Auto_Prev  Manual_Prev                                                     Note
#  DP1         132      240  0.000     0.909      0.909   1.000 0.952 120.0 12.0   0.0   0.0      100.0         90.9                                                         
#  DP2         103      269 -0.055     0.282      0.190   0.727 0.302  16.0 68.0   6.0  13.0       81.6         21.4                                                         
#  DP3         125      247  0.000     0.416      0.000   0.000 0.000   0.0  0.0  73.0  52.0        0.0         58.4                                                         
#  DP4         184      188 -0.016     0.228      0.500   0.014 0.027   2.0  2.0 140.0  40.0        2.2         77.2                                                         
#  DP5          24      348  0.647     0.958      0.500   1.000 0.667   1.0  1.0   0.0  22.0        8.3          4.2                                                         
#  DP6         185      187  0.000     0.735      0.000   0.000 0.000   0.0  0.0  49.0 136.0        0.0         26.5                                                         
#  DP7          26      346  0.196     0.769      1.000   0.760 0.864  19.0  0.0   6.0   1.0       73.1         96.2                                                         
#  DP8         276       96  0.034     0.623      0.198   0.367 0.257  18.0 73.0  31.0 154.0       33.0         17.8                                                         
#  DP9           0      372    NaN       NaN        NaN     NaN   NaN   NaN  NaN   NaN   NaN        NaN          NaN                       missing column in one of the files
# DP10           0      372    NaN       NaN        NaN     NaN   NaN   NaN  NaN   NaN   NaN        NaN          NaN                       missing column in one of the files
# DP11           0      372    NaN       NaN        NaN     NaN   NaN   NaN  NaN   NaN   NaN        NaN          NaN no valid 0/1 pairs after filtering blanks/invalid values
# DP12           0      372    NaN       NaN        NaN     NaN   NaN   NaN  NaN   NaN   NaN        NaN          NaN                       missing column in one of the files
# hmzdev6@TopMBA-M4 dp_crawl % python3 analyze_kappa.py
# ⚠️ ซ่อมแถวที่ข้อมูลต่อกันที่บรรทัด 1185: กู้คืนเพิ่ม 1 แถว
# ✅ โหลดข้อมูล observations_raw_dp3.csv สำเร็จ: 1556 แถว (กู้คืน 1 แถว, ข้าม 0 แถว)
# ✅ โหลดข้อมูล Manual สำเร็จ: 390 แถว
# ✅ สรุปผล AUTO ระดับโดเมน: 389 แถว
# คอลัมน์ที่มี: ['domain', 'dp1_flag', 'dp2_flag', 'dp3_flag', 'dp4_flag', 'dp5_flag', 'dp6_flag', 'dp7_flag', 'dp8_flag', 'dp3_flag_adv'] vs ['domain', 'stratum', 'Accessed', 'dp9', 'banner_present', 'banner_type', 'cms_cmp_vendor', 'language_detected', 'accept_all_first_layer', 'reject_all_first_layer', 'manage_first_layer', 'close_button_present', 'clicks_accept_all', 'clicks_reject_all', 'delta_clicks', 'steps_reject_description', 'reject_success', 'dp1_flag', 'dp1_reason', 'dp2_flag', 'has_toggle_analytics', 'has_toggle_ads', 'default_on_analytics', 'default_on_ads', 'dp3_flag', 'dp4_flag', 'dp5_flag', 'dp6_flag', 'dp7_flag', 'dp8_flag', 'dp8_del_req', 'dp3_flag_adv / dp11', 'notes', 'dp10', 'dp12', 'Unnamed: 35']
# ✅ merge สำเร็จ: 372 โดเมน
#   DP  Pairs_Used  Skipped  Kappa  Accuracy  Precision  Recall    F1    TP   FP    FN    TN  Auto_Prev  Manual_Prev                                                     Note
#  DP1         132      240  0.000     0.909      0.909   1.000 0.952 120.0 12.0   0.0   0.0      100.0         90.9                                                         
#  DP2         103      269 -0.055     0.282      0.190   0.727 0.302  16.0 68.0   6.0  13.0       81.6         21.4                                                         
#  DP3         125      247  0.000     0.416      0.000   0.000 0.000   0.0  0.0  73.0  52.0        0.0         58.4                                                         
#  DP4         184      188 -0.016     0.228      0.500   0.014 0.027   2.0  2.0 140.0  40.0        2.2         77.2                                                         
#  DP5          24      348  0.647     0.958      0.500   1.000 0.667   1.0  1.0   0.0  22.0        8.3          4.2                                                         
#  DP6         185      187  0.000     0.735      0.000   0.000 0.000   0.0  0.0  49.0 136.0        0.0         26.5                                                         
#  DP7          26      346  0.196     0.769      1.000   0.760 0.864  19.0  0.0   6.0   1.0       73.1         96.2                                                         
#  DP8         276       96  0.034     0.623      0.198   0.367 0.257  18.0 73.0  31.0 154.0       33.0         17.8                                                         
# DP11           0      372    NaN       NaN        NaN     NaN   NaN   NaN  NaN   NaN   NaN        NaN          N