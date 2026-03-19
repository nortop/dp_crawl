import pandas as pd
from pathlib import Path
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.stats.proportion import proportion_confint

# ---------- 1. โหลดข้อมูล ----------
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")
FIG_DIR = 'figures'
import os
os.makedirs(FIG_DIR, exist_ok=True)

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / 'test.csv'
print("=" * 60)
print(f"กำลังโหลดข้อมูลจาก {CSV_PATH} ...")
try:
    df = pd.read_csv(CSV_PATH)
except pd.errors.ParserError:
    print("⚠️ พบแถว CSV ที่รูปแบบผิดพลาด กำลังข้ามแถวที่เสียหาย...")
    df = pd.read_csv(CSV_PATH, on_bad_lines='skip')

# df = pd.read_csv('observations_raw_dp3.csv')
print(f"Total rows: {len(df)}")
print(f"Unique domains: {df['domain'].nunique()}")

# ---------- 2. กำหนดคอลัมน์ ----------
dp_columns = ['dp1_flag', 'dp2_flag', 'dp3_flag', 'dp4_flag',
              'dp5_flag', 'dp6_flag', 'dp7_flag', 'dp8_flag']
dp_names = ['DP1', 'DP2', 'DP3', 'DP4', 'DP5', 'DP6', 'DP7', 'DP8']

# ---------- 3. กรอง desktop และรวม flags ด้วย max ----------
df_desktop = df[df['device'] == 'desktop'].copy()

# รวม flags
df_banner = df_desktop.groupby('domain')[dp_columns].max().reset_index()

# รวม banner_present (max = 1 ถ้ามี banner ใน run ใด)
banner_info = df_desktop.groupby('domain')['banner_present'].max().reset_index()
df_banner = df_banner.merge(banner_info, on='domain', how='left')

# กรองเฉพาะที่มี banner
df_banner = df_banner[df_banner['banner_present'] == 1].copy()

n = len(df_banner)
print(f"\n✅ จำนวนเว็บไซต์ที่มี banner (รวมทุก run): {n}")

# ---------- 4. คำนวณ prevalence และ confidence interval ----------
prevalence = []
ci_lower = []
ci_upper = []

for col in dp_columns:
    k = df_banner[col].sum()
    p = k / n * 100
    ci = proportion_confint(k, n, alpha=0.05, method='wilson')
    prevalence.append(p)
    ci_lower.append(ci[0] * 100)
    ci_upper.append(ci[1] * 100)
    print(f"{col}: {k}/{n} = {p:.1f}% (95% CI: {ci[0]*100:.1f}–{ci[1]*100:.1f})")

# ---------- 5. สร้าง Bar Chart ----------
plt.figure(figsize=(10, 6))
bars = plt.bar(dp_names, prevalence, color='steelblue', alpha=0.8)
plt.errorbar(dp_names, prevalence,
             yerr=[np.array(prevalence) - np.array(ci_lower),
                   np.array(ci_upper) - np.array(prevalence)],
             fmt='none', capsize=5, color='black', elinewidth=1)

plt.ylabel('Prevalence (%)')
plt.title(f'Overall Prevalence of Dark Patterns (n = {n})')
plt.ylim(0, 100)
plt.grid(axis='y', linestyle='--', alpha=0.3)

# แสดงตัวเลขบนแท่ง
for i, (bar, val) in enumerate(zip(bars, prevalence)):
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
             f'{val:.1f}%', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('figure3_prevalence_overall.pdf', dpi=300, bbox_inches='tight')
plt.savefig('figure3_prevalence_overall.png', dpi=300, bbox_inches='tight')
plt.show()