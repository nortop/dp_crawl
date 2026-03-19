import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import chi2_contingency, fisher_exact
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

# # Windows (รัน Command Prompt as Administrator)
# pip install pandas numpy matplotlib seaborn scipy openpyxl
# sudo pip install pandas numpy matplotlib seaborn scipy openpyxl
# python3 analyze_dp.py

print("="*50)
print("เริ่มการวิเคราะห์ Dark Patterns")
print("="*50)

# ================== โหลดข้อมูล ==================
try:
    df = pd.read_csv('manual_10032026-14032026.csv', encoding='utf-8-sig')
    print(f"✅ โหลดข้อมูลสำเร็จ: {len(df)} แถว")
except FileNotFoundError:
    print("❌ ไม่พบไฟล์ manual_10032026-14032026.csv")
    print("กรุณาวางไฟล์ในโฟลเดอร์เดียวกับสคริปต์นี้")
    exit()

print(f"คอลัมน์ที่มี: {df.columns.tolist()}")

# ================== ทำความสะอาดข้อมูลเบื้องต้น ==================
# กรองเอาเฉพาะที่เข้าถึงได้
df_ok = df[df['Accessed'] == 'ok'].copy()
print(f"✅ เว็บที่เข้าถึงได้: {len(df_ok)} จาก {len(df)}")
print(f"✅ โดเมนที่ไม่ซ้ำ: {df_ok['domain'].nunique()}")

# แก้ชื่อคอลัมน์ dp11 (ถ้ามี)
if 'dp3_flag_adv / dp11' in df_ok.columns:
    df_ok.rename(columns={'dp3_flag_adv / dp11': 'dp11'}, inplace=True)
    print("✅ แก้ชื่อคอลัมน์ dp11 แล้ว")

# รายชื่อ DP flags ที่จะวิเคราะห์
dp_columns = ['dp1_flag', 'dp2_flag', 'dp3_flag', 'dp4_flag', 'dp5_flag',
              'dp6_flag', 'dp7_flag', 'dp8_flag', 'dp9', 'dp10', 'dp11', 'dp12']

# แปลงคอลัมน์เหล่านี้เป็นตัวเลข 0/1
for col in dp_columns:
    if col in df_ok.columns:
        # แทนที่ค่าว่างด้วย NaN
        df_ok[col] = df_ok[col].replace(r'^\s*$', np.nan, regex=True)
        # แทนที่ค่า '0', '1' string เป็นตัวเลข
        df_ok[col] = pd.to_numeric(df_ok[col], errors='coerce')
        # ถ้าคอลัมน์นี้มีค่า NaN หมด แสดงว่าอาจไม่มีข้อมูล
        if df_ok[col].notna().sum() == 0:
            print(f"⚠️ คอลัมน์ {col} ไม่มีข้อมูล")
    else:
        print(f"⚠️ ไม่พบคอลัมน์ {col}")

# ================== 1. สถิติรวมของแต่ละ DP ==================
def prevalence_summary(df, dp_cols):
    summary = []
    for col in dp_cols:
        if col not in df.columns:
            continue
        total = df[col].notna().sum()  # จำนวนที่มีข้อมูล (Applicable)
        if total == 0:
            continue
        count_1 = (df[col] == 1).sum()
        prevalence = count_1 / total if total > 0 else 0
        summary.append({
            'DP': col,
            'Applicable (N)': total,
            'พบ (count=1)': count_1,
            'Prevalence (%)': round(prevalence * 100, 2)
        })
    return pd.DataFrame(summary)

overall = prevalence_summary(df_ok, dp_columns)
print("\n" + "="*50)
print("📊 สถิติรวมของ Dark Patterns")
print("="*50)
if not overall.empty:
    print(overall.to_string(index=False))
else:
    print("ไม่มีข้อมูล DP")

# ================== 2. แยกตาม Stratum ==================
strata = df_ok['stratum'].dropna().unique()
print(f"\nกลุ่ม Stratum ที่พบ: {strata}")

stratum_summary = {}
for stratum in strata:
    sub = df_ok[df_ok['stratum'] == stratum]
    stratum_summary[stratum] = prevalence_summary(sub, dp_columns)

# รวมเป็นตารางเปรียบเทียบ (เฉพาะ Prevalence)
comparison = pd.DataFrame()
for stratum in strata:
    temp = stratum_summary[stratum].set_index('DP')['Prevalence (%)'].rename(stratum)
    comparison = pd.concat([comparison, temp], axis=1)

print("\n" + "="*50)
print("📊 เปรียบเทียบ Prevalence (%) ตาม Stratum")
print("="*50)
if not comparison.empty:
    print(comparison.fillna(0).to_string())
else:
    print("ไม่มีข้อมูลเปรียบเทียบ")

# ================== 3. นับ Tracking Scripts จาก notes ==================
def extract_scripts(notes):
    if pd.isna(notes) or notes == '':
        return []
    # แยกด้วยเครื่องหมาย + และ ,
    notes = str(notes)
    parts = notes.replace(',', '+').split('+')
    return [p.strip() for p in parts if p.strip() != '']

all_scripts = []
for notes in df_ok['notes'].dropna():
    all_scripts.extend(extract_scripts(notes))

script_counter = Counter(all_scripts)
print("\n" + "="*50)
print("📊 Tracking Scripts ที่พบบ่อย (รวมทุก stratum)")
print("="*50)
if script_counter:
    for i, (script, count) in enumerate(script_counter.most_common(20)):
        print(f"{i+1:2d}. {script}: {count} ครั้ง")
else:
    print("ไม่พบข้อมูล tracking scripts")

# แยกตาม stratum
script_by_stratum = {}
for stratum in strata:
    sub = df_ok[df_ok['stratum'] == stratum]
    scripts = []
    for notes in sub['notes'].dropna():
        scripts.extend(extract_scripts(notes))
    script_by_stratum[stratum] = Counter(scripts)

# ================== 4. ทดสอบนัยสำคัญทางสถิติ ==================
print("\n" + "="*50)
print("📊 การทดสอบความแตกต่างระหว่าง Stratum (Chi-square)")
print("="*50)

significance = []
for col in dp_columns:
    if col not in df_ok.columns:
        continue
    # ใช้เฉพาะแถวที่ไม่ใช่ NaN ในคอลัมน์นั้น
    temp = df_ok[df_ok[col].notna()].copy()
    if temp.shape[0] < 10:  # ต้องการอย่างน้อย 10 ตัวอย่าง
        continue
    
    # สร้าง contingency table
    crosstab = pd.crosstab(temp['stratum'], temp[col])
    
    # ถ้ามี stratum น้อยกว่า 2 หรือไม่มีทั้ง 0 และ 1 ให้ข้าม
    if crosstab.shape[0] < 2 or crosstab.shape[1] < 2:
        p = np.nan
        method = "ไม่พอ"
    else:
        try:
            # ตรวจสอบ expected counts
            chi2, p, dof, expected = chi2_contingency(crosstab)
            method = "Chi-square"
            # ถ้ามี expected < 5 เกิน 20% ควรใช้ Fisher's exact
            if (expected < 5).sum() / expected.size > 0.2:
                # ใช้ Fisher's exact สำหรับ 2x2 เท่านั้น
                if crosstab.shape == (2,2):
                    odds, p = fisher_exact(crosstab)
                    method = "Fisher's exact"
        except:
            p = np.nan
            method = "error"
    
    significance.append({
        'DP': col, 
        'p-value': round(p, 4) if not np.isnan(p) else '-',
        'Test': method,
        'มีนัยสำคัญ (p<0.05)': p < 0.05 if not np.isnan(p) else False
    })

sig_df = pd.DataFrame(significance)
if not sig_df.empty:
    print(sig_df.to_string(index=False))
else:
    print("ไม่มีข้อมูลเพียงพอสำหรับการทดสอบ")

# ================== 5. การวิเคราะห์เพิ่มเติม ==================
print("\n" + "="*50)
print("📊 การวิเคราะห์เพิ่มเติม")
print("="*50)

# ความสัมพันธ์ระหว่าง dp7 กับ dp3
df_ok['dp3_and_dp7'] = ((df_ok['dp3_flag'] == 1) & (df_ok['dp7_flag'] == 1)).astype(int)
co_occur = df_ok[df_ok['dp3_flag'].notna() & df_ok['dp7_flag'].notna()]['dp3_and_dp7'].sum()
total_both = df_ok[df_ok['dp3_flag'].notna() & df_ok['dp7_flag'].notna()].shape[0]
if total_both > 0:
    print(f"📌 เว็บที่มีทั้ง dp3 และ dp7: {co_occur} จาก {total_both} ({co_occur/total_both*100:.2f}%)")
else:
    print("📌 ไม่มีข้อมูล dp3 และ dp7 พร้อมกัน")

# เว็บที่ไม่มีปุ่ม reject (dp1_reason)
dp1_no_reject = df_ok[df_ok['dp1_reason'].str.contains('no_reject', na=False)].shape[0]
if dp1_no_reject > 0:
    print(f"📌 เว็บที่ไม่มีปุ่ม reject เลย: {dp1_no_reject} จาก {df_ok[df_ok['dp1_flag'].notna()].shape[0]}")

# เฉลี่ย delta_clicks
if 'delta_clicks' in df_ok.columns:
    df_ok['delta_clicks'] = pd.to_numeric(df_ok['delta_clicks'], errors='coerce')
    avg_delta = df_ok['delta_clicks'].mean()
    print(f"📌 ค่าเฉลี่ย delta_clicks: {avg_delta:.2f}")

# ================== 6. สร้างกราฟ ==================
try:
    # สร้างโฟลเดอร์สำหรับรูป
    import os
    if not os.path.exists('charts'):
        os.makedirs('charts')
    
    # กราฟแท่งแสดง prevalence รวม
    plt.figure(figsize=(12, 6))
    overall_sorted = overall.sort_values('Prevalence (%)', ascending=False)
    bars = plt.bar(overall_sorted['DP'], overall_sorted['Prevalence (%)'], color='skyblue')
    plt.xlabel('Dark Pattern')
    plt.ylabel('Prevalence (%)')
    plt.title('Prevalence ของ Dark Patterns (ภาพรวม)')
    plt.xticks(rotation=45)
    for bar, val in zip(bars, overall_sorted['Prevalence (%)']):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, f'{val}%', 
                 ha='center', va='bottom', fontsize=9)
    plt.tight_layout()
    plt.savefig('charts/prevalence_overall.png', dpi=150)
    plt.close()
    print("✅ สร้างกราฟ prevalence_overall.png แล้ว")
    
    # กราฟ heatmap เปรียบเทียบตาม stratum
    plt.figure(figsize=(14, 8))
    sns.heatmap(comparison.fillna(0), annot=True, fmt='.1f', cmap='YlOrRd', cbar_kws={'label': 'Prevalence (%)'})
    plt.title('Prevalence ของ Dark Patterns แยกตาม Stratum (%)')
    plt.tight_layout()
    plt.savefig('charts/prevalence_by_stratum.png', dpi=150)
    plt.close()
    print("✅ สร้างกราฟ prevalence_by_stratum.png แล้ว")
    
    # กราฟ tracking scripts
    top_scripts = script_counter.most_common(15)
    if top_scripts:
        plt.figure(figsize=(12, 6))
        scripts, counts = zip(*top_scripts)
        plt.barh(scripts, counts, color='lightcoral')
        plt.xlabel('จำนวนครั้งที่พบ')
        plt.title('Tracking Scripts ที่พบบ่อยที่สุด 15 อันดับ')
        plt.tight_layout()
        plt.savefig('charts/top_tracking_scripts.png', dpi=150)
        plt.close()
        print("✅ สร้างกราฟ top_tracking_scripts.png แล้ว")
        
except Exception as e:
    print(f"⚠️ ไม่สามารถสร้างกราฟได้: {e}")

# ================== 7. บันทึกผลลัพธ์ลง Excel ==================
try:
    with pd.ExcelWriter('dp_analysis_results.xlsx', engine='openpyxl') as writer:
        if not overall.empty:
            overall.to_excel(writer, sheet_name='Overall', index=False)
        if not comparison.empty:
            comparison.to_excel(writer, sheet_name='By_Stratum')
        if not sig_df.empty:
            sig_df.to_excel(writer, sheet_name='Significance', index=False)
        
        # บันทึก tracking scripts
        if script_counter:
            script_df = pd.DataFrame(script_counter.most_common(), columns=['Script', 'Count'])
            script_df.to_excel(writer, sheet_name='Tracking_Scripts', index=False)
        
        # บันทึกข้อมูลดิบเฉพาะคอลัมน์สำคัญ
        cols_to_save = ['domain', 'stratum'] + [c for c in dp_columns if c in df_ok.columns] + ['notes']
        cols_exist = [c for c in cols_to_save if c in df_ok.columns]
        df_ok[cols_exist].to_excel(writer, sheet_name='Raw_Data', index=False)
    
    print("\n" + "="*50)
    print("✅ วิเคราะห์เสร็จสมบูรณ์!")
    print(f"📁 ผลลัพธ์ถูกบันทึกใน 'dp_analysis_results.xlsx'")
    print(f"📁 กราฟถูกบันทึกในโฟลเดอร์ 'charts/'")
    print("="*50)
    
except Exception as e:
    print(f"❌ ไม่สามารถบันทึกไฟล์ Excel ได้: {e}")