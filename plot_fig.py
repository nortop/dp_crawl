import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from sklearn.metrics import pairwise_distances

# ------------------------------------------------------------
# Figure: Top Tracking Scripts
# ------------------------------------------------------------
# Data from manual validation (as provided)
tracking_data = {
    'Google Ads': 262,
    'Facebook': 154,
    'Google Analytics': 143,
    'Cloudflare': 74,
    'Microsoft': 50,
    'ByteDance/TikTok': 42,  # รวม ByteDance (29) + TikTok (13)
    'Twitter': 20,
    'RTB House': 20,
    'Criteo': 19,
    'Taboola': 12,
    'OneTrust': 10,
    'Line': 10,
    'Braze': 9,
    'Akamai': 9,
    'Hotjar': 9,
    'Trade Desk': 8,
    'Amazon': 8,
    'GMPG': 7
}

# Sort by frequency descending
tracking_sorted = dict(sorted(tracking_data.items(), key=lambda x: x[1], reverse=True))

plt.figure(figsize=(10, 6))
bars = plt.bar(tracking_sorted.keys(), tracking_sorted.values(), color='steelblue', alpha=0.8)
plt.xticks(rotation=45, ha='right')
plt.ylabel('Number of websites')
plt.title('Top Tracking Scripts Observed (Manual Validation, n=390)')
plt.grid(axis='y', linestyle='--', alpha=0.3)

# Add value labels on bars
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 5,
             f'{int(height)}', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('top_tracking_scripts.pdf', dpi=300, bbox_inches='tight')
plt.savefig('top_tracking_scripts.png', dpi=300, bbox_inches='tight')
plt.show()

# ------------------------------------------------------------
# Figure: Co-occurrence Heatmap (Jaccard Indices)
# ------------------------------------------------------------
# Assuming you have a CSV file 'manual_validation_flags.csv' with columns:
# domain, dp1, dp2, dp3, dp4, dp5, dp6, dp7, dp8, dp9, dp10, dp11, dp12
# (each value 0/1)

# If you don't have the file, you can create a sample from the prevalence data,
# but for real use, replace with your actual file.
df = pd.read_csv('manual_10032026-14032026.csv', encoding='utf-8-sig')

# Select only DP columns
dp_cols = ['dp1_flag', 'dp2_flag', 'dp3_flag', 'dp4_flag', 'dp5_flag',
              'dp6_flag', 'dp7_flag', 'dp8_flag', 'dp9', 'dp10', 'dp3_flag_adv / dp11', 'dp12']
dp_names = ['DP1','DP2','DP3','DP4','DP5','DP6','DP7','DP8','DP9','DP10','DP11','DP12']

# Convert to binary matrix
X = df[dp_cols].values

# Compute Jaccard distance -> similarity
jaccard_dist = pairwise_distances(X.T, metric='jaccard')  # distance = 1 - similarity
jaccard_sim = 1 - jaccard_dist

# Create DataFrame for heatmap
jaccard_df = pd.DataFrame(jaccard_sim, index=dp_names, columns=dp_names)

# Plot heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(jaccard_df, annot=True, fmt='.2f', cmap='YlOrRd', 
            vmin=0, vmax=1, square=True, cbar_kws={'label': 'Jaccard Index'})
plt.title('Co-occurrence of Dark Patterns (Jaccard Index) – Manual Validation')
plt.tight_layout()
plt.savefig('cooccurrence_heatmap.pdf', dpi=300, bbox_inches='tight')
plt.savefig('cooccurrence_heatmap.png', dpi=300, bbox_inches='tight')
plt.show()

