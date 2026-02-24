🕵️ Cookie Banner Dark Pattern Crawler (DP3 Differential)
Large-scale automated measurement of cookie banners and dark patterns with pre-consission tracking detection (DP3 Differential Model).

🚀 What This Project Does
This crawler automatically:
* Detects cookie banners
* Measures UI asymmetry (Accept vs Reject)
* Identifies multiple dark pattern types (DP1, DP2, DP7)
* Detects pre-consission tracking using a differential model (DP3)
* Captures screenshots as evidence
* Outputs structured CSV for analysis
Designed for:
* Dark pattern research
* GDPR / PDPA compliance audits
* Consent Management Platform (CMP) evaluation
* Large-scale web measurement studies


🧠 Core Idea: DP3 Differential Model
Traditional crawlers often produce false positives when detecting tracking.
This project uses isolated browser contexts to compare tracking behavior across states:
State	Description
S0	Fresh visit (no interaction)
S1	Explicit Reject All
S2	Explicit Accept All (optional)

DP3 is triggered when:
* Tracking exists in S0 (before consent)
* Tracking decreases after Reject (S1)
* This reduces noise from lazy-loading and script timing.

📦 Installation
Requirements
* Node.js ≥ 18
* Playwright

npm install
npx playwright install

📁 Evidence Structure
evidence/
  domain/
    desktop/
      run1_banner.png
      run1_manage.png
Screenshots are captured before interaction and after navigation to settings.

📄 Example Input