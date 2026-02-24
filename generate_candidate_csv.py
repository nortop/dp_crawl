import csv
import os

def norm_domain(x: str) -> str:
    x = x.strip().lower()
    x = x.replace("https://", "").replace("http://", "")
    x = x.split("/")[0]           # drop any path
    if x.startswith("www."):
        x = x[4:]
    return x

def uniq_keep_order(items):
    seen = set()
    out = []
    for it in items:
        it = norm_domain(it)
        if it and it not in seen:
            out.append(it)
            seen.add(it)
    return out

# 8 strata × 50 = 400
STRATA = {
    "GOV": [
        "opm.go.th","mdes.go.th","moph.go.th","mof.go.th","mfa.go.th","moi.go.th","moc.go.th","mot.go.th","moac.go.th","moe.go.th",
        "m-society.go.th","justice.go.th","mnre.go.th","industry.go.th","m-culture.go.th","labour.go.th",
        "rd.go.th","customs.go.th","excise.go.th","dlt.go.th","dopa.go.th","dbd.go.th","dsi.go.th","prd.go.th","ocsc.go.th",
        "parliament.go.th","senate.go.th","nacc.go.th","nbtc.go.th",
        "etda.or.th","depa.or.th","dga.or.th",
        "tmd.go.th","nso.go.th","ddpm.go.th","doeb.go.th","doae.go.th","dld.go.th","fisheries.go.th","opdc.go.th","boi.go.th",
        "ratchakitcha.soc.go.th","oic.or.th",
        # MOPH ecosystem (subdomains often behave like distinct sites re: banner/CMP)
        "dms.moph.go.th","ddc.moph.go.th","dtam.moph.go.th","dmsc.moph.go.th","hss.moph.go.th","anamai.moph.go.th","fda.moph.go.th",
    ],

    "EDU": [
        "chula.ac.th","mahidol.ac.th","ku.ac.th","tu.ac.th","cmu.ac.th","kku.ac.th","psu.ac.th","kmitl.ac.th","kmutt.ac.th","kmutnb.ac.th",
        "su.ac.th","swu.ac.th","nida.ac.th","burapha.ac.th","nu.ac.th","wu.ac.th","mju.ac.th","msu.ac.th","ubu.ac.th","payap.ac.th",
        "utcc.ac.th","tni.ac.th","pim.ac.th","bu.ac.th","au.edu","rangsit.edu","dpu.ac.th","stamford.edu","ru.ac.th","mcu.ac.th",
        "mfu.ac.th","sut.ac.th",
        # RMUT network
        "rmutl.ac.th","rmuti.ac.th","rmutp.ac.th","rmutk.ac.th","rmutr.ac.th","rmutto.ac.th","rmutb.ac.th","rmutst.ac.th",
        # Rajabhat network (selection)
        "ssru.ac.th","sdru.ac.th","nrru.ac.th","snru.ac.th","nstru.ac.th","pbru.ac.th","ubru.ac.th","yru.ac.th","crru.ac.th","lpru.ac.th",
    ],

    "FIN": [
        # Banks
        "scb.co.th","scbx.com","kbank.co.th","bangkokbank.com","krungthai.com","krungsri.com","ttb.co.th","uob.co.th","cimbthai.com","tisco.co.th",
        "kkpfg.com","lhbank.co.th","icbc.co.th","standardchartered.co.th","thaicreditbank.com",
        # Specialized FI
        "baac.or.th","gsb.or.th","exim.go.th","ghbank.co.th","smebank.co.th",
        # Markets
        "set.or.th","settrade.com","tfex.co.th","aimc.or.th","thaibma.or.th",
        # Brokers / AMCs
        "bualuang.co.th","phillip.co.th","yuanta.co.th","finansia.com","asiawealth.co.th","innovestx.co.th",
        "scbam.com","ktam.co.th","bblam.co.th","principal.co.th","eastspring.co.th","uobam.co.th",
        # Insurance
        "aia.co.th","muangthai.co.th","thai-life.com","krungthai-axa.co.th","allianz.co.th","generali.co.th",
        "bangkokinsurance.com","viriyah.co.th","dhipaya.co.th","tqm.co.th","tipinsure.com","thaiins.com","tli.co.th",
    ],

    "HEALTH": [
        # Hospitals / groups
        "bumrungrad.com","bangkokhospital.com","samitivejhospitals.com","phyathai.com","bnhhospital.com","vibhavadi.com","praram9.com","paolohospital.com","thonburihospital.com","medparkhospital.com",
        "vejthani.com","rutnin.com","yanhee.net","missionhospital.org","camillianhospital.org","saintlouis.or.th","sikarin.com","nakornthon.com","piyavate.com","wattanosoth.com",
        "bangpakokhospital.com","chularat.com","kasemrad.co.th","bch.in.th","bdms.co.th","thg.co.th",
        # Public/teaching hospitals
        "rajavithi.go.th","sirirajhospital.org","rama.mahidol.ac.th","ramathibodi.mahidol.ac.th","med.cmu.ac.th","med.psu.ac.th","med.kku.ac.th","med.tu.ac.th",
        "sirirajpiyamaharajkarun.com","theptarin.com","ramkhamhaeng.co.th","bangmodhospital.com","synphaet.co.th","thainakarin.co.th",
        # Councils / system orgs
        "pharmacycouncil.org","thaimedicalcouncil.org","ha.or.th","hsri.or.th","gpo.or.th",
        # Health retail / platforms
        "boots.co.th","watsons.co.th","fascino.co.th","hdmall.co.th","doctorraksa.com",
    ],

    "TELCO_UTIL": [
        # Telco / ISP
        "ais.co.th","ais.th","dtac.co.th","truecorp.co.th","trueinternet.co.th","truevisions.co.th","3bb.co.th","ntplc.co.th","tot.co.th","cattelecom.com",
        "inet.co.th","sinet.co.th","csloxinfo.com","uih.co.th","jasmine.com","samarttel.com","thaicom.net","aisfibre3.com",
        # Utilities (electric/water/energy)
        "pea.co.th","mea.or.th","egat.co.th","erc.or.th","pwa.co.th","mwa.co.th",
        "pttplc.com","pttep.com","bgrimm.com","gulf.co.th","ea.co.th","banpu.com","gpscgroup.com","ratch.co.th","egco.com","bcpg.co.th","irpc.co.th",
        # Transport / public utilities operators
        "bts.co.th","btsgroup.co.th","bemplc.co.th","mrta.co.th","bmta.co.th","railway.co.th","port.co.th","expressway.co.th","tollway.co.th","dmt.co.th",
        # (optional measurement sites used by Thai users)
        "speedtest.net","fast.com",
    ],

    "NEWS_MEDIA": [
        "thairath.co.th","matichon.co.th","khaosod.co.th","dailynews.co.th","bangkokpost.com","nationthailand.com","prachachat.net","thansettakij.com","mgronline.com","siamrath.co.th",
        "komchadluek.net","naewna.com","thaipost.net","posttoday.com","thethaiger.com","thaipbs.or.th","pptvhd36.com","amarintv.com","tnnthailand.com","workpointtoday.com",
        "workpointtv.com","ch3thailand.com","ch7.com","one31.net","mononews.com","tna.mcot.net","mcot.net","springnews.co.th","voicetv.co.th","thestandard.co",
        "today.line.me","sanook.com","kapook.com","trueid.net","beartai.com","dek-d.com","pantip.com","topnews.co.th","isranews.org","bangkokbiznews.com",
        "brandinside.asia","techsauce.co","droidsans.com","thisrupt.co","themomentum.co","thestandardwealth.com","siamzone.com","thaich8.com","news1005.fm","innews.news",
    ],

    "ECOM_RETAIL": [
        "shopee.co.th","lazada.co.th","jd.co.th","central.co.th","powerbuy.co.th","tops.co.th","bigc.co.th","lotuss.com","makro.pro","homepro.co.th",
        "globalhouse.co.th","thaiwatsadu.com","indexlivingmall.com","sbdesignsquare.com","bananait.co.th","advice.co.th","itcityonline.com","nocnoc.com","konvy.com","jib.co.th",
        "officemate.co.th","b2s.co.th","kingpower.com","allonline.7eleven.co.th","7eleven.co.th",
        # Food / quick commerce
        "foodpanda.co.th","grab.com","lineman.wongnai.com","wongnai.com","robinhood.co.th","gojek.com","hungryhub.com","makroclick.com",
        # Marketplaces / price compare / classifieds
        "kaidee.com","tarad.com","priceza.com",
        # Fashion / cross-border (Thai users)
        "pomelofashion.com","aliexpress.com","amazon.com","ebay.com","etsy.com",
        # Extra Thai retail
        "supersports.co.th","centralgroup.com","decathlon.co.th","ikea.com/th","sephora.co.th","unilever.co.th","nestle.co.th","apple.com/th",
    ],

    "TRAVEL_SERVICES": [
        # Official tourism / airports / transport
        "tourismthailand.org","airportthai.co.th","suvarnabhumiairport.com","donmueangairport.com","transport.co.th","railway.co.th",
        # Airlines (strong TH footprint)
        "thaiairways.com","bangkokair.com","nokair.com","airasia.com","lionairthai.com","vietjetair.com",
        "qatarairways.com","singaporeair.com","emirates.com","cathaypacific.com",
        # OTAs / booking / travel tech
        "agoda.com","booking.com","trip.com","traveloka.com","klook.com","airbnb.com","skyscanner.net","expedia.com","getyourguide.com","flightradar24.com",
        # Hotel groups
        "centarahotelsresorts.com","dusit.com","minorhotels.com","onyx-hospitality.com","anantara.com","avani.com","marriott.com","hilton.com","ihg.com","accor.com",
        # Ferries / local travel services
        "lomprayah.com","seatranferry.com","phuketferry.com","12go.asia","thairentacar.com","avisthailand.com","budget.co.th","airportels.asia",
    ],
}

# Config
MIN_PER_STRAT = int(os.environ.get("MIN_DOMAINS_PER_STRAT", "50"))
# Set ALLOW_SHORT_STRAT=1 (or true/yes) to continue even when a stratum is short
ALLOW_SHORT_STRAT = os.environ.get("ALLOW_SHORT_STRAT", "").lower() in ("1", "true", "yes")

# Build rows
rows = []
for stratum, domains in STRATA.items():
    u = uniq_keep_order(domains)
    if len(u) < MIN_PER_STRAT:
        msg = f"[WARN] Stratum {stratum} has only {len(u)} unique domains (< {MIN_PER_STRAT})."
        if ALLOW_SHORT_STRAT:
            print(msg + " Continuing due to ALLOW_SHORT_STRAT.")
        else:
            print(msg + " Using available domains and continuing.")
    for i, d in enumerate(u[:MIN_PER_STRAT], start=1):
        rows.append({
            "domain": d,
            "stratum": stratum,
            "source": "seed_list",
            "source_rank": ""  # keep empty unless you want to store rank from a particular list
        })

# Write CSV
out_file = "candidate.csv"
with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["domain","stratum","source","source_rank"])
    w.writeheader()
    w.writerows(rows)

# Summary
from collections import Counter
cnt = Counter([r["stratum"] for r in rows])
print("Wrote:", out_file)
print("Counts:", dict(sorted(cnt.items())))
print("Total:", len(rows))
