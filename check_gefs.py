import s3fs

f = s3fs.S3FileSystem(anon=True)
P = "noaa-gefs-pds/gefs.20260427/12/atmos/"

for s in ["pgrb2ap5", "pgrb2bp5", "pgrb2sp25"]:
    pre = "gec00" if s in ("pgrb2bp5", "pgrb2ap5") else "geavg"
    res = "0p25" if s == "pgrb2sp25" else "0p50"
    sfx = {"pgrb2ap5": "pgrb2a", "pgrb2bp5": "pgrb2b", "pgrb2sp25": "pgrb2s"}[s]
    k = f"{P}{s}/{pre}.t12z.{sfx}.{res}.f024.idx"
    try:
        txt = f.open(k, "r").read()
    except Exception as e:
        print(s, "FAIL", e)
        continue
    matches = [l for l in txt.split("\n") if "above ground" in l or "surface" in l]
    print(s, "surface fields:", len(matches))
    print(*matches[:5], sep="\n")
