import os
import time
import requests
import pandas as pd


def download_pdf(session, accession, out_dir="ler"):
    url = f"https://adamswebsearch2.nrc.gov/webSearch2/main.jsp?AccessionNumber={accession}"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf,*/*",
    }

    try:
        res = session.get(url, headers=headers, timeout=30, allow_redirects=True)

        content_type = res.headers.get("Content-Type", "").lower()

        # Check PDF by file signature or content-type
        is_pdf = res.content[:4] == b"%PDF" or "application/pdf" in content_type

        if not is_pdf:
            print(f"[WARN] Not a PDF: {accession}")
            print(f"       status={res.status_code}, content-type={content_type}")
            print(f"       preview={res.text[:120]}")
            return

        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{accession}.pdf")

        with open(path, "wb") as f:
            f.write(res.content)

        print(f"[OK] {accession}")

    except Exception as e:
        print(f"[ERROR] {accession}: {e}")


def main():
    df = pd.read_excel("LERSearchResults.xlsx")
    df.columns = df.columns.str.strip()

    acc_col = [c for c in df.columns if "Accession" in c][0]

    session = requests.Session()

    for i, row in df.iterrows():
        accession = str(row[acc_col]).strip()

        if accession == "" or accession.lower() == "nan":
            continue

        print(f"[{i+1}/{len(df)}] {accession}")
        download_pdf(session, accession, out_dir="ler")

        time.sleep(1)


if __name__ == "__main__":
    main()