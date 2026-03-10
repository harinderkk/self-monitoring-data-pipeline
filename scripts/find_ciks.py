import requests
import time

headers = {'User-Agent': 'yourname@email.com'}

CANADIAN_COMPANIES = {
    'Royal Bank of Canada':      '0001000275',
    'Toronto Dominion Bank':     '0000947263',
    'CIBC':                      '0001045520',
    'Suncor Energy':             '0000311337',
    'Canadian National Railway': '0001232384',
    'Teck Resources':            '0000886986',
    'BCE Inc':                   '0000718940',
    'Sun Life Financial':        '0001097362',
}

for company, cik in CANADIAN_COMPANIES.items():
    r = requests.get(
        f'https://data.sec.gov/submissions/CIK{cik}.json',
        headers=headers
    )
    data = r.json()
    
    forms = data['filings']['recent']['form']
    dates = data['filings']['recent']['filingDate']
    
    annual_forms = ['40-F', '10-K', '20-F']
    annual_filings = [
        (date, form) for form, date in zip(forms, dates)
        if any(af in form for af in annual_forms)
    ]
    
    print(f'{company} (CIK {cik})')
    print(f'  Registered name: {data.get("name")}')
    print(f'  Annual filings found: {len(annual_filings)}')
    if annual_filings:
        print(f'  Most recent: {annual_filings[0][0]} ({annual_filings[0][1]})')
    print()
    
    time.sleep(0.1)