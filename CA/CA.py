import os
from datetime import datetime
import pandas as pd

def preprocess_spx_data(df):
    df_v1 = df[['LAST UPDATED DATE', 
                'CURRENT COMPANY NAME', 
                'CURRENT BLOOMBERG TICKER', 
                'ACTION TYPE', 
                'STATUS', 
                'COMMENTS']]

    df_v1 = df_v1.rename(columns={'LAST UPDATED DATE': 'Date', 
                                  'CURRENT COMPANY NAME' : 'Name', 
                                  'CURRENT BLOOMBERG TICKER': 'Ticker',
                                  'ACTION TYPE': 'Action_type',
                                  'STATUS': 'Status',
                                  'COMMENTS': 'Comments'})

    df_v1 = df_v1[(df_v1.Status == 'Pending') & (df_v1.Action_type != 'Dividend')].reset_index(drop=True)
    df_v1['Date'] = pd.to_datetime(df_v1['Date'], format='%Y%m%d')
    
    return df_v1

def preprocess_ndx_data(df):
    df_v1 = df[['Effective Date',
                'Issue Name',
                'Issue Symbol',
                'Action Description',
                'Status']]
    
    df_v1 = df_v1.rename(columns={'Effective Date': 'Date',
                                  'Issue Name': 'Name',
                                  'Issue Symbol': 'Ticker',
                                  'Action Description': 'Action_type',
                                  'Status': 'Status'})

    df_v1 = df_v1[(df_v1.Status == 'PE') & (df_v1.Action_type != 'Cash Dividend')].reset_index(drop=True)
    df_v1['Date'] = pd.to_datetime(df_v1['Date'], format='%Y-%m-%d')

    return df_v1

def preprocess_sx5e_data(df):
    df_v1 = df[['Date_Effective',
                'Company_Name',
                'ISIN',
                'Corporate_Action_Type',
                'Comment']]
    
    df_v1 = df_v1.rename(columns={'Date_Effective': 'Date',
                                  'Company_Name': 'Name',
                                  'ISIN': 'ISIN',
                                  'Corporate_Action_Type': 'Action_type',
                                  'Comment': 'Comments'})
    
    df_v1 = df_v1[(df_v1.Action_type != 'Cash Dividend')].reset_index(drop=True)
    df_v1['Date'] = pd.to_datetime(df_v1['Date'], format='%Y%m%d')
    
    return df_v1

def read_excel_sheet(path, file_name, sheet_name, preprocess_func):
    try:
        raw_latest = pd.read_excel(path + file_name, 
                                   sheet_name = sheet_name, 
                                   skiprows = 1 if sheet_name == 'NDX' else None)
        
        processed_latest = preprocess_func(raw_latest)
        return processed_latest
    
    except ValueError as e:
        print(f"Error processing data from sheet '{sheet_name}' in file '{file_name}': {e}")
        return None
    except KeyError as e:
        print(f"Column not found in sheet '{sheet_name}' in file '{file_name}': {e}")
        print(f"Available columns: {list(raw_latest.columns)}")
        return None

def get_last_business_day(reference_date=None):
    if reference_date is None:
        reference_date = pd.Timestamp.today()
    # 평일(월~금)만 영업일로 간주
    last_bd = pd.bdate_range(end=reference_date, periods=2)[0]
    return last_bd

def read_CA_excel(start_date=None):  
    if start_date is None:
        last_bizday = get_last_business_day()
    else:
        last_bizday = datetime.strptime(start_date, '%Y-%m-%d')
    
    path = f'\\\\10.206.101.81\\09_idx\\해외인덱스팀\\운용관리\\CA\\{last_bizday.year}\\{last_bizday.strftime("%b")}\\'
    
    try:
        files = os.listdir(path)
        # 임시 Excel 파일(~$) 제외
        files = [f for f in files if not f.startswith('~$')]
        files.sort(reverse=True)
        
        if len(files) < 2:
            print(f"[ERROR] 파일이 부족합니다. 최소 2개 필요, 현재 {len(files)}개")
            return None, None, None, None, None, None, None, None

        # latest : T (7/1) / Previous : T-1 (6/30) 
        latest_file = files[0]             # 가장 최신 파일 (7/1)
        previous_day_file = files[1]       # 두 번째 파일 (6/30)

        # SP500
        sp500_latest = read_excel_sheet(path, latest_file, 'SP500', preprocess_spx_data)
        sp500_previous = read_excel_sheet(path, previous_day_file, 'SP500', preprocess_spx_data)

        # SPY
        sp500_div_latest = read_excel_sheet(path, latest_file, 'SPY', preprocess_spx_data)
        sp500_div_previous = read_excel_sheet(path, previous_day_file, 'SPY', preprocess_spx_data)

        # NDX
        nasdaq_latest = read_excel_sheet(path, latest_file, 'NDX', preprocess_ndx_data)
        nasdaq_previous = read_excel_sheet(path, previous_day_file, 'NDX', preprocess_ndx_data)
        
        # SX5E
        sx5e_latest = read_excel_sheet(path, latest_file, 'SX5E', preprocess_sx5e_data)
        sx5e_previous = read_excel_sheet(path, previous_day_file, 'SX5E', preprocess_sx5e_data)
        
        return sp500_latest, sp500_previous, sp500_div_latest, sp500_div_previous, nasdaq_latest, nasdaq_previous, sx5e_latest, sx5e_previous
        
    except Exception as e:
        print(f"[ERROR] read_CA_excel 오류: {e}")
        return None, None, None, None, None, None, None, None

def compare_spx_changes(df_latest, df_previous):
    if df_latest is None or df_previous is None:
        print("[S&P 500]")
        print("[ERROR] 데이터를 읽을 수 없습니다.")
        print()
        return
        
    latest_tickers = set(df_latest['Ticker'])
    previous_tickers = set(df_previous['Ticker'])

    changed = latest_tickers - previous_tickers

    print("[S&P 500]")
    if changed:
        ticker_info = df_latest[df_latest['Ticker'].isin(changed)][['Ticker', 'Name', 'Action_type']].drop_duplicates()
        print("-" * 65)
        print(f"{'Ticker':<12} {'Company Name':<30} {'Action Type':<20}")
        print("-" * 65)
        
        # 모든 데이터를 한 번에 출력
        output_lines = []
        for _, row in ticker_info.iterrows():
            line = f"{row['Ticker']:<12} {row['Name']:<30} {row['Action_type']:<20}"
            output_lines.append(line)
        
        print('\n'.join(output_lines))
        print(f"\n총 {len(output_lines)}개의 Corporate Action이 발견되었습니다.")
    else:
        print("[OK] 이상 없음")
    print()

def compare_spy_changes(df_latest, df_previous):
    if df_latest is None or df_previous is None:
        print("[SPEHY]")
        print("[ERROR] 데이터를 읽을 수 없습니다.")
        print()
        return
        
    latest_tickers = set(df_latest['Ticker'])
    previous_tickers = set(df_previous['Ticker'])

    changed = latest_tickers - previous_tickers

    print("[SPEHY]")
    if changed:
        ticker_info = df_latest[df_latest['Ticker'].isin(changed)][['Ticker', 'Name', 'Action_type']].drop_duplicates()
        print("-" * 65)
        print(f"{'Ticker':<12} {'Company Name':<30} {'Action Type':<20}")
        print("-" * 65)
        
        # 모든 데이터를 한 번에 출력
        output_lines = []
        for _, row in ticker_info.iterrows():
            line = f"{row['Ticker']:<12} {row['Name']:<30} {row['Action_type']:<20}"
            output_lines.append(line)
        
        print('\n'.join(output_lines))
        print(f"\n총 {len(output_lines)}개의 Corporate Action이 발견되었습니다.")
    else:
        print("[OK] 이상 없음")
    print()

def compare_ndx_changes(df_latest, df_previous):
    if df_latest is None or df_previous is None:
        print("[NASDAQ 100]")
        print("[ERROR] 데이터를 읽을 수 없습니다.")
        print()
        return
        
    latest_tickers = set(df_latest['Ticker'])
    previous_tickers = set(df_previous['Ticker'])

    changed = latest_tickers - previous_tickers

    print("[NASDAQ 100]")
    if changed:
        ticker_info = df_latest[df_latest['Ticker'].isin(changed)][['Ticker', 'Name', 'Action_type']].drop_duplicates()
        print("-" * 65)
        print(f"{'Ticker':<12} {'Company Name':<30} {'Action Type':<20}")
        print("-" * 65)
        
        # 모든 데이터를 한 번에 출력
        output_lines = []
        for _, row in ticker_info.iterrows():
            line = f"{row['Ticker']:<12} {row['Name']:<30} {row['Action_type']:<20}"
            output_lines.append(line)
        
        print('\n'.join(output_lines))
        print(f"\n총 {len(output_lines)}개의 Corporate Action이 발견되었습니다.")
    else:
        print("[OK] 이상 없음")
    print()

def compare_sx5e_changes(df_latest, df_previous):
    if df_latest is None or df_previous is None:
        print("[SX5E]")
        print("[ERROR] 데이터를 읽을 수 없습니다.")
        print()
        return
        
    latest_isins = set(df_latest['ISIN'])
    previous_isins = set(df_previous['ISIN'])

    changed = latest_isins - previous_isins

    print("[SX5E]")
    if changed:
        ticker_info = df_latest[df_latest['ISIN'].isin(changed)][['ISIN', 'Name', 'Action_type']].drop_duplicates()
        print("-" * 65)
        print(f"{'ISIN':<12} {'Company Name':<30} {'Action Type':<20}")
        print("-" * 65)
        
        output_lines = []
        for _, row in ticker_info.iterrows():
            line = f"{row['ISIN']:<12} {row['Name']:<30} {row['Action_type']:<20}"
            output_lines.append(line)
        
        print('\n'.join(output_lines))
        print(f"\n총 {len(output_lines)}개의 Corporate Action이 발견되었습니다.")
    else:
        print("[OK] 이상 없음")
    print()
    
def run_ca_analysis(start_date=None, end_date=None, fund_code=None, show_details=False):
    """CA 분석을 실행하는 메인 함수
    
    Parameters:
    -----------
    start_date : str, optional
        시작 날짜 (YYYY-MM-DD 형식). None이면 어제 영업일
    end_date : str, optional
        종료 날짜 (YYYY-MM-DD 형식). None이면 start_date와 동일
    fund_code : str or list, optional
        처리할 펀드 코드. None이면 전체
    show_details : bool or list, optional
        세부 종목 CA 내역 출력 여부. 
        - True: 모든 펀드의 CA 내역 출력
        - False: 변경사항만 출력
        - ['SP500', 'NDX']: SP500과 NDX만 세부내역 출력
    """
    
    try:
        sp500_latest, sp500_previous, sp500_div_latest, sp500_div_previous, nasdaq_latest, nasdaq_previous, sx5e_latest, sx5e_previous = read_CA_excel(start_date)
        
        # fund_code에 따라 선택적으로 실행
        if fund_code is None or (isinstance(fund_code, list) and 'SP500' in fund_code) or fund_code == 'SP500':
            # SP500 CA 내역 출력
            if show_details and sp500_latest is not None:
                print(f"\n[SP500 CA 내역 - {start_date if start_date else '최근'}]")
                print("=" * 80)
                if len(sp500_latest) > 0:
                    print(f"총 {len(sp500_latest)}개의 SP500 CA 데이터")
                    print("-" * 80)
                    print(f"{'Ticker':<12} {'Company Name':<30} {'Action Type':<20}")
                    print("-" * 80)
                    for _, row in sp500_latest.iterrows():
                        print(f"{row['Ticker']:<12} {row['Name']:<30} {row['Action_type']:<20}")
                else:
                    print("SP500 CA 데이터가 없습니다.")
                print()
            
            compare_spx_changes(sp500_latest, sp500_previous)
        
        if fund_code is None or (isinstance(fund_code, list) and 'SPEHYDUP' in fund_code) or fund_code == 'SPEHYDUP':
            # SPEHY CA 내역 출력
            if show_details and sp500_div_latest is not None:
                print(f"\n[SPEHY CA 내역 - {start_date if start_date else '최근'}]")
                print("=" * 80)
                if len(sp500_div_latest) > 0:
                    print(f"총 {len(sp500_div_latest)}개의 SPEHY CA 데이터")
                    print("-" * 80)
                    print(f"{'Ticker':<12} {'Company Name':<30} {'Action Type':<20}")
                    print("-" * 80)
                    for _, row in sp500_div_latest.iterrows():
                        print(f"{row['Ticker']:<12} {row['Name']:<30} {row['Action_type']:<20}")
                else:
                    print("SPEHY CA 데이터가 없습니다.")
                print()
            
            compare_spy_changes(sp500_div_latest, sp500_div_previous)
        
        if fund_code is None or (isinstance(fund_code, list) and 'NDX' in fund_code) or fund_code == 'NDX':
            # NDX CA 내역 출력
            if show_details and nasdaq_latest is not None:
                print(f"\n[NDX CA 내역 - {start_date if start_date else '최근'}]")
                print("=" * 80)
                if len(nasdaq_latest) > 0:
                    print(f"총 {len(nasdaq_latest)}개의 NDX CA 데이터")
                    print("-" * 80)
                    print(f"{'Ticker':<12} {'Company Name':<30} {'Action Type':<20}")
                    print("-" * 80)
                    for _, row in nasdaq_latest.iterrows():
                        print(f"{row['Ticker']:<12} {row['Name']:<30} {row['Action_type']:<20}")
                else:
                    print("NDX CA 데이터가 없습니다.")
                print()
            
            compare_ndx_changes(nasdaq_latest, nasdaq_previous)
        
        if fund_code is None or (isinstance(fund_code, list) and 'SX5E' in fund_code) or fund_code == 'SX5E':
            # SX5E CA 내역 출력
            if show_details and sx5e_latest is not None:
                print(f"\n[SX5E CA 내역 - {start_date if start_date else '최근'}]")
                print("=" * 80)
                if len(sx5e_latest) > 0:
                    print(f"총 {len(sx5e_latest)}개의 SX5E CA 데이터")
                    print("-" * 80)
                    print(f"{'ISIN':<12} {'Company Name':<30} {'Action Type':<20}")
                    print("-" * 80)
                    for _, row in sx5e_latest.iterrows():
                        print(f"{row['ISIN']:<12} {row['Name']:<30} {row['Action_type']:<20}")
                else:
                    print("SX5E CA 데이터가 없습니다.")
                print()
            
            compare_sx5e_changes(sx5e_latest, sx5e_previous)
        

        return sp500_latest, sp500_previous, sp500_div_latest, sp500_div_previous, nasdaq_latest, nasdaq_previous, sx5e_latest, sx5e_previous
    except Exception as e:
        print(f"[ERROR] 오류 발생: {e}")
        print(f"{'='*60}\n")
        return None
    