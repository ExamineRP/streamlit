import sys
import traceback
from datetime import datetime, timedelta
import pandas as pd
from sftpcom import run_sftp_download
from CA import run_ca_analysis


def get_last_business_day():
    """어제 영업일 반환"""
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

def get_recent_business_day():
    """가장 최근 영업일 -1 반환"""
    from pandas.tseries.offsets import BDay
    today = datetime.now()
    recent_bizday = today - BDay(1)
    return recent_bizday.strftime('%Y-%m-%d')


def run_process(start_date=None, end_date=None, fund_code=None, show_details=False):
    """
    SFTP 데이터 다운로드 및 CA 분석 실행
    
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
    
    print(f"\n{'='*80}")
    print(f"[PROCESS] SFTP 다운로드 및 CA 분석 시작 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    # 날짜 설정
    if start_date is None:
        start_date = get_last_business_day()
    if end_date is None:
        end_date = get_recent_business_day()
    
    try:
        # Step 1: 일반 SFTP 다운로드
        print(f"\n[STEP 1] 일반 SFTP 다운로드 시작...")
        run_sftp_download(
            start_date=start_date,
            end_date=end_date,
            fund_code=fund_code,
            is_ca=False
        )
        print(f"[STEP 1] 일반 SFTP 다운로드 완료")
        
        # Step 2: CA SFTP 다운로드
        print(f"\n[STEP 2] CA SFTP 다운로드 시작...")
        run_sftp_download(
            start_date=start_date,
            end_date=end_date,
            fund_code=fund_code,
            is_ca=True
        )
        print(f"[STEP 2] CA SFTP 다운로드 완료")
        
        # Step 3: CA 분석
        print(f"\n[STEP 3] CA 분석 시작...")
        run_ca_analysis(
            start_date=start_date,
            end_date=end_date,
            fund_code=fund_code,
            show_details=show_details
        )
        print(f"[STEP 3] CA 분석 완료")
        
        print(f"\n{'='*80}")
        print(f"[SUCCESS] 전체 프로세스 완료 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"[ERROR] 프로세스 실행 중 오류 발생: {e}")
        print(f"[ERROR] 오류 상세: {traceback.format_exc()}")
        print(f"{'='*80}\n")
        raise e


if __name__ == "__main__":
    run_process(start_date="2025-06-30", end_date=None, fund_code=["SP500", 'SX5E'], show_details=["SP500"])  # ["SP500", 'SPEHYDUP', 'NDX', 'SX5E']