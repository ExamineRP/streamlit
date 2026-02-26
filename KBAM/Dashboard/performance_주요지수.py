"""
주요 지수 탭 - 지수별 누적 수익률 비교 및 지수별 수익률 비교
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from call import get_major_indices_returns, get_major_indices_raw_data, get_price_major_index_for_comparison
from utils import get_business_day, get_period_dates, get_period_options, get_period_dates_from_base_date


def render():
    """주요 지수 탭 렌더링"""
    # 기간 선택 옵션 및 라벨 가져오기
    period_options, period_labels = get_period_options()
    
    # 각 기간에 대한 날짜 계산
    today = datetime.now().date()
    period_dates = get_period_dates(today)
    
    # 전역 기간 선택 (세션 상태 사용) - 기본은 YTD
    if 'selected_period' not in st.session_state:
        st.session_state.selected_period = 'YTD'
    if 'custom_start_date' not in st.session_state:
        st.session_state.custom_start_date = today - timedelta(days=30)
    if 'custom_end_date' not in st.session_state:
        st.session_state.custom_end_date = today - timedelta(days=1)
    
    # 기간 선택 UI (차트 위쪽에 배치)
    st.markdown("### 📊 지수별 누적 수익률 비교")
    selected_period = st.radio(
        "",
        options=period_options,
        horizontal=True,
        index=period_options.index(st.session_state.selected_period) if st.session_state.selected_period in period_options else 0,
        label_visibility="collapsed",
        key="period_radio"
    )
    
    # 기준일자: 위(최종 수익률·차트)와 아래(지수별 수익률 비교 표)가 동일 수치가 되도록 단일 기준 사용
    if 'comparison_base_date' not in st.session_state:
        st.session_state.comparison_base_date = get_business_day(today, 1)
    comparison_base_date = st.date_input(
        "기준일자",
        value=st.session_state.comparison_base_date,
        max_value=today - timedelta(days=1),
        key="comparison_base_date_input"
    )
    st.session_state.comparison_base_date = comparison_base_date
    
    # 기간 선택이 변경되면 세션 업데이트
    if st.session_state.selected_period != selected_period:
        st.session_state.selected_period = selected_period
        st.rerun()
    
    # price_major_index DB ticker -> 표시명 (지수별 수익률 비교 표와 동일)
    _ticker_to_display = {
        'SPX Index': 'SPX-SPX', 'SPEHYDUP Index': 'SPHYDA-USA', 'SPHYD Index': 'SPHYDA-USA',
        'NDX Index': 'NDX-USA', 'SX5E Index': 'ESX-STX', 'HSCEI Index': 'HSCEI-HKX',
        'NIFTY Index': 'NSENIF-NSE', 'VN30 Index': 'VN30-STC', 'NKY Index': 'NIK-NKX', 'KOSPI Index': 'KOSPI-KRX',
    }
    _db_tickers = list(_ticker_to_display.keys())
    
    try:
        _end_str = comparison_base_date.strftime("%Y-%m-%d")
        _fetch_start = (comparison_base_date - timedelta(days=1200)).strftime("%Y-%m-%d")
        with st.spinner("지수별 수익률 데이터를 조회하는 중..."):
            _price_data = get_price_major_index_for_comparison(
                fetch_start_date=_fetch_start,
                end_date_str=_end_str,
                ticker_list=_db_tickers,
            )
        _comparison_df = pd.DataFrame(_price_data)
        
        if not _comparison_df.empty:
            _comparison_df['dt'] = pd.to_datetime(_comparison_df['dt'])
            _comparison_df['index_name'] = _comparison_df['index_name'].astype(str).str.strip()
            _comparison_df['display_name'] = _comparison_df['index_name'].map(_ticker_to_display)
            _comparison_df = _comparison_df[_comparison_df['display_name'].notna()].copy()
            _available = list(dict.fromkeys(_ticker_to_display[t] for t in _comparison_df['index_name'].unique() if t in _ticker_to_display))
        else:
            _available = []
        
        def _period_bounds(base_date):
            # YTD: 연말(전년 12/31) 종가 ~ 기준일로 통일 (1/1 데이터 유무와 무관하게 27.06% 등 동일 수치)
            ytd_start = base_date.replace(month=1, day=1) - timedelta(days=1)
            return {
                '1D': (base_date - timedelta(days=1), base_date),
                '1W': (base_date - timedelta(days=7), base_date),
                '1M': (base_date - timedelta(days=30), base_date),
                '3M': (base_date - timedelta(days=90), base_date),
                '6M': (base_date - timedelta(days=180), base_date),
                '1Y': (base_date - timedelta(days=365), base_date),
                'MTD': (base_date.replace(day=1), base_date),
                'YTD': (ytd_start, base_date),
            }
        
        def _calc_return(idx_data: pd.DataFrame, start_b: datetime.date, end_b: datetime.date):
            if idx_data.empty:
                return None
            try:
                idx_data = idx_data.copy()
                idx_data['dt'] = pd.to_datetime(idx_data['dt'])
                idx_data['dt_date'] = idx_data['dt'].dt.date
                start_c = idx_data[idx_data['dt_date'] <= start_b]
                start_c = start_c if not start_c.empty else idx_data[idx_data['dt_date'] >= start_b]
                if start_c.empty:
                    return None
                start_row = start_c.iloc[-1] if (idx_data['dt_date'] <= start_b).any() else start_c.iloc[0]
                end_c = idx_data[idx_data['dt_date'] <= end_b]
                end_c = end_c if not end_c.empty else idx_data[idx_data['dt_date'] >= end_b]
                if end_c.empty:
                    return None
                end_row = end_c.iloc[-1] if (idx_data['dt_date'] <= end_b).any() else end_c.iloc[0]
                sp, ep = float(start_row['price']), float(end_row['price'])
                if pd.isna(sp) or pd.isna(ep) or sp == 0:
                    return None
                return (ep - sp) / sp * 100
            except Exception:
                return None
        
        _bounds = _period_bounds(comparison_base_date)
        _start_b, _end_b = _bounds.get(selected_period, (comparison_base_date - timedelta(days=30), comparison_base_date))
        
        # 최종 수익률 = 지수별 수익률 비교 표와 동일한 정의
        final_returns = pd.Series(dtype=float)
        for _dn in _available:
            _idx_data = _comparison_df[_comparison_df['display_name'] == _dn].sort_values('dt')
            _r = _calc_return(_idx_data, _start_b, _end_b)
            if _r is not None:
                final_returns[_dn] = _r
        final_returns = final_returns.sort_values(ascending=False)
        
        st.caption(f"**기간** ({selected_period}): {_start_b} ~ {_end_b} (기준일자: {comparison_base_date})")
        
        if not final_returns.empty:
            # 최종 수익률을 메트릭 카드로 상단에 표시 (상위 5개, 지수별 수익률 비교 표와 동일 수치)
            if not final_returns.empty:
                st.subheader("🏆 최종 수익률 Top 5")
                top5_cols = st.columns(5)
                for idx, (index_name, return_val) in enumerate(final_returns.head(5).items()):
                    with top5_cols[idx]:
                        # None 체크 추가
                        if return_val is None or pd.isna(return_val):
                            continue
                        
                        if return_val >= 0:
                            delta_color = "normal"
                            delta_prefix = "+"
                        else:
                            delta_color = "inverse"
                            delta_prefix = ""
                        
                        st.metric(
                            label=index_name.replace(" Index", ""),
                            value=f"{return_val:.2f}%",
                            delta=f"{delta_prefix}{return_val:.2f}%",
                            delta_color=delta_color
                        )
            
            # 전체 수익률을 정렬된 테이블로 표시
            if not final_returns.empty:
                with st.expander("📋 전체 지수 수익률 보기", expanded=False):
                    # None 값 제거
                    valid_returns = final_returns[final_returns.notna()]
                    if not valid_returns.empty:
                        returns_df = pd.DataFrame({
                            '지수명': valid_returns.index,
                            '수익률(%)': [f"{val:.2f}%" if pd.notna(val) else "N/A" for val in valid_returns.values]
                        })
                        returns_df['순위'] = range(1, len(returns_df) + 1)
                        returns_df = returns_df[['순위', '지수명', '수익률(%)']]
                        
                        def color_returns(val):
                            try:
                                return_val = float(val.rstrip('%'))
                                if return_val >= 2:
                                    return 'background-color: #d4edda; color: #155724; font-weight: bold'
                                elif return_val >= 0:
                                    return 'background-color: #fff3cd; color: #856404'
                                elif return_val >= -2:
                                    return 'background-color: #f8d7da; color: #721c24'
                                else:
                                    return 'background-color: #f5c6cb; color: #721c24; font-weight: bold'
                            except:
                                return ''
                        
                        styled_df = returns_df.style.applymap(color_returns, subset=['수익률(%)'])
                        st.markdown("""
                        <style>
                        .dataframe {
                            font-size: 16px !important;
                        }
                        .dataframe th {
                            font-size: 18px !important;
                            font-weight: bold !important;
                            padding: 12px !important;
                        }
                        .dataframe td {
                            font-size: 16px !important;
                            padding: 10px !important;
                        }
                        </style>
                        """, unsafe_allow_html=True)
                        st.dataframe(styled_df, use_container_width=True, hide_index=True)
            
            # Plotly 차트: 기준일자까지 동일 가격 데이터로 누적 수익률 (표와 수치 일치)
            if not final_returns.empty and not _comparison_df.empty:
                valid_final_returns = final_returns[final_returns.notna()]
                if not valid_final_returns.empty:
                    _comparison_df['dt_date'] = _comparison_df['dt'].dt.date
                    distinct_colors = [
                        '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                        '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
                    ]
                    additional_colors = ['#ff9896', '#c5b0d5', '#c49c94', '#f7b6d3', '#dbdb8d']
                    all_colors = distinct_colors + additional_colors
                    color_map = {name: all_colors[i % len(all_colors)] for i, name in enumerate(valid_final_returns.index)}
                    fig = go.Figure()
                    
                    for index_name in valid_final_returns.index:
                        idx_data = _comparison_df[_comparison_df['display_name'] == index_name].sort_values('dt').copy()
                        if idx_data.empty:
                            continue
                        start_c = idx_data[idx_data['dt_date'] <= _start_b]
                        if start_c.empty:
                            start_c = idx_data[idx_data['dt_date'] >= _start_b]
                        if start_c.empty:
                            continue
                        base_price = float(start_c.iloc[-1]['price']) if (idx_data['dt_date'] <= _start_b).any() else float(start_c.iloc[0]['price'])
                        window = idx_data[(idx_data['dt_date'] >= _start_b) & (idx_data['dt_date'] <= _end_b)]
                        if window.empty:
                            continue
                        window = window.copy()
                        window['cumulative_return'] = (window['price'].astype(float) - base_price) / base_price * 100
                        return_val = valid_final_returns[index_name]
                        line_width = 3.0 if abs(return_val) > 2 else 2.0
                        line_dash = 'dash' if return_val < 0 else 'solid'
                        fig.add_trace(go.Scatter(
                            x=window['dt'],
                            y=window['cumulative_return'],
                            mode='lines',
                            name=index_name.replace(" Index", ""),
                            line=dict(color=color_map[index_name], width=line_width, dash=line_dash),
                            hovertemplate=f'<b>{index_name.replace(" Index", "")}</b><br>날짜: %{{x}}<br>수익률: %{{y:.2f}}%<br>최종: {return_val:.2f}%<extra></extra>'
                        ))
                    
                    fig.update_layout(
                        title="",
                        xaxis_title="날짜",
                        yaxis_title="수익률 (%)",
                        hovermode='x unified',
                        legend=dict(
                            orientation="v",
                            yanchor="top",
                            y=1,
                            xanchor="left",
                            x=1.02,
                            font=dict(size=20)
                        ),
                        height=600,
                        template='plotly_white',
                        xaxis=dict(
                            showgrid=True,
                            gridwidth=1,
                            gridcolor='lightgray',
                            title_font=dict(size=24),
                            tickfont=dict(size=20)
                        ),
                        yaxis=dict(
                            showgrid=True,
                            gridwidth=1,
                            gridcolor='lightgray',
                            zeroline=True,
                            zerolinecolor='black',
                            zerolinewidth=1,
                            title_font=dict(size=24),
                            tickfont=dict(size=20)
                        )
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("표시할 수익률 데이터가 없습니다. 기준일자를 확인해주세요.")
        
        # 지수간 비교 테이블 (위와 동일한 기준일자·가격 데이터 사용)
        st.markdown("---")
        st.subheader("📊 지수별 수익률 비교")
        st.caption("YTD = 해당 연도 1월 1일 **이전 최종 거래일**(연말 종가) ~ 기준일. 위 최종 수익률·차트와 동일 수치입니다.")
        
        comparison_indices_df = _comparison_df
        available_indices = _available
        
        selected_indices_for_comparison = st.multiselect(
            "비교할 지수 선택",
            options=available_indices,
            default=available_indices,
            format_func=lambda x: x.replace(" Index", "")
        )
        
        if selected_indices_for_comparison:
                period_bounds = _period_bounds(comparison_base_date)
                
                if comparison_indices_df.empty:
                    st.warning("지수별 수익률 비교를 위한 데이터를 가져올 수 없습니다.")
                else:
                    # 디버깅: 데이터 확인
                    # st.write(f"전체 데이터 개수: {len(comparison_indices_df)}")
                    # st.write(f"데이터 날짜 범위: {comparison_indices_df['dt'].min()} ~ {comparison_indices_df['dt'].max()}")
                    
                    comparison_data = []
                    for display_name in selected_indices_for_comparison:
                        index_data = comparison_indices_df[
                            comparison_indices_df['display_name'] == display_name
                        ].sort_values('dt').copy()
                        
                        if not index_data.empty:
                            row_data = {
                                '지수명': display_name.replace(" Index", "") if " Index" in str(display_name) else display_name
                            }
                            
                            for period_name, (start_bound, end_bound) in period_bounds.items():
                                return_val = _calc_return(index_data, start_bound, end_bound)
                                row_data[period_name] = return_val
                            
                            comparison_data.append(row_data)
                    
                    if not comparison_data:
                        st.warning("선택한 지수에 대한 데이터를 찾을 수 없습니다.")
                    
                    if comparison_data:
                        comparison_df = pd.DataFrame(comparison_data)
                        
                        # 원하는 컬럼 순서 정의: 1D -> 1W -> MTD -> 1M -> 3M -> 6M -> YTD -> 1Y
                        desired_column_order = ['1D', '1W', 'MTD', '1M', '3M', '6M', 'YTD', '1Y']
                        # period_bounds에 있는 컬럼만 사용
                        available_columns = [col for col in desired_column_order if col in comparison_df.columns]
                        column_order = ['지수명'] + available_columns
                        
                        # 정렬 옵션 설정 (기본 YTD 내림차순)
                        available_sort_columns = [col for col in desired_column_order if col in comparison_df.columns]
                        if 'comparison_sort_column' not in st.session_state:
                            st.session_state.comparison_sort_column = 'YTD' if 'YTD' in available_sort_columns else '정렬 안함'
                        
                        sort_options = ['정렬 안함'] + available_sort_columns
                        
                        # 현재 선택된 정렬 기준의 인덱스 찾기
                        current_index = 0
                        if st.session_state.comparison_sort_column in sort_options:
                            current_index = sort_options.index(st.session_state.comparison_sort_column)
                        
                        selected_sort = st.selectbox(
                            "정렬 기준 컬럼 선택 (내림차순)",
                            options=sort_options,
                            index=current_index,
                            key="comparison_sort_select"
                        )
                        
                        # 선택된 정렬 기준 저장
                        st.session_state.comparison_sort_column = selected_sort
                        
                        # 정렬 수행 (문자열 포맷팅 전에 숫자 값으로 정렬)
                        if selected_sort != '정렬 안함' and selected_sort in comparison_df.columns:
                            # 정렬용 임시 컬럼 생성 (숫자 값으로 변환)
                            sort_values = []
                            for idx in comparison_df.index:
                                val = comparison_df.loc[idx, selected_sort]
                                if val is None or pd.isna(val):
                                    sort_values.append(-999999)
                                elif isinstance(val, (int, float)):
                                    sort_values.append(float(val))
                                else:
                                    # 이미 문자열인 경우
                                    try:
                                        sort_values.append(float(str(val).rstrip('%')))
                                    except:
                                        sort_values.append(-999999)
                            
                            # 정렬용 컬럼 추가
                            comparison_df = comparison_df.copy()
                            comparison_df['_sort_temp'] = sort_values
                            
                            # 내림차순 정렬 (큰 값부터 작은 값 순서로)
                            comparison_df = comparison_df.sort_values('_sort_temp', ascending=False, na_position='last').reset_index(drop=True)
                            
                            # 정렬용 임시 컬럼 제거
                            comparison_df = comparison_df.drop('_sort_temp', axis=1)
                        
                        # 정렬 후에 문자열로 포맷팅
                        for period_name in available_columns:
                            if period_name in comparison_df.columns:
                                comparison_df[period_name] = comparison_df[period_name].apply(
                                    lambda x: f"{x:.2f}%" if (x is not None and pd.notna(x) and isinstance(x, (int, float))) else "N/A"
                                )
                        
                        # 최종 컬럼 순서 적용 (정렬된 행 순서는 유지)
                        comparison_df = comparison_df[column_order]
                        
                        def color_comparison_returns(val):
                            if val == "N/A":
                                return ''
                            try:
                                return_val = float(val.rstrip('%'))
                                if return_val >= 2:
                                    return 'background-color: #d4edda; color: #155724; font-weight: bold'
                                elif return_val >= 0:
                                    return 'background-color: #fff3cd; color: #856404'
                                elif return_val >= -2:
                                    return 'background-color: #f8d7da; color: #721c24'
                                else:
                                    return 'background-color: #f5c6cb; color: #721c24; font-weight: bold'
                            except:
                                return ''
                        
                        styled_comparison_df = comparison_df.style
                        for period_name in available_columns:
                            if period_name in comparison_df.columns:
                                styled_comparison_df = styled_comparison_df.applymap(
                                    color_comparison_returns,
                                    subset=[period_name]
                                )
                        
                        st.markdown("""
                        <style>
                        .dataframe {
                            font-size: 32px !important;
                        }
                        .dataframe th {
                            font-size: 36px !important;
                            font-weight: bold !important;
                            padding: 24px !important;
                            cursor: pointer;
                        }
                        .dataframe td {
                            font-size: 32px !important;
                            padding: 20px !important;
                        }
                        </style>
                        """, unsafe_allow_html=True)
                        st.dataframe(styled_comparison_df, use_container_width=True, hide_index=True)
        else:
            st.info("비교할 지수를 선택해주세요.")
    except Exception as e:
        st.error(f"오류 발생: {e}")
        import traceback
        with st.expander("상세 오류 정보"):
            st.code(traceback.format_exc())