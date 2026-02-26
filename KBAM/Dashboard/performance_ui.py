"""
Performance UI 컨트롤러
Performance 하위 카테고리(지수분석, 섹터분석, 종목분석)를 통합 관리하는 메인 컨트롤러
"""
import streamlit as st
from performance_주요지수 import render as render_주요지수
from performance_섹터분석 import render as render_섹터분석
from performance_종목분석 import render as render_종목분석


def render():
    """Performance UI 렌더링 함수"""
    # 사이드바 스타일링
    st.sidebar.markdown("""
        <style>
        .sidebar-menu {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        }
        .menu-section {
            margin: 15px 0;
            padding: 10px 0;
            border-bottom: 1px solid #e0e0e0;
        }
        .menu-item {
            padding: 8px 0;
            cursor: pointer;
            transition: all 0.2s;
        }
        .menu-item:hover {
            background-color: #f0f0f0;
            padding-left: 5px;
        }
        [data-testid="stSidebar"] {
            background-color: #fafafa;
        }
        [data-testid="stSidebar"] [data-baseweb="select"] {
            background-color: white;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Analysis 섹션
    with st.sidebar.expander("📈 Analysis", expanded=True):
        perf_option = st.radio(
            "Analysis",
            ["지수 분석", "섹터 분석", "종목 분석"],
            label_visibility="collapsed",
            key="perf_radio",
            index=0  # 기본값: 지수 분석
        )
        st.session_state.perf_tab = perf_option
    
    st.sidebar.markdown("---")
    
    # 페이지 제목
    st.title("📈 Analysis")
    
    # 탭 스타일링 (글자 크기 확대)
    st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 10px 20px;
        font-size: 18px;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        font-size: 20px;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Analysis 하위 카테고리 탭: 지수 분석 > 섹터 분석 > 종목 분석
    perf_tab_names = ["📊 지수 분석", "🏢 섹터 분석", "🏆 종목 분석"]
    perf_tab_labels = ["지수 분석", "섹터 분석", "종목 분석"]

    # 세션 상태 초기화
    if 'perf_tab' not in st.session_state:
        st.session_state.perf_tab = "지수 분석"
    
    # 사이드바에서 선택한 옵션에 따라 해당 탭 인덱스 찾기
    try:
        selected_tab_idx = perf_tab_labels.index(st.session_state.perf_tab)
    except ValueError:
        selected_tab_idx = 0
    
    # 탭 생성
    perf_tabs = st.tabs(perf_tab_names)
    
    # ========== 탭 1: 지수 분석 ==========
    with perf_tabs[0]:
        render_주요지수()
    
    # ========== 탭 2: 섹터 분석 ==========
    with perf_tabs[1]:
        render_섹터분석()
    
    # ========== 탭 3: 종목 분석 ==========
    with perf_tabs[2]:
        render_종목분석()


# 독립 실행 시 (performance_ui.py를 직접 실행할 때)
if __name__ == "__main__" or not hasattr(st.session_state, 'main_menu'):
    # 페이지 설정
    st.set_page_config(
        page_title="Index Quant",
        page_icon="📈",
        layout="wide"
    )
    
    # 사이드바 헤더
    st.sidebar.markdown("### 📊 KBAM Index Quant")
    st.sidebar.markdown("---")
    
    render()