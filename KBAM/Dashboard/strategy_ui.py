"""
Strategy UI 컨트롤러
Strategy 하위 카테고리(성과추적 등)를 통합 관리하는 메인 컨트롤러
"""
import streamlit as st
from strategy_성과추적 import render as render_성과추적


def render():
    """Strategy UI 렌더링 함수"""
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
    
    # Strategy 섹션
    with st.sidebar.expander("📊 Strategy", expanded=True):
        strategy_option = st.radio(
            "Strategy",
            ["성과 추적"],
            label_visibility="collapsed",
            key="strategy_radio",
            index=0
        )
        st.session_state.strategy_tab = strategy_option
    
    st.sidebar.markdown("---")
    
    # 페이지 제목
    st.title("📊 Strategy Dashboard")
    
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
    
    # Strategy 하위 카테고리 탭
    strategy_tab_names = ["📊 성과 추적"]
    strategy_tab_labels = ["성과 추적"]
    
    # 세션 상태 초기화
    if 'strategy_tab' not in st.session_state:
        st.session_state.strategy_tab = "성과 추적"
    
    # 사이드바에서 선택한 옵션에 따라 해당 탭 인덱스 찾기
    try:
        selected_tab_idx = strategy_tab_labels.index(st.session_state.strategy_tab)
    except ValueError:
        selected_tab_idx = 0
    
    # 탭 생성
    strategy_tabs = st.tabs(strategy_tab_names)
    
    # ========== 탭 1: 성과 추적 ==========
    with strategy_tabs[0]:
        render_성과추적()


# 독립 실행 시 (strategy_ui.py를 직접 실행할 때)
if __name__ == "__main__" or not hasattr(st.session_state, 'main_menu'):
    # 페이지 설정
    st.set_page_config(
        page_title="KBAM Strategy Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    # 사이드바 헤더
    st.sidebar.markdown("### 📊 KBAM AI Quant")
    st.sidebar.markdown("---")
    
    render()