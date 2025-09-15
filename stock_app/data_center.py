#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据中心 - Streamlit应用主页面 (最终完整版)
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from database import BondDatabase

# 页面配置
st.set_page_config(
    page_title="可转债数据中心",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# 初始化数据库连接
@st.cache_resource
def get_database():
    return BondDatabase()

db = get_database()

# 缓存数据获取函数
@st.cache_data
def get_available_dates():
    return db.get_available_dates()

@st.cache_data
def get_bond_ratings():
    return db.get_bond_ratings()

@st.cache_data
def get_quality_report():
    """获取并缓存数据质量报告"""
    return db.get_column_quality_stats()

available_dates = get_available_dates()
bond_ratings = get_bond_ratings()

# 添加CSS样式
st.markdown("""
<style>
/* 控件对齐样式 */
.stDateInput > div > div > input { height: 38px !important; }
.stTextInput > div > div > input { height: 38px !important; }
.stSelectbox > div > div > div { height: 38px !important; }
.stDateInput, .stTextInput, .stSelectbox { display: flex; align-items: center; }
/* 表格样式优化 */
.stDataFrame { border: 1px solid #d0d0d0; border-radius: 4px; font-family: 'Arial', sans-serif; }
.stDataFrame table { table-layout: auto !important; width: auto !important; font-size: 13px !important; line-height: 1.2 !important; font-weight: 600 !important; color: #1a1a1a !important; }
.stDataFrame th, .stDataFrame td { white-space: nowrap !important; width: auto !important; min-width: fit-content !important; }
.stDataFrame th { background-color: #f8f9fa !important; font-weight: 700 !important; color: #212529 !important; padding: 6px 8px !important; border-bottom: 2px solid #dee2e6 !important; }
.stDataFrame td { padding: 4px 8px !important; border-bottom: 1px solid #e9ecef !important; font-weight: 600 !important; }
.stDataFrame tbody tr:nth-child(even) { background-color: #f8f9fa !important; }
.stDataFrame tbody tr:nth-child(odd) { background-color: #ffffff !important; }
.stDataFrame tbody tr:hover { background-color: #e3f2fd !important; }
/* 固定表头CSS */
.stDataFrame th { position: sticky; top: 0; z-index: 2; background-color: #f8f9fa !important; }
.stDataFrame > div { overflow-y: hidden !important; }
.stDataFrame > div > div { overflow-x: auto !important; overflow-y: hidden !important; }
.stDataFrame iframe { overflow: hidden !important; }
</style>
""", unsafe_allow_html=True)

# 紧凑顶部
col1, col2 = st.columns([1, 2])
with col1:
    if available_dates:
        date_objects = [datetime.strptime(d, '%Y-%m-%d').date() for d in available_dates if d]
        if date_objects:
            selected_date_obj = st.date_input(
                "日期", value=date_objects[0], min_value=min(date_objects), max_value=max(date_objects),
                help="选择有数据的交易日期", label_visibility="collapsed"
            )
            selected_date = selected_date_obj.strftime('%Y-%m-%d')
        else:
            selected_date = None
    else:
        selected_date = None
with col2:
    search_term = st.text_input("搜索", placeholder="输入债券名称、代码或关键词", label_visibility="collapsed")

# 主内容区域
if selected_date:
    filters = {'date': selected_date}
    
    with st.spinner("加载数据中..."):
        try:
            df = db.search_bonds(filters, sort_column='double_low', sort_direction='ASC')
            
            if not df.empty:
                display_df = df.copy()
                column_mapping = {
                    'bond_code': '债券代码', 'bond_name': '债券名称', 'price': '转债价格', 'premium_rate': '溢价率%',
                    'double_low': '双低值', 'conv_value': '转股价值', 'stock_code': '正股代码', 'stock_name': '正股名称',
                    'stock_price': '正股价格', 'stock_pb': '正股PB', 'conv_price': '转股价', 'bond_rating': '评级',
                    'remaining_years': '剩余年限', 'remaining_size': '剩余规模(亿)', 'ytm_before_tax': '税前收益%',
                    'turnover_rate': '换手率%', 'put_trigger_price': '回售触发价', 'force_redeem_trigger_price': '强赎触发价',
                    'pure_bond_value': '纯债价值', 'pure_bond_premium_rate': '纯债溢价率%', 'maturity_date': '到期日期',
                    'conv_proportion': '转债占比%', 'price_chg_pct': '涨跌幅%', 'stock_chg_pct': '正股涨跌%',
                    'open_price': '开盘价', 'high_price': '最高价', 'low_price': '最低价', 'volume': '成交量(手)', 'turnover': '成交额(万)'
                }
                display_df = display_df.rename(columns=column_mapping)
                
                numeric_cols = [
                    '转债价格', '溢价率%', '双低值', '转股价值', '正股价格', '正股PB', '转股价', '剩余年限',
                    '剩余规模(亿)', '税前收益%', '换手率%', '回售触发价', '强赎触发价', '纯债价值',
                    '纯债溢价率%', '转债占比%', '涨跌幅%', '正股涨跌%', '开盘价', '最高价', '最低价'
                ]
                for col in numeric_cols:
                    if col in display_df.columns:
                        display_df[col] = pd.to_numeric(display_df[col], errors='coerce')

                if '成交量(手)' in display_df.columns:
                    display_df['成交量(手)'] = display_df['成交量(手)'] / 100
                
                if '成交额(万)' in display_df.columns:
                    display_df['成交额(万)'] = display_df['成交额(万)'] / 10000
                
                if search_term:
                    mask = (display_df['债券名称'].str.contains(search_term, case=False, na=False) |
                            display_df['债券代码'].str.contains(search_term, case=False, na=False) |
                            display_df['正股名称'].str.contains(search_term, case=False, na=False) |
                            display_df['正股代码'].str.contains(search_term, case=False, na=False))
                    display_df = display_df[mask]
                
                if search_term:
                    st.caption(f"🔍 搜索 \"{search_term}\" 找到 {len(display_df)} 条结果")
                else:
                    st.caption(f"📊 {selected_date} | 共 {len(display_df)} 只转债")
                
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        '债券代码': st.column_config.TextColumn(help="转债代码", disabled=True),
                        '债券名称': st.column_config.TextColumn(help="转债名称", disabled=True),
                        '成交量(手)': st.column_config.NumberColumn(format="%d", help="成交量（单位：手）"),
                        '成交额(万)': st.column_config.NumberColumn(format="%.2f", help="成交额（单位：万元）"),
                        '转债价格': st.column_config.NumberColumn(format="%.2f", help="转债收盘价"),
                        '涨跌幅%': st.column_config.NumberColumn(format="%.2f%%", help="转债当日涨跌幅"),
                        '溢价率%': st.column_config.NumberColumn(format="%.2f%%", help="转股溢价率"),
                        '双低值': st.column_config.NumberColumn(format="%.2f", help="价格+溢价率，越小越好"),
                        '转股价值': st.column_config.NumberColumn(format="%.2f", help="转股价值"),
                        '正股价格': st.column_config.NumberColumn(format="%.2f", help="正股价格"),
                        '正股PB': st.column_config.NumberColumn(format="%.2f", help="正股市净率"),
                        '换手率%': st.column_config.NumberColumn(format="%.2f%%", help="当日换手率"),
                        '正股涨跌%': st.column_config.NumberColumn(format="%.2f%%", help="正股当日涨跌幅"),
                        '剩余年限': st.column_config.NumberColumn(format="%.2f", help="距离到期年数"),
                        '税前收益%': st.column_config.NumberColumn(format="%.2f%%", help="持有到期年化收益率"),
                        '剩余规模(亿)': st.column_config.NumberColumn(format="%.2f", help="剩余规模"),
                        '正股代码': st.column_config.TextColumn(help="正股代码"),
                        '正股名称': st.column_config.TextColumn(help="正股名称"),
                        '开盘价': st.column_config.NumberColumn(format="%.2f", help="开盘价"),
                        '最高价': st.column_config.NumberColumn(format="%.2f", help="最高价"),
                        '最低价': st.column_config.NumberColumn(format="%.2f", help="最低价"),
                        '转股价': st.column_config.NumberColumn(format="%.2f", help="转股价"),
                        '纯债价值': st.column_config.NumberColumn(format="%.2f", help="纯债价值"),
                        '纯债溢价率%': st.column_config.NumberColumn(format="%.2f%%", help="纯债溢价率"),
                        '评级': st.column_config.TextColumn(help="债券评级"),
                        '回售触发价': st.column_config.NumberColumn(format="%.2f", help="回售触发价"),
                        '强赎触发价': st.column_config.NumberColumn(format="%.2f", help="强赎触发价"),
                        '转债占比%': st.column_config.NumberColumn(format="%.2f%%", help="转债占比"),
                        '到期日期': st.column_config.DateColumn(help="到期日期"),
                        '交易日期': st.column_config.DateColumn(help="数据日期")
                    }
                )
            else:
                st.warning("🔍 当前日期没有数据，请选择其他日期")
        except Exception as e:
            st.error(f"❌ 数据加载失败: {str(e)}")
            st.exception(e)
else:
    st.warning("⚠️ 请选择查询日期")

with st.expander("📊 数据统计与质量看板", expanded=False):
    try:
        stats = db.get_database_stats()
        st.info(f"📊 **{stats['total_records']:,}** 条记录 | **{stats['total_bonds']}** 只转债 | **{stats['trading_days']}** 个交易日 | {stats['date_range']}")
    except Exception as e:
        st.error(f"统计信息获取失败: {str(e)}")
    
    st.subheader("字段质量详情")
    with st.spinner("正在生成质量报告..."):
        quality_df = get_quality_report()
        if not quality_df.empty:
            quality_df['完整度'] = (100 - quality_df['缺失比例(%)']) / 100
            st.dataframe(
                quality_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "字段名": st.column_config.TextColumn(width="medium"),
                    "中文含义": st.column_config.TextColumn(width="medium"),
                    "缺失比例(%)": st.column_config.NumberColumn(format="%.2f%%"),
                    "完整度": st.column_config.ProgressColumn(
                        "完整度", format="%.2f%%", min_value=0, max_value=1,
                    )
                }
            )
        else:
            st.warning("无法生成数据质量报告。")