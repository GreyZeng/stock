#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库连接和查询模块 (最终版 - 包含字段中文描述)
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Optional, Tuple
import streamlit as st

class BondDatabase:
    """可转债数据库操作类"""
    
    def __init__(self, db_path: str = '../data/cb_data.db'):
        self.db_path = db_path
    
    def get_connection(self):
        """获取数据库连接"""
        return sqlite3.connect(self.db_path)
    
    def get_available_dates(self) -> List[str]:
        """获取可用的交易日期列表"""
        try:
            with self.get_connection() as conn:
                query = "SELECT DISTINCT trade_date FROM cb_daily_history ORDER BY trade_date DESC"
                df = pd.read_sql_query(query, conn)
                return df['trade_date'].tolist()
        except Exception:
            return []
    
    def get_bond_ratings(self) -> List[str]:
        """获取所有债券评级"""
        try:
            with self.get_connection() as conn:
                query = "SELECT DISTINCT bond_rating FROM cb_daily_history WHERE bond_rating IS NOT NULL ORDER BY bond_rating"
                df = pd.read_sql_query(query, conn)
                return df['bond_rating'].tolist()
        except Exception:
            return []

    # --- 核心新增：获取包含中文描述的数据质量统计 ---
    def get_column_quality_stats(self) -> pd.DataFrame:
        """获取 cb_daily_history 表中每个字段的数据质量统计，并附带中文描述"""
        
        # 定义字段的中文描述字典
        field_descriptions = {
            'trade_date': '交易日期', 'bond_code': '债券代码', 'bond_name': '债券名称', 'price': '转债价格',
            'price_chg_pct': '涨跌幅(%)', 'open_price': '开盘价', 'high_price': '最高价', 'low_price': '最低价',
            'volume': '成交量(股)', 'turnover': '成交额(元)', 'turnover_rate': '换手率(%)', 'stock_code': '正股代码',
            'stock_name': '正股名称', 'stock_price': '正股价格', 'stock_chg_pct': '正股涨跌幅(%)', 'stock_pb': '正股PB',
            'conv_price': '转股价', 'conv_value': '转股价值', 'premium_rate': '溢价率(%)', 'pure_bond_value': '纯债价值',
            'pure_bond_premium_rate': '纯债溢价率(%)', 'double_low': '双低值', 'bond_rating': '评级',
            'put_trigger_price': '回售触发价', 'force_redeem_trigger_price': '强赎触发价',
            'conv_proportion': '转债占比(%)', 'maturity_date': '到期日期', 'remaining_years': '剩余年限',
            'remaining_size': '剩余规模(亿元)', 'ytm_before_tax': '税前到期收益率(%)',
            'created_at': '创建时间', 'updated_at': '更新时间'
        }

        with self.get_connection() as conn:
            table_info_query = "PRAGMA table_info(cb_daily_history);"
            columns_df = pd.read_sql_query(table_info_query, conn)
            
            total_records_query = "SELECT COUNT(*) FROM cb_daily_history;"
            total_records = pd.read_sql_query(total_records_query, conn).iloc[0, 0]

            if total_records == 0:
                return pd.DataFrame(columns=["字段名", "中文含义", "数据类型", "缺失数量", "缺失比例(%)", "唯一值数量"])

            stats_list = []
            for _, row in columns_df.iterrows():
                col_name = row['name']
                col_type = row['type']

                missing_count_query = f"SELECT COUNT(*) FROM cb_daily_history WHERE \"{col_name}\" IS NULL;"
                missing_count = pd.read_sql_query(missing_count_query, conn).iloc[0, 0]

                unique_count_query = f"SELECT COUNT(DISTINCT \"{col_name}\") FROM cb_daily_history;"
                unique_count = pd.read_sql_query(unique_count_query, conn).iloc[0, 0]
                
                missing_ratio = (missing_count / total_records) * 100 if total_records > 0 else 0

                stats_list.append({
                    "字段名": col_name,
                    "中文含义": field_descriptions.get(col_name, "未知字段"), # 从字典中获取中文名
                    "数据类型": col_type,
                    "缺失数量": missing_count,
                    "缺失比例(%)": round(missing_ratio, 2),
                    "唯一值数量": unique_count
                })
            
            return pd.DataFrame(stats_list)
            
    def build_where_conditions(self, filters: Dict) -> Tuple[str, List]:
        conditions = []
        params = []
        if filters.get('date'):
            conditions.append("trade_date = ?")
            params.append(filters['date'])
        if filters.get('bond_name'):
            conditions.append("(bond_name LIKE ? OR bond_code LIKE ?)")
            search_term = f"%{filters['bond_name']}%"
            params.extend([search_term, search_term])
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return where_clause, params
    
    def search_bonds(self, filters: Dict, sort_column: str = 'double_low', 
                    sort_direction: str = 'ASC', limit: Optional[int] = None) -> pd.DataFrame:
        where_clause, params = self.build_where_conditions(filters)
        query = f"""
        SELECT 
            trade_date, bond_code, bond_name,
            COALESCE(price, 0) as price, COALESCE(price_chg_pct, 0) as price_chg_pct, COALESCE(open_price, 0) as open_price,
            COALESCE(high_price, 0) as high_price, COALESCE(low_price, 0) as low_price, COALESCE(volume, 0) as volume,
            COALESCE(turnover, 0) as turnover, COALESCE(turnover_rate, 0) as turnover_rate, stock_code, stock_name,
            COALESCE(stock_price, 0) as stock_price, COALESCE(stock_chg_pct, 0) as stock_chg_pct, COALESCE(stock_pb, 0) as stock_pb,
            COALESCE(conv_price, 0) as conv_price, COALESCE(conv_value, 0) as conv_value, COALESCE(premium_rate, 0) as premium_rate,
            COALESCE(pure_bond_value, 0) as pure_bond_value, COALESCE(pure_bond_premium_rate, 0) as pure_bond_premium_rate,
            COALESCE(double_low, 0) as double_low, bond_rating, COALESCE(put_trigger_price, 0) as put_trigger_price,
            COALESCE(force_redeem_trigger_price, 0) as force_redeem_trigger_price, COALESCE(conv_proportion, 0) as conv_proportion,
            maturity_date, COALESCE(remaining_years, 0) as remaining_years, COALESCE(remaining_size, 0) as remaining_size,
            COALESCE(ytm_before_tax, 0) as ytm_before_tax
        FROM cb_daily_history 
        WHERE {where_clause} ORDER BY {sort_column} {sort_direction}
        """
        if limit: query += f" LIMIT {limit}"
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    
    def get_database_stats(self) -> Dict:
        with self.get_connection() as conn:
            stats = {}
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM cb_daily_history")
            stats['total_records'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT bond_code) FROM cb_daily_history")
            stats['total_bonds'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT trade_date) FROM cb_daily_history")
            stats['trading_days'] = cursor.fetchone()[0]
            cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM cb_daily_history")
            date_range = cursor.fetchone()
            stats['date_range'] = f"{date_range[0]} 到 {date_range[1]}" if date_range and date_range[0] else "N/A"
            return stats