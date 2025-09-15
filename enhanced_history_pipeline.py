#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版可转债历史数据抓取管道 V3.3 (终极兼容版)
修复了因不同接口返回不同列名导致的 KeyError
"""

import os
import sys
import logging
import pandas as pd
import akshare as ak
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects import sqlite
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests
from typing import Dict, List, Optional, Tuple, Callable
import json
import inspect

# --- 配置区 ---
DB_FOLDER = 'data'
DB_FILE = 'cb_data.db'
DB_PATH = os.path.join(DB_FOLDER, DB_FILE)
HISTORY_TABLE_NAME = 'cb_daily_history'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
engine = create_engine(f'sqlite:///{DB_PATH}')

def robust_akshare_call(func: Callable, max_retries: int = 3, initial_delay: float = 2.0, **kwargs):
    delay = initial_delay
    func_params = inspect.signature(func).parameters
    for attempt in range(max_retries):
        try:
            call_kwargs = kwargs.copy()
            if 'timeout' in func_params and 'timeout' not in call_kwargs:
                call_kwargs['timeout'] = 30
            result = func(**call_kwargs)
            time.sleep(0.5)
            return result
        except Exception as e:
            error_str = str(e).lower()
            if "remote end closed connection" in error_str or "connection aborted" in error_str:
                logging.warning(f"远程服务器关闭连接: {func.__name__} (第 {attempt + 1} 次尝试). {kwargs.get('symbol')}")
            elif "该股票代码不存在" in str(e) or "not found" in error_str or "'date'" in str(e): # 增加对KeyError的处理
                logging.warning(f"接口 {func.__name__} 报告代码不存在或返回数据异常: {kwargs.get('symbol')}. 错误: {e}. 停止重试。")
                return pd.DataFrame()
            else:
                 logging.warning(f"调用 {func.__name__} 失败 (第 {attempt + 1} 次尝试): {kwargs.get('symbol')}. 错误: {e}")
            if attempt < max_retries - 1:
                logging.info(f"将在 {delay:.2f} 秒后重试...")
                time.sleep(delay)
                delay *= 2
            else:
                logging.error(f"调用 {func.__name__} 达到最大重试次数，放弃: {kwargs.get('symbol')}")
                return pd.DataFrame()
    return pd.DataFrame()

class EnhancedBondDataCollector:
    def __init__(self):
        self.engine = engine

    def get_all_bonds_list(self) -> pd.DataFrame:
        try:
            logging.info("开始从同花顺获取全量可转债列表...")
            all_bonds_df = robust_akshare_call(ak.bond_zh_cov_info_ths)
            if all_bonds_df.empty: return pd.DataFrame()
            all_bonds_df.rename(columns={'债券代码': 'bond_code', '正股代码': 'stock_code', '债券简称': 'bond_name', '正股简称': 'stock_name'}, inplace=True)
            if 'stock_code' in all_bonds_df.columns:
                 all_bonds_df['stock_code'] = all_bonds_df['stock_code'].astype(str).str.replace(r'^(sh|sz)', '', regex=True, case=False).str.strip()
            required_cols = ['bond_code', 'stock_code', 'bond_name', 'stock_name']
            all_bonds_df = all_bonds_df[required_cols].drop_duplicates(subset=['bond_code']).reset_index(drop=True)
            logging.info(f"成功获取到 {len(all_bonds_df)} 只历史可转债信息。")
            return all_bonds_df
        except Exception as e:
            logging.error(f"从同花顺获取全量可转债列表失败: {e}", exc_info=True)
            return pd.DataFrame()
            
    def get_bond_value_analysis(self, bond_code: str) -> pd.DataFrame:
        value_df = robust_akshare_call(ak.bond_zh_cov_value_analysis, symbol=bond_code)
        return self._clean_value_analysis_data(value_df)

    def get_bond_history(self, bond_code: str) -> pd.DataFrame:
        hist_df = robust_akshare_call(ak.stock_zh_a_hist, symbol=bond_code, period="daily", adjust="")
        if not hist_df.empty:
            logging.info(f"成功从[东财]获取债券 {bond_code} 历史行情。")
            return self._clean_bond_history_data(hist_df)
        market_code = self._get_market_code_for_hist(bond_code)
        if market_code:
            hist_df = robust_akshare_call(ak.bond_zh_hs_cov_daily, symbol=market_code)
            if not hist_df.empty:
                logging.info(f"成功从[新浪]获取债券 {bond_code} 历史行情。")
                return self._clean_bond_history_data(hist_df)
        logging.warning(f"所有数据源均未能获取债券 {bond_code} 的历史行情。")
        return pd.DataFrame()

    def get_stock_history_data(self, stock_code: str) -> pd.DataFrame:
        stock_df = robust_akshare_call(ak.stock_zh_a_hist, symbol=stock_code, adjust="hfq")
        if not stock_df.empty:
            logging.info(f"成功从[东财]获取正股 {stock_code} 历史行情。")
            return self._clean_stock_history_data(stock_df)
        market_code = self._get_market_code_for_stock_hist(stock_code)
        if market_code:
            stock_df = robust_akshare_call(ak.stock_zh_a_hist_tx, symbol=market_code, adjust="hfq")
            if not stock_df.empty:
                logging.info(f"成功从[腾讯]获取正股 {stock_code} 历史行情。")
                return self._clean_stock_history_data(stock_df)
        logging.warning(f"所有数据源均未能获取正股 {stock_code} 的历史行情。")
        return pd.DataFrame()

    def _clean_value_analysis_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        df.rename(columns={'日期': 'trade_date', '收盘价': 'price_val', '纯债价值': 'pure_bond_value', '转股价值': 'conv_value',
                           '纯债溢价率': 'pure_bond_premium_rate', '转股溢价率': 'premium_rate'}, inplace=True)
        numeric_cols = ['price_val', 'pure_bond_value', 'conv_value', 'pure_bond_premium_rate', 'premium_rate']
        for col in numeric_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        return df

    # --- 核心修正：强化此函数对不同数据源的兼容性 ---
    def _clean_bond_history_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        # 定义一个更宽容的列名映射，覆盖所有可能的名字
        column_mapping = {
            '日期': 'trade_date', 'date': 'trade_date',
            '开盘': 'open_price', 'open': 'open_price',
            '最高': 'high_price', 'high': 'high_price',
            '最低': 'low_price', 'low': 'low_price',
            '收盘': 'price', 'close': 'price',
            '成交量': 'volume', 'volume': 'volume',
            '成交额': 'turnover', 'amount': 'turnover',
            '换手率': 'turnover_rate'
        }
        # 只重命名DataFrame中实际存在的列
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

        numeric_cols = ['open_price', 'high_price', 'low_price', 'price', 'volume', 'turnover', 'turnover_rate']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # 单位转换逻辑，增加对空值的检查
                if col == 'volume' and df[col].notna().any() and df[col][df[col].notna()].mean() < 1000000:
                    df[col] = df[col] * 100
        
        # 确保 trade_date 列存在并格式化
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        else:
            # 如果万一没有任何日期列，则返回空，防止后续出错
            logging.error(f"历史数据清洗失败：找不到日期列。原始列: {df.columns.tolist()}")
            return pd.DataFrame()
            
        return df

    def _clean_stock_history_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return pd.DataFrame()
        column_mapping = {'日期': 'trade_date', 'date': 'trade_date', '收盘': 'stock_price', 'close': 'stock_price', '涨跌幅': 'stock_chg_pct'}
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        numeric_cols = ['stock_price', 'stock_chg_pct']
        for col in numeric_cols:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
        required_cols = ['trade_date', 'stock_price', 'stock_chg_pct']
        # 增加对列是否存在的检查
        if not all(c in df.columns for c in required_cols):
            return pd.DataFrame()
        return df[required_cols]
    
    def _get_market_code_for_hist(self, bond_code: str) -> Optional[str]:
        if not isinstance(bond_code, str): return None
        if bond_code.startswith('11'): return f"sh{bond_code}"
        elif bond_code.startswith('12'): return f"sz{bond_code}"
        return None
        
    def _get_market_code_for_stock_hist(self, stock_code: str) -> Optional[str]:
        if not isinstance(stock_code, str): return None
        if stock_code.startswith('6'): return f"sh{stock_code}"
        elif stock_code.startswith(('0', '3')): return f"sz{stock_code}"
        return None
        
    def collect_comprehensive_bond_data(self, bond_info: Dict) -> pd.DataFrame:
        bond_code = bond_info.get('bond_code')
        stock_code = bond_info.get('stock_code')
        if not bond_code or not stock_code or pd.isna(stock_code): return pd.DataFrame()
        logging.info(f"开始收集债券 {bond_code} (正股: {stock_code})")
        hist_df = self.get_bond_history(bond_code)
        value_df = self.get_bond_value_analysis(bond_code)
        stock_df = self.get_stock_history_data(stock_code)
        merged_df = self._merge_bond_data(hist_df, value_df, stock_df, bond_info)
        if not merged_df.empty:
            logging.debug(f"债券 {bond_code} 数据收集完成，共 {len(merged_df)} 条记录")
        else:
            logging.warning(f"债券 {bond_code} 数据合并后为空")
        return merged_df

    def _merge_bond_data(self, hist_df: pd.DataFrame, value_df: pd.DataFrame, stock_df: pd.DataFrame, bond_info: Dict) -> pd.DataFrame:
        if hist_df.empty and value_df.empty: return pd.DataFrame()
        if not hist_df.empty:
            merged_df = hist_df
            if not value_df.empty:
                merged_df = pd.merge(merged_df, value_df, on='trade_date', how='left', suffixes=('', '_val'))
                if 'price' in merged_df.columns and 'price_val' in merged_df.columns:
                    merged_df['price'] = merged_df['price'].fillna(merged_df['price_val'])
                    merged_df.drop(columns=['price_val'], inplace=True)
        else:
            merged_df = value_df.rename(columns={'price_val': 'price'})
        for key, value in bond_info.items(): merged_df[key] = value
        if not stock_df.empty:
            merged_df = pd.merge(merged_df, stock_df, on='trade_date', how='left')
        return self._calculate_derived_metrics(merged_df)

    def _calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        df = df.sort_values(by='trade_date').reset_index(drop=True)
        if 'price' in df.columns:
            df['price_chg_pct'] = df['price'].pct_change() * 100
        if 'price' in df.columns and 'premium_rate' in df.columns:
            df['double_low'] = df['price'] + df['premium_rate']
        return df

    def save_to_database(self, df: pd.DataFrame, table_name: str) -> int:
        if df.empty: return 0
        try:
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str).apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8', 'ignore'))
            with self.engine.connect() as connection:
                with connection.begin():
                    table_info = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 0", connection)
                    existing_columns = table_info.columns.tolist()
                    columns_to_save = [col for col in df.columns if col in existing_columns]
                    df_to_save = df[columns_to_save]
                    records_to_insert = df_to_save.to_dict(orient='records')
                    if not records_to_insert: return 0
                    metadata = MetaData()
                    table = Table(table_name, metadata, autoload_with=connection)
                    stmt = sqlite.insert(table).values(records_to_insert)
                    stmt = stmt.on_conflict_do_nothing(index_elements=['trade_date', 'bond_code'])
                    result = connection.execute(stmt)
                    return result.rowcount
        except Exception as e:
            logging.error(f"保存数据到数据库失败: {e}", exc_info=True)
            return 0
            
    def run_comprehensive_collection(self, max_workers: int = 5) -> None:
        logging.info("开始运行增强版可转债历史数据收集（基于全量列表）")
        bond_list = self.get_all_bonds_list()
        if bond_list.empty:
            logging.error("无法获取全量债券列表，程序退出")
            return
        tasks = bond_list.to_dict('records')
        total_tasks = len(tasks)
        completed_count = 0
        logging.info(f"共找到 {total_tasks} 只历史可转债，开始并行收集...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_bond = {executor.submit(self.collect_comprehensive_bond_data, bond_info): bond_info['bond_code'] for bond_info in tasks}
            for future in as_completed(future_to_bond):
                bond_code = future_to_bond[future]
                try:
                    bond_data = future.result()
                    if not bond_data.empty:
                        saved_count = self.save_to_database(bond_data, HISTORY_TABLE_NAME)
                        logging.info(f"债券 {bond_code} 数据保存完成，新增/更新 {saved_count} 条记录")
                except Exception as exc:
                    logging.error(f"债券 {bond_code} 数据处理失败: {exc}", exc_info=True)
                completed_count += 1
                logging.info(f"进度: {completed_count}/{total_tasks} ({(completed_count/total_tasks)*100:.2f}%)")
        logging.info("增强版可转债历史数据收集完成")