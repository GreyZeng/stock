#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据源管理模块
管理多个数据源的配置、优先级和故障转移
"""

import os
import sys
import logging
import pandas as pd
import akshare as ak
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import time
from dataclasses import dataclass
from enum import Enum

# 配置
DB_FOLDER = 'data'
DB_FILE = 'cb_data.db'
DB_PATH = os.path.join(DB_FOLDER, DB_FILE)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)


class DataSourceStatus(Enum):
    """数据源状态枚举"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    MAINTENANCE = "maintenance"


@dataclass
class DataSourceConfig:
    """数据源配置"""
    name: str
    priority: int  # 优先级，数字越小优先级越高
    status: DataSourceStatus
    request_delay: float  # 请求间隔（秒）
    max_retries: int  # 最大重试次数
    timeout: int  # 超时时间（秒）
    last_success: Optional[datetime] = None
    last_error: Optional[str] = None
    error_count: int = 0


class DataSourceManager:
    """数据源管理器"""
    
    def __init__(self):
        self.sources = {}
        self._initialize_sources()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def _initialize_sources(self):
        """初始化数据源配置"""
        self.sources = {
            'jsl': DataSourceConfig(
                name='集思录',
                priority=1,
                status=DataSourceStatus.ACTIVE,
                request_delay=0.3,
                max_retries=3,
                timeout=10
            ),
            'eastmoney': DataSourceConfig(
                name='东方财富',
                priority=2,
                status=DataSourceStatus.ACTIVE,
                request_delay=0.5,
                max_retries=3,
                timeout=15
            ),
            'sina': DataSourceConfig(
                name='新浪财经',
                priority=3,
                status=DataSourceStatus.ACTIVE,
                request_delay=0.2,
                max_retries=3,
                timeout=10
            ),
            'tencent': DataSourceConfig(
                name='腾讯财经',
                priority=4,
                status=DataSourceStatus.ACTIVE,
                request_delay=0.4,
                max_retries=3,
                timeout=10
            )
        }
    
    def get_source_by_priority(self) -> List[Tuple[str, DataSourceConfig]]:
        """按优先级获取数据源"""
        active_sources = [
            (source_id, config) for source_id, config in self.sources.items()
            if config.status == DataSourceStatus.ACTIVE
        ]
        return sorted(active_sources, key=lambda x: x[1].priority)
    
    def update_source_status(self, source_id: str, status: DataSourceStatus, error_msg: str = None):
        """更新数据源状态"""
        if source_id in self.sources:
            config = self.sources[source_id]
            config.status = status
            
            if status == DataSourceStatus.ACTIVE:
                config.last_success = datetime.now()
                config.error_count = 0
                config.last_error = None
            else:
                config.error_count += 1
                config.last_error = error_msg
                
            logging.info(f"数据源 {source_id} 状态更新为: {status.value}")
    
    def get_bond_list_with_fallback(self) -> pd.DataFrame:
        """获取债券列表（带故障转移）"""
        for source_id, config in self.get_source_by_priority():
            try:
                logging.info(f"尝试从 {config.name} 获取债券列表")
                
                if source_id == 'jsl':
                    result = self._get_bond_list_from_jsl()
                elif source_id == 'eastmoney':
                    result = self._get_bond_list_from_eastmoney()
                else: continue
                
                if not result.empty:
                    self.update_source_status(source_id, DataSourceStatus.ACTIVE)
                    logging.info(f"成功从 {config.name} 获取到 {len(result)} 只债券")
                    return result
                else:
                    self.update_source_status(source_id, DataSourceStatus.ERROR, "返回空数据")
                    
            except Exception as e:
                error_msg = str(e)
                logging.error(f"从 {config.name} 获取债券列表失败: {error_msg}", exc_info=True)
                self.update_source_status(source_id, DataSourceStatus.ERROR, error_msg)
                if config.error_count >= config.max_retries:
                    self.update_source_status(source_id, DataSourceStatus.INACTIVE, "错误次数过多")
                    logging.warning(f"数据源 {config.name} 暂时禁用")
            
            time.sleep(config.request_delay)
        
        logging.error("所有数据源都无法获取债券列表")
        return pd.DataFrame()

    def _get_bond_list_from_jsl(self) -> pd.DataFrame:
        cookie_path = os.path.join('config', 'jsl_cookie.txt')
        cookie = ""
        if os.path.exists(cookie_path):
            with open(cookie_path, 'r', encoding='utf-8') as f:
                cookie = f.read().strip()
        
        bond_df = ak.bond_cb_jsl(cookie=cookie) if cookie else ak.bond_cb_jsl()
        return self._clean_jsl_data(bond_df)
    
    def _get_bond_list_from_eastmoney(self) -> pd.DataFrame:
        bond_df = ak.bond_zh_cov()
        return self._clean_eastmoney_data(bond_df)

    def _clean_jsl_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        column_mapping = {
            '代码': 'bond_code', '转债名称': 'bond_name', '现价': 'price', '涨跌幅': 'price_chg_pct', '正股代码': 'stock_code',
            '正股名称': 'stock_name', '正股价': 'stock_price', '正股涨跌': 'stock_chg_pct', '正股PB': 'stock_pb',
            '转股价': 'conv_price', '转股价值': 'conv_value', '转股溢价率': 'premium_rate', '债券评级': 'bond_rating',
            '回售触发价': 'put_trigger_price', '强赎触发价': 'force_redeem_trigger_price', '转债占比': 'conv_proportion',
            '到期时间': 'maturity_date', '剩余年限': 'remaining_years', '剩余规模': 'remaining_size', '成交额': 'turnover',
            '换手率': 'turnover_rate', '到期税前收益': 'ytm_before_tax', '双低': 'double_low'
        }
        df = df.rename(columns=column_mapping)
        numeric_cols = [
            'price', 'price_chg_pct', 'stock_price', 'stock_chg_pct', 'stock_pb', 'conv_price', 'conv_value',
            'premium_rate', 'put_trigger_price', 'force_redeem_trigger_price', 'conv_proportion',
            'remaining_years', 'remaining_size', 'turnover', 'turnover_rate', 'ytm_before_tax', 'double_low'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        string_cols = ['bond_code', 'bond_name', 'stock_code', 'stock_name', 'bond_rating', 'maturity_date']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        if 'stock_code' in df.columns:
            df['stock_code'] = df['stock_code'].astype(str).str.replace(r'^(sh|sz)', '', regex=True, case=False).str.strip()
        return df

    def _clean_eastmoney_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        column_mapping = {
            '债券代码': 'bond_code', '债券简称': 'bond_name', '申购日期': 'subscription_date', '申购代码': 'subscription_code',
            '正股简称': 'stock_name', '转股价格': 'conv_price', '到期时间': 'maturity_date', '信用评级': 'bond_rating'
        }
        df = df.rename(columns=column_mapping)
        if 'conv_price' in df.columns:
            df['conv_price'] = pd.to_numeric(df['conv_price'], errors='coerce')
        return df

    def get_source_status_report(self) -> Dict:
        """获取数据源状态报告"""
        return {
            'timestamp': datetime.now().isoformat(),
            'sources': {
                source_id: {
                    'name': config.name,
                    'status': config.status.value,
                    'priority': config.priority,
                    'error_count': config.error_count,
                    'last_success': config.last_success.isoformat() if config.last_success else None,
                    'last_error': config.last_error
                } for source_id, config in self.sources.items()
            }
        }
    
    def save_source_status_report(self) -> None:
        """保存数据源状态报告"""
        try:
            report = self.get_source_status_report()
            os.makedirs(DB_FOLDER, exist_ok=True)
            report_path = os.path.join(DB_FOLDER, 'source_status_report.json')
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            logging.info(f"数据源状态报告已保存到: {report_path}")
        except Exception as e:
            logging.error(f"保存数据源状态报告失败: {e}", exc_info=True)