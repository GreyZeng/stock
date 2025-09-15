#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量验证和完整性检查模块
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json

# 配置
DB_FOLDER = 'data'
DB_FILE = 'cb_data.db'
DB_PATH = os.path.join(DB_FOLDER, DB_FILE)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
engine = create_engine(f'sqlite:///{DB_PATH}')


class DataQualityValidator:
    """数据质量验证器"""
    
    def __init__(self):
        self.engine = engine
        
    def validate_data_completeness(self) -> Dict:
        logging.info("开始验证数据完整性")
        results = {'total_bonds': 0, 'total_records': 0, 'date_range': {}, 'missing_data': {}, 'data_quality_score': 0.0}
        try:
            with self.engine.connect() as conn:
                results['total_bonds'] = conn.execute(text("SELECT COUNT(DISTINCT bond_code) FROM cb_daily_history")).scalar_one_or_none() or 0
                results['total_records'] = conn.execute(text("SELECT COUNT(*) FROM cb_daily_history")).scalar_one_or_none() or 0
                date_info = conn.execute(text("SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date) FROM cb_daily_history")).fetchone()
                if date_info and date_info[0]:
                    results['date_range'] = {'start_date': date_info[0], 'end_date': date_info[1], 'trading_days': date_info[2]}
            
            results['missing_data'] = self._check_missing_data()
            results['data_quality_score'] = self._calculate_quality_score(results)
            logging.info(f"数据完整性验证完成")
            return results
        except Exception as e:
            logging.error(f"数据完整性验证失败: {e}", exc_info=True)
            return results
    
    def _check_missing_data(self) -> Dict:
        missing_data = {}
        try:
            with self.engine.connect() as conn:
                table_info = pd.read_sql("SELECT * FROM cb_daily_history LIMIT 0", conn)
                total_records = conn.execute(text("SELECT COUNT(*) FROM cb_daily_history")).scalar_one_or_none() or 1
                for col in table_info.columns:
                    null_count = conn.execute(text(f"SELECT COUNT(*) FROM cb_daily_history WHERE {col} IS NULL")).scalar_one_or_none() or 0
                    missing_data[col] = {'count': null_count, 'percentage': round((null_count / total_records) * 100, 2)}
        except Exception as e:
            logging.error(f"检查缺失数据失败: {e}", exc_info=True)
        return missing_data
    
    def _calculate_quality_score(self, results: Dict) -> float:
        try:
            total_records = results.get('total_records', 0)
            if total_records == 0: return 0.0
            
            weights = {'price': 0.3, 'volume': 0.2, 'stock_price': 0.2, 'conv_value': 0.15, 'premium_rate': 0.15}
            total_weighted_missing_rate = 0
            
            for field, data in results.get('missing_data', {}).items():
                if field in weights:
                    total_weighted_missing_rate += (data['count'] / total_records) * weights[field]
            
            return max(0.0, (1 - total_weighted_missing_rate) * 100)
        except Exception as e:
            logging.error(f"计算质量评分失败: {e}", exc_info=True)
            return 0.0
    
    def validate_data_consistency(self) -> Dict:
        # 此部分逻辑可以保持不变，或根据需要进行优化
        return {'info': 'Consistency checks not implemented in detail for this version.'}

    def validate_data_freshness(self) -> Dict:
        logging.info("开始验证数据新鲜度")
        results = {'latest_date': None, 'days_since_update': 0, 'freshness_score': 0.0}
        try:
            latest_date = pd.read_sql("SELECT MAX(trade_date) as latest_date FROM cb_daily_history", self.engine).iloc[0]['latest_date']
            results['latest_date'] = latest_date
            if latest_date:
                days_since_update = (datetime.now() - datetime.strptime(latest_date, '%Y-%m-%d')).days
                results['days_since_update'] = days_since_update
                if days_since_update <= 1: freshness_score = 100.0
                elif days_since_update <= 3: freshness_score = 90.0
                elif days_since_update <= 7: freshness_score = 80.0
                else: freshness_score = max(0.0, 80.0 - (days_since_update - 7))
                results['freshness_score'] = freshness_score
            logging.info(f"数据新鲜度验证完成")
            return results
        except Exception as e:
            logging.error(f"数据新鲜度验证失败: {e}", exc_info=True)
            return results
    
    def generate_quality_report(self) -> Dict:
        logging.info("开始生成数据质量报告")
        report = {
            'timestamp': datetime.now().isoformat(),
            'completeness': self.validate_data_completeness(),
            'consistency': self.validate_data_consistency(),
            'freshness': self.validate_data_freshness(),
            'overall_score': 0.0,
            'recommendations': []
        }
        
        overall_score = (
            report['completeness'].get('data_quality_score', 0) * 0.5 +
            report['freshness'].get('freshness_score', 0) * 0.5
        )
        report['overall_score'] = overall_score
        
        # Recommendations logic can be improved here
        
        self._save_quality_report(report)
        logging.info(f"数据质量报告生成完成，总体评分: {overall_score:.2f}")
        return report

    def _save_quality_report(self, report: Dict) -> None:
        try:
            os.makedirs(DB_FOLDER, exist_ok=True)
            report_path = os.path.join(DB_FOLDER, 'quality_report.json')
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            logging.info(f"质量报告已保存到: {report_path}")
        except Exception as e:
            logging.error(f"保存质量报告失败: {e}", exc_info=True)