#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主数据收集器 V3.5 (最终完整版)
"""

import os
import sys
import logging
import argparse
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
import json
from sqlalchemy import create_engine, text
import akshare as ak

# 导入自定义模块
from enhanced_history_pipeline import EnhancedBondDataCollector
from data_quality_validator import DataQualityValidator
from data_source_manager import DataSourceManager

# 配置
DB_FOLDER = 'data'
DB_FILE = 'cb_data.db'
DB_PATH = os.path.join(DB_FOLDER, DB_FILE)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)


class MasterDataCollector:
    """主数据收集器"""
    
    def __init__(self):
        self.engine = None
        self.data_source_manager = DataSourceManager()
        self.quality_validator = DataQualityValidator()
        self.bond_collector = EnhancedBondDataCollector()
        self.trade_calendar = None

    def _get_trade_calendar(self):
        """获取并缓存交易日历"""
        if self.trade_calendar is None:
            try:
                self.trade_calendar = ak.tool_trade_date_hist_sina()
                self.trade_calendar['trade_date'] = pd.to_datetime(self.trade_calendar['trade_date']).dt.strftime('%Y-%m-%d')
            except Exception as e:
                logging.error(f"获取交易日历失败: {e}")
                self.trade_calendar = pd.DataFrame()
        return self.trade_calendar

    def initialize_database(self):
        """初始化数据库"""
        logging.info("初始化数据库...")
        os.makedirs(DB_FOLDER, exist_ok=True)
        self.engine = create_engine(f'sqlite:///{DB_PATH}')
        with self.engine.connect() as connection:
            with connection.begin():
                # 创建历史数据表
                create_history_table_sql = """
                CREATE TABLE IF NOT EXISTS cb_daily_history (
                    trade_date TEXT NOT NULL, bond_code TEXT NOT NULL, bond_name TEXT, price REAL, price_chg_pct REAL, open_price REAL,
                    high_price REAL, low_price REAL, volume REAL, turnover REAL, turnover_rate REAL, stock_code TEXT, stock_name TEXT,
                    stock_price REAL, stock_chg_pct REAL, stock_pb REAL, conv_price REAL, conv_value REAL, premium_rate REAL,
                    pure_bond_value REAL, pure_bond_premium_rate REAL, double_low REAL, bond_rating TEXT, put_trigger_price REAL,
                    force_redeem_trigger_price REAL, conv_proportion REAL, maturity_date TEXT, remaining_years REAL,
                    remaining_size REAL, ytm_before_tax REAL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (trade_date, bond_code)
                );
                """
                connection.execute(text(create_history_table_sql))
                # 创建最新数据表
                create_latest_table_sql = """
                CREATE TABLE IF NOT EXISTS convertible_bond_data (
                    bond_code TEXT PRIMARY KEY, bond_name TEXT, price REAL, price_chg_pct REAL, stock_code TEXT, stock_name TEXT,
                    stock_price REAL, stock_chg_pct REAL, stock_pb REAL, conv_price REAL, conv_value REAL, premium_rate REAL,
                    bond_rating TEXT, put_trigger_price REAL, force_redeem_trigger_price REAL, conv_proportion REAL,
                    maturity_date TEXT, remaining_years REAL, remaining_size REAL, turnover REAL, turnover_rate REAL,
                    ytm_before_tax REAL, double_low REAL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                connection.execute(text(create_latest_table_sql))
                # 创建债券信息表
                create_info_table_sql = """
                CREATE TABLE IF NOT EXISTS bond_info (
                    bond_code TEXT PRIMARY KEY, bond_name TEXT, stock_code TEXT, stock_name TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                connection.execute(text(create_info_table_sql))
        logging.info("数据库初始化完成")

    # --- 核心修正：恢复被遗漏的方法 ---
    def collect_latest_data(self) -> Dict:
        """收集最新数据"""
        logging.info("开始收集最新数据...")
        result = {'success': False, 'bonds_collected': 0, 'errors': []}
        try:
            if self.engine is None: self.initialize_database()
            bond_list = self.data_source_manager.get_bond_list_with_fallback()
            if bond_list.empty:
                result['errors'].append("无法获取债券列表")
                return result
            self._save_bond_info(bond_list)
            self._save_latest_data(bond_list)
            result['success'] = True
            result['bonds_collected'] = len(bond_list)
            logging.info(f"最新数据收集完成，共收集 {len(bond_list)} 只债券")
        except Exception as e:
            error_msg = f"收集最新数据失败: {e}"
            logging.error(error_msg, exc_info=True)
            result['errors'].append(error_msg)
        return result

    def _save_bond_info(self, bond_list: pd.DataFrame):
        if bond_list.empty: return
        try:
            info_df = bond_list[['bond_code', 'bond_name', 'stock_code', 'stock_name']].drop_duplicates(subset=['bond_code'])
            info_df.to_sql('bond_info', con=self.engine, if_exists='replace', index=False)
            logging.info(f"债券信息保存完成，共 {len(info_df)} 只债券")
        except Exception as e:
            logging.error(f"保存债券信息失败: {e}", exc_info=True)
    
    def _save_latest_data(self, bond_list: pd.DataFrame):
        if bond_list.empty: return
        try:
            bond_list.to_sql('convertible_bond_data', con=self.engine, if_exists='replace', index=False)
            logging.info(f"最新数据保存完成，共 {len(bond_list)} 只债券")
        except Exception as e:
            logging.error(f"保存最新数据失败: {e}", exc_info=True)

    def get_missing_dates(self) -> List[str]:
        with self.engine.connect() as connection:
            latest_date_in_db = connection.execute(text("SELECT MAX(trade_date) FROM cb_daily_history")).scalar_one_or_none()

        if latest_date_in_db is None:
            logging.warning("历史数据表为空，将不会进行断点回补。请先运行一次 'historical' 或 'full' 模式。")
            return []

        start_date = (datetime.strptime(latest_date_in_db, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        today_str = datetime.now().strftime('%Y-%m-%d')
        
        trade_calendar = self._get_trade_calendar()
        if trade_calendar.empty:
            logging.error("无法获取交易日历，跳过断点回补。")
            return []

        trade_dates_series = trade_calendar['trade_date']
        mask = (trade_dates_series >= start_date) & (trade_dates_series <= today_str)
        missing_dates = trade_dates_series[mask].tolist()
        return missing_dates
            
    def _archive_latest_to_history_and_backfill(self):
        if self.engine is None: self.initialize_database()

        trade_calendar = self._get_trade_calendar()
        if trade_calendar.empty:
            logging.error("无法获取交易日历，无法执行存档任务。")
            return
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        latest_trade_date = trade_calendar[trade_calendar['trade_date'] <= today_str]['trade_date'].max()
        
        if not latest_trade_date:
            logging.error("无法确定最新的交易日。")
            return
        
        logging.info(f"确定的最新交易日为: {latest_trade_date}")

        missing_dates = self.get_missing_dates()
        if latest_trade_date in missing_dates:
            missing_dates.remove(latest_trade_date)

        if missing_dates:
            logging.info(f"发现缺失的历史交易日: {missing_dates}，开始进行数据回补...")
            bond_list = self.bond_collector.get_all_bonds_list()
            if not bond_list.empty:
                for trade_date in missing_dates:
                    logging.info(f"正在为日期 {trade_date} 回补数据...")
                    for _, bond_info in bond_list.iterrows():
                        bond_data = self.bond_collector.collect_comprehensive_bond_data(bond_info)
                        if not bond_data.empty:
                            date_specific_data = bond_data[bond_data['trade_date'] == trade_date]
                            if not date_specific_data.empty:
                                self.bond_collector.save_to_database(date_specific_data, 'cb_daily_history')
            else:
                logging.error("无法获取债券列表，跳过历史数据回补。")

        logging.info(f"开始采集并存档 {latest_trade_date} 的最新数据...")
        latest_result = self.collect_latest_data()
        if not latest_result.get('success'):
            logging.error("最新数据采集失败，无法存档。")
            return

        try:
            with self.engine.connect() as connection:
                with connection.begin():
                    latest_df = pd.read_sql_table('convertible_bond_data', connection)
                    if latest_df.empty:
                        logging.warning("最新数据表为空，跳过存档。")
                        return

                    latest_df['trade_date'] = latest_trade_date
                    table_info = pd.read_sql("SELECT * FROM cb_daily_history LIMIT 0", connection)
                    existing_columns = table_info.columns.tolist()
                    columns_to_save = [col for col in latest_df.columns if col in existing_columns]
                    df_to_save = latest_df[columns_to_save]
                    
                    connection.execute(text(f"DELETE FROM cb_daily_history WHERE trade_date = :date"), {'date': latest_trade_date})
                    df_to_save.to_sql('cb_daily_history', con=connection, if_exists='append', index=False)
                    logging.info(f"成功将 {len(df_to_save)} 条最新数据存档到日期 {latest_trade_date}")

                    logging.info("开始用最新静态数据回填历史记录...")
                    backfill_fields = ['bond_rating', 'put_trigger_price', 'force_redeem_trigger_price', 'conv_price', 'maturity_date', 'stock_pb']
                    backfill_data = latest_df[['bond_code'] + [col for col in backfill_fields if col in latest_df.columns]]
                    
                    for field in backfill_fields:
                        if field not in backfill_data.columns: continue
                        logging.info(f"正在回填字段: {field}...")
                        for _, row in backfill_data.iterrows():
                            bond_code, value_to_fill = row['bond_code'], row[field]
                            if pd.notna(value_to_fill):
                                update_sql = text(f"UPDATE cb_daily_history SET {field} = :value WHERE bond_code = :bond_code AND {field} IS NULL")
                                connection.execute(update_sql, {'value': value_to_fill, 'bond_code': bond_code})
                    logging.info(f"历史回填完成。")
        except Exception as e:
            logging.error(f"存档与回填过程失败: {e}", exc_info=True)
            
    def run_full_collection(self, max_workers: int = 5) -> Dict:
        logging.info("====== 开始完整数据收集流程 ======")
        results = {'timestamp': datetime.now().isoformat(), 'latest_data': {}, 'historical_data': {}, 
                   'quality_validation': {}, 'statistics': {}, 'overall_success': False}
        try:
            self.initialize_database()
            results['historical_data'] = self.collect_historical_data(max_workers)
            self._archive_latest_to_history_and_backfill()
            results['quality_validation'] = self.validate_data_quality()
            results['statistics'] = self.get_data_statistics()
            self.data_source_manager.save_source_status_report()
            results['overall_success'] = results['historical_data'].get('success', False)
            logging.info("====== 完整数据收集流程完成 ======")
        except Exception as e:
            error_msg = f"完整数据收集流程失败: {e}"
            logging.error(error_msg, exc_info=True)
            results['error'] = error_msg
        
        self._save_collection_report(results)
        return results

    def collect_historical_data(self, max_workers: int = 5) -> Dict:
        logging.info("开始收集历史数据...")
        result = {'success': False, 'bonds_processed': 0, 'total_records': 0, 'errors': []}
        try:
            if self.engine is None: self.initialize_database()
            self.bond_collector.run_comprehensive_collection(max_workers=max_workers)
            with self.engine.connect() as connection:
                bonds_in_db = connection.execute(text("SELECT COUNT(DISTINCT bond_code) FROM cb_daily_history")).scalar_one_or_none() or 0
                total_records = connection.execute(text("SELECT COUNT(*) FROM cb_daily_history")).scalar_one_or_none() or 0
            result['success'] = True
            result['bonds_processed'] = bonds_in_db
            result['total_records'] = total_records
            logging.info(f"历史数据收集完成，数据库中现有 {bonds_in_db} 只债券，{total_records} 条记录")
        except Exception as e:
            error_msg = f"收集历史数据失败: {e}"
            logging.error(error_msg, exc_info=True)
            result['errors'].append(error_msg)
        return result

    def validate_data_quality(self) -> Dict:
        logging.info("开始验证数据质量...")
        try:
            if self.engine is None: self.initialize_database()
            report = self.quality_validator.generate_quality_report()
            logging.info(f"数据质量验证完成，总体评分: {report.get('overall_score', 0):.2f}/100")
            return report
        except Exception as e:
            error_msg = f"数据质量验证失败: {e}"
            logging.error(error_msg, exc_info=True)
            return {'error': error_msg}
    
    def get_data_statistics(self) -> Dict:
        logging.info("获取数据统计信息...")
        stats = {'total_bonds': 0, 'total_records': 0, 'date_range': {}, 'latest_update': None, 'data_sources': {}}
        try:
            if self.engine is None: self.initialize_database()
            with self.engine.connect() as connection:
                stats['total_bonds'] = connection.execute(text("SELECT COUNT(DISTINCT bond_code) FROM cb_daily_history")).scalar_one_or_none() or 0
                stats['total_records'] = connection.execute(text("SELECT COUNT(*) FROM cb_daily_history")).scalar_one_or_none() or 0
                date_info = connection.execute(text("SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date) FROM cb_daily_history")).fetchone()
                if date_info and date_info[0]:
                    stats['date_range'] = {'start_date': date_info[0], 'end_date': date_info[1], 'trading_days': date_info[2]}
                stats['latest_update'] = connection.execute(text("SELECT MAX(updated_at) FROM cb_daily_history")).scalar_one_or_none()
            stats['data_sources'] = self.data_source_manager.get_source_status_report()
            logging.info(f"数据统计信息获取完成")
        except Exception as e:
            error_msg = f"获取数据统计信息失败: {e}"
            logging.error(error_msg, exc_info=True)
            stats['error'] = error_msg
        return stats

    def _save_collection_report(self, results: Dict):
        try:
            report_path = os.path.join(DB_FOLDER, 'collection_report.json')
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)
            logging.info(f"收集结果报告已保存到: {report_path}")
        except Exception as e:
            logging.error(f"保存收集结果报告失败: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description='可转债历史数据收集器')
    parser.add_argument('--mode', choices=['latest', 'historical', 'quality', 'full', 'archive'], default='archive', help='运行模式: archive为日常模式，只采集并存档最新数据，并回补断点')
    parser.add_argument('--workers', type=int, default=5, help='并发工作线程数 (主要用于 historical 和 full 模式)')
    parser.add_argument('--verbose', action='store_true', help='详细输出')
    args = parser.parse_args()
    
    if args.verbose: logging.getLogger().setLevel(logging.DEBUG)
    
    collector = MasterDataCollector()
    result = {}
    
    # 重新定义main函数体以正确调用方法
    if args.mode == 'latest':
        result = collector.collect_latest_data()
    elif args.mode == 'historical':
        result = collector.collect_historical_data(args.workers)
    elif args.mode == 'quality':
        result = collector.validate_data_quality()
    elif args.mode == 'full':
        result = collector.run_full_collection(args.workers)
    elif args.mode == 'archive':
        collector.initialize_database()
        collector._archive_latest_to_history_and_backfill()
        result = {"status": "archive and backfill process completed."}
    
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

if __name__ == '__main__':
    # 调用 main 函数
    main()