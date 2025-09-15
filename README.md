安装依赖：
pip install pandas SQLAlchemy akshare streamlit plotly numpy





全量同步
python master\_data\_collector.py --mode full

增量补数
python master\_data\_collector.py --mode archive



运行程序
cd stock\_app

执行：
python -m streamlit run data\_center.py
或者

python -m streamlit run data\_center.py --server.address 0.0.0.0

