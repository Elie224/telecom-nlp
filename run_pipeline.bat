@echo off
cd /d C:\Users\KOURO\Desktop\telecom-nlp
call C:\Users\KOURO\anaconda3\Scripts\activate.bat telecom-nlp
python run_pipeline.py %* >> logs\scheduler.log 2>&1
