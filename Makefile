.PHONY: setup seed run test lint api ui streamlit

setup:
	pip install -r requirements.txt
	./scripts/setup_lab.sh

seed:
	python python/run_sql_file.py --file sql/11_seed_lab_data.sql
	python python/run_sql_file.py --file sql/12_run_workload.sql

run:
	python python/run_copilot.py --days 7 --mode DRY_RUN --ai on --verify on

test:
	pytest -q

lint:
	python -m py_compile python/*.py ui/app.py

api:
	python -m uvicorn python.chat_api:app --reload --port 8000

ui:
	cd web && npm install && npm run dev

streamlit:
	streamlit run ui/app.py
