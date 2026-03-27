#!/bin/bash
cd /Users/danielhutchinson/pension_pe_performance
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
