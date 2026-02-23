@echo off
REM Quick test runner for Phase 5 database validation
REM Run all Phase 5 message expiry tests against PostgreSQL, CrateDB, and MongoDB

cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File ".\run_all_db_tests.ps1" %*
