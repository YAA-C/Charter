@echo off
set req_python_version=3.10.0
for /f "delims=" %%i in ('py --version 2^>^&1') do set python_version=%%i

if not "%python_version%" == "Python %req_python_version%" (
    echo Install Python %req_python_version%
    exit
)

if not exist "venv\" (
    echo Creating Virtual Environment...
    py -m venv venv
)

call .\venv\Scripts\activate

echo Checking and downloading dependencies
pip install -q -r requirements.txt

call deactivate

echo Setup completed Successfully...