@echo off
cls

REM Check if the Kafka path argument is provided
IF "%1"=="" (
    echo ERROR: Please provide the path to the Kafka installation folder as an argument.
    pause
    exit /b
)

set KAFKA_DIR=%1

REM Store the current directory
set CURRENT_DIR=%cd%

REM Change to the Kafka directory
cd /d %KAFKA_DIR%

REM Check available disk space on the drive where Kafka is located
for /f "tokens=3" %%a in ('fsutil volume diskfree C:') do set DISK_SPACE=%%a
if %DISK_SPACE% lss 1000000 (
    echo ERROR: Not enough disk space.
    pause
    exit /b
)

REM Check if Kafka log directory exists and is accessible
if not exist kafka-logs (
    echo ERROR: Kafka log directory does not exist.
    pause
    exit /b
)
echo Kafka log directory exists.

REM Stop any running Zookeeper process on port 2181
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :2181') do (
    taskkill /PID %%a /F
)

REM Stop any running Kafka process on port 9092
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :9092') do (
    taskkill /PID %%a /F
)

REM Delete the .lock file if it exists
if exist kafka-logs\.lock (
    echo Deleting .lock file...
    del kafka-logs\.lock
)

REM Clean up Kafka and Zookeeper directories
echo Cleaning up Kafka log directories...
rd /s /q kafka-logs
md kafka-logs

echo Cleaning up Zookeeper data directories...
rd /s /q zookeeper-data
md zookeeper-data

REM Start Zookeeper in a new terminal window
echo Starting Zookeeper...
start cmd /k "bin\windows\zookeeper-server-start.bat config\zookeeper.properties"
timeout /t 15 /nobreak

REM Check if Zookeeper is running on port 2181
echo Checking if Zookeeper is running...
set ZOOKEEPER_RUNNING=0
for /L %%i in (1,1,30) do (
    netstat -ano | findstr :2181 >nul 2>&1
    if %errorlevel%==0 (
        set ZOOKEEPER_RUNNING=1
        echo Zookeeper is running.
        goto :ZOOKEEPER_CHECK_DONE
    )
    timeout /t 1 /nobreak >nul
)
:ZOOKEEPER_CHECK_DONE

if %ZOOKEEPER_RUNNING%==0 (
    echo ERROR: Zookeeper failed to start.
    pause
    exit /b
)

REM Clean up Zookeeper nodes related to Kafka
echo Cleaning up Zookeeper nodes related to Kafka...
start cmd /k "bin\windows\zookeeper-shell.bat localhost:2181 rmr /brokers/ids"
timeout /t 5 /nobreak

REM Start Kafka server in a new terminal window
echo Starting Kafka server...
start cmd /k "bin\windows\kafka-server-start.bat config\server.properties"
timeout /t 15 /nobreak

REM Check if Kafka server is running on port 9092
echo Checking if Kafka server is running...
set KAFKA_RUNNING=0
for /L %%i in (1,1,30) do (
    netstat -ano | findstr :9092 >nul 2>&1
    if %errorlevel%==0 (
        set KAFKA_RUNNING=1
        echo Kafka server is running.
        goto :KAFKA_CHECK_DONE
    )
    timeout /t 1 /nobreak >nul
)
:KAFKA_CHECK_DONE

if %KAFKA_RUNNING%==0 (
    echo ERROR: Kafka server failed to start.
    pause
    exit /b
)

REM Check if the topic exists and create if not
call bin\windows\kafka-topics.bat --describe --topic traffic_violation_video_stream --bootstrap-server localhost:9092 >nul 2>nul || call bin\windows\kafka-topics.bat --create --topic traffic_violation_video_stream --bootstrap-server localhost:9092
call bin\windows\kafka-topics.bat --describe --topic result --bootstrap-server localhost:9092 >nul 2>nul || call bin\windows\kafka-topics.bat --create --topic result --bootstrap-server localhost:9092

REM List existing Kafka topics in a new terminal window
echo Listing existing Kafka topics...
start cmd /k "bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092"

REM Return to the original directory
cd /d %CURRENT_DIR%
