@echo off
REM Compiles all sources into .\out (needs JDK 17+)
cd /d "%~dp0"
if exist out rmdir /s /q out
mkdir out
dir /s /b src\*.java > out\sources.txt
javac --release 17 -d out @out\sources.txt || exit /b 1
echo Build OK -^> out\
