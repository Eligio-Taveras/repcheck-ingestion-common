@echo off
cd /d "%~dp0"
sbt dockerTest
