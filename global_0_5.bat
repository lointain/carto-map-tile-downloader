@echo off
REM 将CMD编码设置为UTF-8，以避免乱码
chcp 65001

REM 激活虚拟环境
call .venv\Scripts\activate

REM 执行 Python 脚本，下载全球 0-5 层的瓦片
REM 纬度范围现在在Python代码内部进行限制，所以这里可以使用 -90 到 90
python enhanced_tile_downloader.py ^
    --url dark_all ^
    --min_zoom 0 ^
    --max_zoom 5 ^
    --min_lat -90.0 ^
    --min_lon -180.0 ^
    --max_lat 90.0 ^
    --max_lon 180.0 ^
    --output "Global_Tiles_Z0-5_Dark" ^
    --workers 3 ^
    --retries 3

REM 虚拟环境在脚本执行完毕后会自动失效，无需额外 deactiviate
echo 全球瓦片下载任务已完成。
pause