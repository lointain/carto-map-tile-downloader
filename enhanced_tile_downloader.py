import os
import requests
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import argparse
import logging
from tqdm import tqdm
from datetime import datetime

# --- 配置日志 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 预设的 CartoDB 瓦片 URL 模板 ---
TILE_URL_TEMPLATES = {
    "dark_all": "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
    "light_all": "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
    "dark_nolabels": "https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png",
    "light_nolabels": "https://{s}.basemaps.cartocdn.com/light_nolabels/{z}/{x}/{y}{r}.png",
    "dark_only_labels": "https://{s}.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}{r}.png",
    "light_only_labels": "https://{s}.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}{r}.png",
    "voyager": "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
    "voyager_nolabels": "https://{s}.basemaps.cartocdn.com/rastertiles/voyager_nolabels/{z}/{x}/{y}{r}.png",
    "voyager_only_labels": "https://{s}.basemaps.cartocdn.com/rastertiles/voyager_only_labels/{z}/{x}/{y}{r}.png",
}

# --- 瓦片服务器子域名 ---
SUBDOMAINS = ['a', 'b', 'c', 'd']

# --- 墨卡托投影的有效纬度范围，避免数学错误 ---
MERCATOR_MAX_LAT = 85.05112878


# --- 辅助函数 ---
def deg2num(lat_deg, lon_deg, zoom):
    """
    将经纬度转换为瓦片坐标 (x, y)。
    参考: https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Python
    """
    # 限制纬度在墨卡托投影有效范围内
    lat_deg = max(-MERCATOR_MAX_LAT, min(MERCATOR_MAX_LAT, lat_deg))

    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom

    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)

    # 确保瓦片X坐标在有效范围内 (0 到 2^zoom - 1)
    # 对于全球范围，X 坐标是会“循环”的，所以 % n 是一个好方法来确保在范围内
    xtile = xtile % int(n)

    # 确保瓦片Y坐标在有效范围内 (0 到 2^zoom - 1)
    # 尽管 Y 坐标通常不会像 X 那样“循环”，但为了严谨性也确保一下
    ytile = max(0, min(ytile, int(n) - 1))

    return xtile, ytile


def get_tile_range(min_lat, min_lon, max_lat, max_lon, zoom):
    """
    根据经纬度范围和缩放级别计算瓦片 X/Y 范围。
    """
    # 确保经纬度顺序正确
    if min_lat > max_lat:
        min_lat, max_lat = max_lat, min_lat
    if min_lon > max_lon:
        min_lon, max_lon = max_lon, min_lon

    n_tiles_at_zoom = 2 ** zoom

    # 核心改进：使用浮点数容差来判断是否为全球范围
    # 设定一个足够小的浮点数误差容忍度
    epsilon = 1e-6  # 容忍度可以根据需要调整，1e-6 通常足够

    # 关键修改：在判断是否为全球纬度范围时，先将输入的经纬度钳制到墨卡托投影的有效范围
    # 这样可以处理用户输入 -90/90 度的情况，使其能正确识别为“全球纬度”
    clamped_min_lat_for_check = max(-MERCATOR_MAX_LAT, min(MERCATOR_MAX_LAT, min_lat))
    clamped_max_lat_for_check = max(-MERCATOR_MAX_LAT, min(MERCATOR_MAX_LAT, max_lat))

    is_global_lon = abs(min_lon - (-180.0)) < epsilon and abs(max_lon - 180.0) < epsilon
    # 使用钳制后的纬度值进行全球范围的判断
    is_global_lat = abs(clamped_min_lat_for_check - (-MERCATOR_MAX_LAT)) < epsilon and \
                    abs(clamped_max_lat_for_check - MERCATOR_MAX_LAT) < epsilon

    if is_global_lon and is_global_lat:
        min_x = 0
        max_x = n_tiles_at_zoom - 1
        min_y = 0
        max_y = n_tiles_at_zoom - 1
        logger.info(f"检测到全球范围下载，强制瓦片范围 Z={zoom}: X=[{min_x}, {max_x}], Y=[{min_y}, {max_y}]")
    else:
        # 否则，按常规方式计算瓦片范围
        # deg2num 函数内部已经包含了纬度钳制，所以这里可以直接使用原始的 max_lat/min_lat
        x1, y1 = deg2num(max_lat, min_lon, zoom)  # 左上角 (max_lat, min_lon)
        x2, y2 = deg2num(min_lat, max_lon, zoom)  # 右下角 (min_lat, max_lon)

        # 对于 X 坐标，如果区域跨越了经度180/-180度线，min_x, max_x 的计算需要特殊考虑
        # 但对于非全球区域，如果 deg2num 已经将 X 坐标模数化到 [0, N-1]
        # 并且区域不跨越 180 度线，那么 min(x1,x2), max(x1,x2) 依然是正确的
        # 如果区域跨越 180 度线 (例如从东经170到西经-170)，这里需要更复杂的逻辑，
        # 例如计算两个区间 [x1, N-1] 和 [0, x2] 的并集。
        # 但为了简化并专注于全球下载问题，这里保持原始的 min/max 逻辑。
        # 假设用户在非全球模式下不会输入跨越180度线的经度范围。

        min_x = min(x1, x2)
        max_x = max(x1, x2)
        min_y = min(y1, y2)
        min_y = min(y1, y2)
        max_y = max(y1, y2)

        logger.info(f"缩放级别 Z={zoom} 的瓦片范围: X=[{min_x}, {max_x}], Y=[{min_y}, {max_y}]")

    return min_x, max_x, min_y, max_y


def download_tile(base_url, z, x, y, output_dir, session, retries=3, timeout=10):
    """
    下载单个瓦片，支持重试。
    """
    # 随机选择子域名
    subdomain = random.choice(SUBDOMAINS)
    r_suffix = ''  # CartoDB 通常不需要 @2x 后缀

    url = base_url.format(s=subdomain, z=z, x=x, y=y, r=r_suffix)
    filepath = os.path.join(output_dir, str(z), str(x), f"{y}.png")

    if os.path.exists(filepath):
        # logger.debug(f"瓦片已存在，跳过: {filepath}")
        return True, filepath  # 返回 True 表示成功 (已存在也算成功)

    attempt = 0
    while attempt <= retries:
        try:
            response = session.get(url, stream=True, timeout=timeout)
            response.raise_for_status()  # 检查 HTTP 错误 (2xx 成功状态码)

            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            # logger.debug(f"下载成功: {filepath}")
            return True, filepath
        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code if http_err.response else 'N/A'
            logger.error(f"HTTP 错误 {status_code}: {url} - {http_err}")
            # 对于 400 Bad Request, 404 Not Found, 403 Forbidden，不再重试，因为这些通常表示瓦片本身无效或不存在。
            if status_code in [400, 404, 403]:
                logger.error(f"瓦片无效、不存在或无权限访问，不再重试: {url}")
                return False, None
            # 其他 HTTP 错误仍然重试
            attempt += 1
            if attempt <= retries:
                logger.warning(f"下载失败 (尝试 {attempt}/{retries}): {url} - {http_err}. 正在重试...")
            else:
                logger.error(f"下载失败 (已达最大重试次数): {url} - {http_err}")
                return False, None
        except requests.exceptions.ConnectionError as conn_err:
            attempt += 1
            if attempt <= retries:
                logger.warning(f"连接错误 (尝试 {attempt}/{retries}): {url} - {conn_err}. 正在重试...")
            else:
                logger.error(f"连接失败 (已达最大重试次数): {url} - {conn_err}")
                return False, None
        except requests.exceptions.Timeout as timeout_err:
            attempt += 1
            if attempt <= retries:
                logger.warning(f"请求超时 (尝试 {attempt}/{retries}): {url} - {timeout_err}. 正在重试...")
            else:
                logger.error(f"请求超时 (已达最大重试次数): {url} - {timeout_err}")
                return False, None
        except requests.exceptions.RequestException as e:
            # 捕获所有其他 requests 异常
            attempt += 1
            if attempt <= retries:
                logger.warning(f"未知请求错误 (尝试 {attempt}/{retries}): {url} - {e}. 正在重试...")
            else:
                logger.error(f"未知请求错误 (已达最大重试次数): {url} - {e}")
                return False, None
    return False, None  # 最终失败


def download_tiles_for_zoom_level(
        base_url_template,
        zoom_level,
        min_x, max_x,
        min_y, max_y,
        output_directory,
        max_workers,
        retries,
        request_headers,
        proxies,
        total_tiles_for_all_zooms_pbar=None  # 用于全局进度条
):
    """
    下载单个缩放级别内的瓦片。
    """
    logger.info(f"开始下载缩放级别 Z={zoom_level} 的瓦片 (X: [{min_x}-{max_x}], Y: [{min_y}-{max_y}])")

    total_tiles_in_this_zoom = (max_x - min_x + 1) * (max_y - min_y + 1)
    if total_tiles_in_this_zoom <= 0:
        logger.warning(f"Z={zoom_level} 没有瓦片可供下载，请检查坐标范围。")
        return 0, 0  # 成功0，失败0

    session = requests.Session()
    session.headers.update(request_headers)  # 设置会话请求头
    session.proxies.update(proxies)  # 设置会话代理

    tasks = []
    for x in range(min_x, max_x + 1):
        for y in range(min_y, max_y + 1):
            tasks.append((zoom_level, x, y))

    successful_downloads = 0
    failed_downloads = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(
            download_tile, base_url_template, z, x, y, output_directory, session, retries
        ): (z, x, y) for z, x, y in tasks}

        # 使用 tqdm 显示当前缩放级别的进度条
        with tqdm(total=total_tiles_in_this_zoom, desc=f"Z={zoom_level}下载进度", unit="瓦片",
                  leave=False) as pbar_zoom:
            for future in as_completed(futures):
                success, filepath = future.result()
                if success:
                    successful_downloads += 1
                else:
                    failed_downloads += 1
                pbar_zoom.update(1)  # 更新当前缩放级别的进度条
                if total_tiles_for_all_zooms_pbar:
                    total_tiles_for_all_zooms_pbar.update(1)  # 更新全局进度条

    logger.info(f"Z={zoom_level} 下载完成。成功: {successful_downloads}, 失败: {failed_downloads}")
    session.close()  # 关闭会话
    return successful_downloads, failed_downloads


def main():
    parser = argparse.ArgumentParser(
        description="一个用于下载 CartoDB 底图瓦片的Python程序。支持按经纬度或瓦片XY范围下载指定缩放层级。",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # --- 基本参数组 ---
    basic_args = parser.add_argument_group('基本参数')
    basic_args.add_argument(
        "--url",
        type=str,
        default="dark_all",
        help=f"要下载的瓦片类型或完整URL模板。\n"
             f"可选项: {', '.join(TILE_URL_TEMPLATES.keys())}\n"
             f"或提供自定义URL模板 (例如: https://{{s}}.example.com/tiles/{{z}}/{{x}}/{{y}}.png)"
    )
    basic_args.add_argument(
        "--min_zoom",
        type=int,
        required=True,
        help="起始缩放级别 (Z)。"
    )
    basic_args.add_argument(
        "--max_zoom",
        type=int,
        required=True,
        help="结束缩放级别 (Z)。"
    )
    basic_args.add_argument(
        "--output",
        type=str,
        default=f"tiles_download_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        help="瓦片保存的根目录。默认为 'tiles_download_YYYYMMDD_HHMMSS' 格式的时间戳目录。"
    )

    # --- 范围参数组 ---
    range_args = parser.add_argument_group('范围参数 (二选一)')
    range_args.add_argument(
        "--min_lon",
        type=float,
        help="地理区域的最小经度 (东经)。"
    )
    range_args.add_argument(
        "--min_lat",
        type=float,
        help="地理区域的最小纬度 (北纬)。"
    )
    range_args.add_argument(
        "--max_lon",
        type=float,
        help="地理区域的最大经度 (东经)。"
    )
    range_args.add_argument(
        "--max_lat",
        type=float,
        help="地理区域的最大纬度 (北纬)。"
    )
    range_args.add_argument(
        "--min_x",
        type=int,
        help="瓦片X坐标的最小值。与经纬度参数互斥。"
    )
    range_args.add_argument(
        "--max_x",
        type=int,
        help="瓦片X坐标的最大值。与经纬度参数互斥。"
    )
    range_args.add_argument(
        "--min_y",
        type=int,
        help="瓦片Y坐标的最小值。与经纬度参数互斥。"
    )
    range_args.add_argument(
        "--max_y",
        type=int,
        help="瓦片Y坐标的最大值。与经纬度参数互斥。"
    )

    # --- 高级参数组 ---
    advanced_args = parser.add_argument_group('高级参数')
    advanced_args.add_argument(
        "--workers",
        type=int,
        default=10,
        help="用于并发下载的最大线程数 (默认为10)。"
    )
    advanced_args.add_argument(
        "--retries",
        type=int,
        default=3,
        help="下载失败时的重试次数 (默认为3)。"
    )
    advanced_args.add_argument(
        "--user_agent",
        type=str,
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        help="自定义 User-Agent 请求头。模拟浏览器访问。"
    )
    advanced_args.add_argument(
        "--http_proxy",
        type=str,
        help="HTTP 代理地址 (例如: http://user:pass@host:port)。"
    )
    advanced_args.add_argument(
        "--https_proxy",
        type=str,
        help="HTTPS 代理地址 (例如: https://user:pass@host:port)。"
    )

    args = parser.parse_args()

    # --- 验证输入参数 ---
    # 确定瓦片 URL 模板
    base_url_template = TILE_URL_TEMPLATES.get(args.url, args.url)
    if not base_url_template:
        logger.error(f"无效的瓦片URL或类型: {args.url}")
        return

    # 确保缩放级别范围有效
    if args.min_zoom < 0 or args.max_zoom < args.min_zoom:
        logger.error("--min_zoom 和 --max_zoom 必须是非负整数，并且 --min_zoom 不能大于 --max_zoom。")
        return

    # 确定瓦片 X/Y 范围的逻辑 (互斥性检查)
    coord_params_specified = all([arg is not None for arg in [args.min_lat, args.min_lon, args.max_lat, args.max_lon]])
    tile_params_specified = all([arg is not None for arg in [args.min_x, args.max_x, args.min_y, args.max_y]])

    if coord_params_specified and tile_params_specified:
        logger.error("不能同时指定经纬度范围和瓦片X/Y范围。请选择一种方式。")
        return
    elif not (coord_params_specified or tile_params_specified):
        logger.error(
            "请提供经纬度范围 (--min_lat --min_lon --max_lat --max_lon) 或瓦片X/Y范围 (--min_x --max_x --min_y --max_y)。")
        return

    # --- 设置请求头和代理 ---
    request_headers = {'User-Agent': args.user_agent}
    proxies = {}
    if args.http_proxy:
        proxies['http'] = args.http_proxy
        logger.info(f"使用 HTTP 代理: {args.http_proxy}")
    if args.https_proxy:
        proxies['https'] = args.https_proxy
        logger.info(f"使用 HTTPS 代理: {args.https_proxy}")

    # --- 计算所有层级总瓦片数量，用于全局进度条 ---
    total_tiles_overall = 0
    zoom_level_ranges = {}
    for z in range(args.min_zoom, args.max_zoom + 1):
        if coord_params_specified:
            min_x, max_x, min_y, max_y = get_tile_range(
                args.min_lat, args.min_lon, args.max_lat, args.max_lon, z
            )
            # 经纬度参数检查 (针对用户输入，而不是deg2num内部修正后的值)
            if not (-180 <= args.min_lon <= 180 and not abs(abs(args.min_lon) - 180) < 1e-6 and \
                    -180 <= args.max_lon <= 180 and not abs(abs(args.max_lon) - 180) < 1e-6):
                logger.warning(
                    f"经度参数 {args.min_lon}, {args.max_lon} 超出有效范围 [-180, 180]，可能会导致瓦片坐标计算不准确。")
            # 纬度参数检查，只警告用户，不影响内部对MERCATOR_MAX_LAT的钳制
            if not (-90 <= args.min_lat <= 90 and not abs(abs(args.min_lat) - 90) < 1e-6 and \
                    -90 <= args.max_lat <= 90 and not abs(abs(args.max_lat) - 90) < 1e-6):
                logger.warning(
                    f"纬度参数 {args.min_lat}, {args.max_lat} 超出 [-90, 90] 范围，内部将限制在墨卡托投影有效范围 ({MERCATOR_MAX_LAT})。")

        else:  # tile_params_specified
            min_x, max_x, min_y, max_y = args.min_x, args.max_x, args.min_y, args.max_y
            # 简单验证瓦片坐标是否合理 (例如：min <= max)
            if min_x > max_x or min_y > max_y:
                logger.error(f"瓦片X/Y范围无效: X=[{min_x}, {max_x}], Y=[{min_y}, {max_y}]。请确保最小值不大于最大值。")
                return

        num_tiles_in_zoom = (max_x - min_x + 1) * (max_y - min_y + 1)
        if num_tiles_in_zoom < 0:  # 避免负数瓦片数量，如果范围有误
            num_tiles_in_zoom = 0
        total_tiles_overall += num_tiles_in_zoom
        zoom_level_ranges[z] = (min_x, max_x, min_y, max_y)

    if total_tiles_overall == 0:
        logger.warning("在所有指定缩放级别内，没有瓦片可供下载。请检查输入范围。")
        return

    logger.info(f"预计总共下载 {total_tiles_overall} 个瓦片。")

    overall_successful_downloads = 0
    overall_failed_downloads = 0

    # --- 执行下载任务 ---
    with tqdm(total=total_tiles_overall, desc="总下载进度", unit="瓦片") as pbar_overall:
        for z in range(args.min_zoom, args.max_zoom + 1):
            min_x, max_x, min_y, max_y = zoom_level_ranges[z]

            successful_count, failed_count = download_tiles_for_zoom_level(
                base_url_template=base_url_template,
                zoom_level=z,
                min_x=min_x, max_x=max_x,
                min_y=min_y, max_y=max_y,
                output_directory=args.output,
                max_workers=args.workers,
                retries=args.retries,
                request_headers=request_headers,  # 传递请求头
                proxies=proxies,  # 传递代理设置
                total_tiles_for_all_zooms_pbar=pbar_overall
            )
            overall_successful_downloads += successful_count
            overall_failed_downloads += failed_count

    logger.info(f"\n所有缩放级别下载完成。总成功: {overall_successful_downloads}, 总失败: {overall_failed_downloads}")


if __name__ == "__main__":
    main()