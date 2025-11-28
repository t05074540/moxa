import os
import logging
from logging.handlers import TimedRotatingFileHandler


def setup_logger(
    name: str,
    folder_name: str,
    base_filename: str,
    level: int = logging.DEBUG,
) -> logging.Logger:
    """
    建立並回傳一個 logger：
    - log 檔案放在目前檔案同層下的 `folder_name` 資料夾裡
    - 檔名為 `<base_filename>.log`
    - 每天午夜自動 rotate，保留多個歷史檔案
    - 同時輸出到檔案與 console
    """

    # 取得 log 目錄
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_folder = os.path.join(current_dir, folder_name)

    # 若資料夾不存在就建立
    if not os.path.exists(log_folder):
        try:
            os.makedirs(log_folder)
        except OSError:
            # 多執行緒或多進程同時建目錄時，可忽略已存在的錯誤
            pass

    # log 檔完整路徑：<current_dir>/<folder_name>/<base_filename>.log
    log_filepath = os.path.join(log_folder, "{}.log".format(base_filename))

    # 取得 logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # 不往上層 logger 傳

    # 移除舊 handler，避免重複輸出
    for h in list(logger.handlers):
        logger.removeHandler(h)
        try:
            h.flush()
            if getattr(h, "close", None):
                h.close()
        except Exception:
            pass

    # 建立檔案 handler：每天午夜切檔，保留 7 個備份，UTF-8 編碼
    file_handler = TimedRotatingFileHandler(
        log_filepath,
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )

    # log 格式與時間格式
    formatter = logging.Formatter(
        "%(asctime)s,%(msecs)03d [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 同時也輸出到 console
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
