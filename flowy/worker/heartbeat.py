#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy Worker 心跳管理模块

实现 Worker 心跳机制，定期向数据库报告存活状态。
"""

import threading
import time
from datetime import datetime
from typing import Optional

from flowy.core.config import get_config
from flowy.core.db import get_session, Worker
from flowy.core.logger import get_logger

# 获取心跳专用日志器
logger = get_logger('heartbeat', console_output=True)


class HeartbeatManager:
    """心跳管理器
    
    负责在后台线程定期发送心跳更新到数据库。
    
    Attributes:
        worker_id: Worker 唯一标识
        interval: 心跳间隔（秒）
    """
    
    def __init__(self, worker_id: str, interval: Optional[int] = None):
        """初始化心跳管理器
        
        Args:
            worker_id: Worker ID
            interval: 心跳间隔（秒），默认从配置读取
        """
        self.worker_id = worker_id
        
        # 从配置获取默认间隔
        if interval is None:
            config = get_config()
            interval = config.worker_heartbeat_interval
        
        self.interval = interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
    
    def start(self) -> None:
        """启动心跳线程
        
        在后台线程中定期发送心跳。
        """
        if self._running:
            logger.warning(f"心跳管理器已在运行中: {self.worker_id}")
            return
        
        self._running = True
        self._stop_event.clear()
        
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"heartbeat-{self.worker_id}",
            daemon=True
        )
        self._thread.start()
        
        logger.info(
            f"心跳管理器已启动: worker_id={self.worker_id}, "
            f"interval={self.interval}s"
        )
    
    def stop(self, timeout: float = 5.0) -> None:
        """停止心跳
        
        Args:
            timeout: 等待线程结束的超时时间（秒）
        """
        if not self._running:
            return
        
        logger.info(f"正在停止心跳管理器: {self.worker_id}")
        
        self._running = False
        self._stop_event.set()
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            
            if self._thread.is_alive():
                logger.warning(
                    f"心跳线程未能在 {timeout}s 内停止: {self.worker_id}"
                )
        
        self._thread = None
        logger.info(f"心跳管理器已停止: {self.worker_id}")
    
    def _heartbeat_loop(self) -> None:
        """心跳循环
        
        持续发送心跳直到收到停止信号。
        """
        logger.debug(f"心跳循环开始: {self.worker_id}")
        
        while self._running and not self._stop_event.is_set():
            try:
                self._send_heartbeat()
            except Exception as e:
                logger.warning(f"心跳发送失败: {e}")
            
            # 使用 Event.wait 而不是 time.sleep，以便能够快速响应停止信号
            self._stop_event.wait(timeout=self.interval)
        
        logger.debug(f"心跳循环结束: {self.worker_id}")
    
    def _send_heartbeat(self) -> None:
        """发送心跳
        
        更新数据库中 Worker 的 last_heartbeat 时间戳。
        """
        session = get_session()
        try:
            now = datetime.now()
            
            result = session.query(Worker).filter(
                Worker.id == self.worker_id
            ).update({
                'last_heartbeat': now
            })
            
            session.commit()
            
            if result > 0:
                logger.debug(f"心跳已发送: {self.worker_id} @ {now}")
            else:
                logger.warning(
                    f"心跳更新失败，Worker 不存在: {self.worker_id}"
                )
                
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
    
    @property
    def is_running(self) -> bool:
        """心跳管理器是否正在运行"""
        return self._running and self._thread is not None and self._thread.is_alive()
