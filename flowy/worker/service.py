#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy Worker 服务模块

实现分布式任务执行的 Worker 服务。
"""

import json
import os
import signal
import socket
import time
import uuid
from datetime import datetime
from typing import List, Optional

from flowy.core.config import get_config
from flowy.core.db import get_session, Worker, FlowHistory
from flowy.core.flow import execute_flow
from flowy.core.logger import get_logger
from flowy.core.payload import get_flow_input, save_flow_payload
from flowy.worker.heartbeat import HeartbeatManager

# 获取 Worker 专用日志器
logger = get_logger('worker', console_output=True)


class WorkerService:
    """Worker 服务类
    
    负责轮询和执行匹配标签的任务。
    
    Attributes:
        worker_id: Worker 唯一标识
        tags: Worker 能力标签列表
        poll_interval: 轮询间隔（秒）
        status: Worker 状态 (online/offline/draining)
    """
    
    def __init__(
        self,
        tags: Optional[List[str]] = None,
        worker_id: Optional[str] = None,
        poll_interval: int = 2
    ):
        """初始化 Worker 服务
        
        Args:
            tags: Worker 能力标签列表，默认为空列表
            worker_id: Worker ID，默认自动生成
            poll_interval: 轮询间隔（秒），默认 2 秒
        """
        self.worker_id = worker_id or self._generate_worker_id()
        self.tags = tags or []
        self.poll_interval = poll_interval
        self.status = 'online'
        self._current_task: Optional[FlowHistory] = None
        self._shutdown_requested = False
        self._heartbeat_manager = None
    
    @staticmethod
    def _generate_worker_id() -> str:
        """生成唯一 Worker ID
        
        格式: worker-{hostname}-{pid}-{uuid8}
        - hostname: 主机名（截断到20字符）
        - pid: 进程ID
        - uuid8: 8字符的UUID hex字符串
        
        Returns:
            生成的 Worker ID
        """
        # 获取主机名并截断到20字符
        hostname = socket.gethostname()[:20]
        # 清理主机名中的特殊字符，只保留字母数字和连字符
        hostname = ''.join(c if c.isalnum() or c == '-' else '_' for c in hostname)
        
        pid = os.getpid()
        uuid8 = uuid.uuid4().hex[:8]
        
        return f"worker-{hostname}-{pid}-{uuid8}"
    
    def _register(self) -> None:
        """注册 Worker 到数据库
        
        在数据库中创建或更新 Worker 记录。
        """
        session = get_session()
        try:
            now = datetime.now()
            
            # 检查是否已存在
            existing = session.query(Worker).filter(
                Worker.id == self.worker_id
            ).first()
            
            if existing:
                # 更新现有记录
                existing.hostname = socket.gethostname()
                existing.tags = json.dumps(self.tags)
                existing.status = 'online'
                existing.last_heartbeat = now
                logger.info(f"Worker {self.worker_id} 重新注册")
            else:
                # 创建新记录
                worker = Worker(
                    id=self.worker_id,
                    hostname=socket.gethostname(),
                    tags=json.dumps(self.tags),
                    status='online',
                    last_heartbeat=now,
                    registered_at=now
                )
                session.add(worker)
                logger.info(f"Worker {self.worker_id} 注册成功")
            
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Worker 注册失败: {e}")
            raise
        finally:
            session.close()
    
    def _build_tag_condition(self, session):
        """构建标签匹配条件
        
        Worker 的标签与任务的 target_tags 需要有交集才能匹配。
        
        Args:
            session: 数据库会话
            
        Returns:
            SQLAlchemy 条件表达式
        """
        from sqlalchemy import or_, and_
        
        if not self.tags:
            # 没有标签的 Worker 只能执行没有 target_tags 的任务
            return FlowHistory.target_tags.is_(None)
        
        # 有标签的 Worker 可以执行:
        # 1. 没有 target_tags 的任务
        # 2. target_tags 与 Worker tags 有交集的任务
        conditions = [FlowHistory.target_tags.is_(None)]
        
        # 对于每个 Worker 标签，检查是否在任务的 target_tags 中
        # 使用 LIKE 进行简单的 JSON 数组包含检查
        for tag in self.tags:
            # JSON 数组中的标签格式: "tag" 或 ["tag", ...]
            conditions.append(
                FlowHistory.target_tags.like(f'%"{tag}"%')
            )
        
        return or_(*conditions)
    
    def poll_and_claim(self) -> Optional[FlowHistory]:
        """轮询并认领任务
        
        使用 FOR UPDATE SKIP LOCKED 防止并发认领。
        
        Returns:
            认领到的任务，如果没有可用任务则返回 None
        """
        from sqlalchemy import or_, and_
        
        session = get_session()
        try:
            # 构建标签匹配条件
            tag_condition = self._build_tag_condition(session)
            
            # 查询待认领任务
            # 优先级: target_worker 匹配 > 标签匹配
            # 排序: 优先级降序，创建时间升序
            query = session.query(FlowHistory).filter(
                FlowHistory.status == 'queued',
                or_(
                    # 直接指定给此 Worker
                    FlowHistory.target_worker == self.worker_id,
                    # 或者没有指定 Worker 且标签匹配
                    and_(
                        FlowHistory.target_worker.is_(None),
                        tag_condition
                    )
                )
            ).order_by(
                FlowHistory.priority.desc(),
                FlowHistory.created_at.asc()
            )
            
            # 使用 FOR UPDATE SKIP LOCKED 防止并发认领
            # 注意: SQLite 不支持此语法，仅在 PostgreSQL/MySQL 中有效
            config = get_config()
            if config.is_distributed:
                query = query.with_for_update(skip_locked=True)
            
            task = query.first()
            
            if task:
                # 认领任务
                task.status = 'claimed'
                task.claimed_by = self.worker_id
                task.claimed_at = datetime.now()
                session.commit()
                
                logger.info(
                    f"Worker {self.worker_id} 认领任务 {task.id} "
                    f"(flow_id={task.flow_id}, priority={task.priority})"
                )
                
                # 返回前刷新对象以确保数据完整
                session.refresh(task)
                return task
            
            return None
            
        except Exception as e:
            session.rollback()
            logger.warning(f"任务认领失败: {e}")
            return None
        finally:
            session.close()
    
    def execute(self, task: FlowHistory) -> None:
        """执行任务
        
        更新任务状态: claimed → running → completed/failed
        
        Args:
            task: 要执行的任务
        """
        session = get_session()
        try:
            # 重新获取任务以确保在当前会话中
            task = session.query(FlowHistory).filter(
                FlowHistory.id == task.id
            ).first()
            
            if not task:
                logger.error(f"任务 {task.id} 不存在")
                return
            
            # 更新状态为 running
            task.status = 'running'
            task.start_time = datetime.now()
            session.commit()
            
            logger.info(f"开始执行任务 {task.id} (flow_id={task.flow_id})")
            
            # 从 payload 库获取输入数据
            input_data = get_flow_input(task.id) or {}
            
            # 执行工作流
            result = execute_flow(
                flow_id=task.flow_id,
                input_data=input_data,
                metadata={'flow_history_id': task.id}
            )
            
            # 更新任务状态
            task.end_time = datetime.now()
            
            if result.get('success'):
                task.status = 'completed'
                # 保存输出数据到 payload 库
                save_flow_payload(history_id=task.id, output_data=json.dumps(result))
                logger.info(f"任务 {task.id} 执行成功")
            else:
                task.status = 'failed'
                # 保存错误信息到 payload 库
                save_flow_payload(history_id=task.id, output_data=json.dumps({
                    'error': result.get('error', 'Unknown error'),
                    'traceback': result.get('traceback')
                }))
                logger.error(f"任务 {task.id} 执行失败: {result.get('error')}")
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            
            # 尝试标记任务为失败
            try:
                task = session.query(FlowHistory).filter(
                    FlowHistory.id == task.id
                ).first()
                if task:
                    task.status = 'failed'
                    task.end_time = datetime.now()
                    # 保存错误信息到 payload 库
                    save_flow_payload(history_id=task.id, output_data=json.dumps({
                        'error': f'{type(e).__name__}: {str(e)}'
                    }))
                    session.commit()
            except Exception:
                session.rollback()
            
            logger.error(f"任务执行异常: {e}")
        finally:
            session.close()
    
    def _setup_signal_handlers(self) -> None:
        """设置信号处理器
        
        捕获 SIGTERM 和 SIGINT 信号以实现优雅下线。
        """
        def handle_shutdown(signum, frame):
            signal_name = 'SIGTERM' if signum == signal.SIGTERM else 'SIGINT'
            logger.info(f"收到 {signal_name} 信号，开始优雅下线...")
            self._shutdown_requested = True
            self.status = 'draining'
            
            # 更新数据库中的状态
            session = get_session()
            try:
                worker = session.query(Worker).filter(
                    Worker.id == self.worker_id
                ).first()
                if worker:
                    worker.status = 'draining'
                    session.commit()
            except Exception as e:
                session.rollback()
                logger.error(f"更新 Worker 状态失败: {e}")
            finally:
                session.close()
        
        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)
    
    def _main_loop(self) -> None:
        """主循环
        
        持续轮询和执行任务，直到收到关闭信号。
        
        draining 模式行为:
        - 不接受新任务
        - 继续执行当前任务直到完成
        - 当前任务完成后设置状态为 offline
        """
        logger.info(f"Worker {self.worker_id} 开始轮询任务...")
        logger.info(f"标签: {self.tags}")
        logger.info(f"轮询间隔: {self.poll_interval} 秒")
        
        while True:
            # 检查是否应该退出循环
            if self._shutdown_requested or self.status == 'draining':
                # draining 模式下，如果没有正在执行的任务，则退出
                if self._current_task is None:
                    logger.info("Worker 处于 draining 模式，无正在执行的任务，准备下线...")
                    break
                else:
                    # 有正在执行的任务，等待其完成
                    logger.info("Worker 处于 draining 模式，等待当前任务完成...")
                    time.sleep(1)
                    continue
            
            # 轮询任务
            task = self.poll_and_claim()
            
            if task:
                self._current_task = task
                try:
                    self.execute(task)
                finally:
                    self._current_task = None
                    
                    # 任务完成后检查是否处于 draining 模式
                    if self.status == 'draining' or self._shutdown_requested:
                        logger.info("当前任务已完成，Worker 处于 draining 模式，准备下线...")
                        break
            else:
                # 没有任务，等待一段时间后重试
                time.sleep(self.poll_interval)
        
        # 停止心跳管理器
        self._stop_heartbeat()
        
        # 更新状态为 offline
        self._set_offline()
        logger.info(f"Worker {self.worker_id} 已下线")
    
    def _set_offline(self) -> None:
        """设置 Worker 为离线状态"""
        session = get_session()
        try:
            worker = session.query(Worker).filter(
                Worker.id == self.worker_id
            ).first()
            if worker:
                worker.status = 'offline'
                session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"设置 Worker 离线状态失败: {e}")
        finally:
            session.close()
    
    def start(self) -> None:
        """启动 Worker
        
        执行以下步骤:
        1. 注册 Worker
        2. 启动心跳管理器
        3. 设置信号处理器
        4. 启动主循环
        """
        logger.info(f"启动 Worker: {self.worker_id}")
        
        # 注册 Worker
        self._register()
        
        # 启动心跳管理器
        self._start_heartbeat()
        
        # 设置信号处理器
        self._setup_signal_handlers()
        
        # 启动主循环
        self._main_loop()
    
    def _start_heartbeat(self) -> None:
        """启动心跳管理器"""
        config = get_config()
        self._heartbeat_manager = HeartbeatManager(
            worker_id=self.worker_id,
            interval=config.worker_heartbeat_interval
        )
        self._heartbeat_manager.start()
        logger.info(f"心跳管理器已启动，间隔: {config.worker_heartbeat_interval}s")
    
    def _stop_heartbeat(self) -> None:
        """停止心跳管理器"""
        if self._heartbeat_manager:
            self._heartbeat_manager.stop()
            self._heartbeat_manager = None
            logger.info("心跳管理器已停止")
    
    def stop(self) -> None:
        """停止 Worker
        
        请求优雅关闭:
        1. 设置状态为 draining
        2. 更新数据库中的状态
        3. 主循环会在当前任务完成后退出
        """
        logger.info(f"请求停止 Worker: {self.worker_id}")
        self._shutdown_requested = True
        self.status = 'draining'
        
        # 更新数据库中的状态为 draining
        session = get_session()
        try:
            worker = session.query(Worker).filter(
                Worker.id == self.worker_id
            ).first()
            if worker:
                worker.status = 'draining'
                session.commit()
                logger.info(f"Worker {self.worker_id} 状态已更新为 draining")
        except Exception as e:
            session.rollback()
            logger.error(f"更新 Worker 状态失败: {e}")
        finally:
            session.close()
