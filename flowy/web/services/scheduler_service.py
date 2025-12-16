#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""调度服务模块"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from flowy.core.db import get_session, Trigger, FlowHistory
from flowy.core.flow import execute_flow
from flowy.core.json_utils import json

logger = logging.getLogger(__name__)


class SchedulerService:
    """调度服务类"""
    
    _scheduler: Optional[BackgroundScheduler] = None
    _scheduler_started: bool = False
    
    @classmethod
    def init_scheduler(cls):
        """初始化调度器"""
        if cls._scheduler is None:
            executors = {
                'default': ThreadPoolExecutor(max_workers=10)
            }
            
            cls._scheduler = BackgroundScheduler(
                executors=executors,
                timezone='Asia/Shanghai',
                job_defaults={
                    'coalesce': True,  # 合并错过的执行
                    'max_instances': 1  # 同一任务最多1个实例
                }
            )
            logger.info("调度器已初始化")
    
    @classmethod
    def start_scheduler(cls):
        """启动调度器"""
        if not cls._scheduler_started:
            cls.init_scheduler()
            cls.load_triggers_from_db()
            cls._scheduler.start()
            cls._scheduler_started = True
            logger.info("调度器已启动")
    
    @classmethod
    def shutdown_scheduler(cls):
        """关闭调度器"""
        if cls._scheduler and cls._scheduler_started:
            cls._scheduler.shutdown(wait=False)
            cls._scheduler_started = False
            logger.info("调度器已关闭")
    
    @classmethod
    def add_job(cls, trigger_id: int, flow_id: str, cron_expression: str):
        """添加调度任务

        Args:
            trigger_id: 触发器ID
            flow_id: 工作流ID
            cron_expression: Cron表达式
        """
        if cls._scheduler is None:
            raise RuntimeError("调度器未初始化")

        job_id = f"trigger_{trigger_id}"

        # 检查任务是否已存在，避免重复添加
        if cls._scheduler.get_job(job_id):
            logger.warning(f"调度任务已存在，将替换: {job_id}")

        trigger = CronTrigger.from_crontab(cron_expression)

        cls._scheduler.add_job(
            func=cls.execute_trigger,
            trigger=trigger,
            id=job_id,
            args=[trigger_id],
            replace_existing=True,  # 确保替换现有任务
            misfire_grace_time=30,  # 允许30秒的误差时间
            coalesce=True,  # 合并错过的执行
            max_instances=1  # 同一任务最多1个实例
        )
        logger.info(f"添加调度任务: {job_id}, cron: {cron_expression}")
    
    @classmethod
    def remove_job(cls, trigger_id: int):
        """移除调度任务
        
        Args:
            trigger_id: 触发器ID
        """
        if cls._scheduler is None:
            return
        
        job_id = f"trigger_{trigger_id}"
        if cls._scheduler.get_job(job_id):
            cls._scheduler.remove_job(job_id)
            logger.info(f"移除调度任务: {job_id}")
    
    @classmethod
    def pause_job(cls, trigger_id: int):
        """暂停调度任务
        
        Args:
            trigger_id: 触发器ID
        """
        if cls._scheduler is None:
            return
        
        job_id = f"trigger_{trigger_id}"
        if cls._scheduler.get_job(job_id):
            cls._scheduler.pause_job(job_id)
            logger.info(f"暂停调度任务: {job_id}")
    
    @classmethod
    def resume_job(cls, trigger_id: int):
        """恢复调度任务
        
        Args:
            trigger_id: 触发器ID
        """
        if cls._scheduler is None:
            return
        
        job_id = f"trigger_{trigger_id}"
        if cls._scheduler.get_job(job_id):
            cls._scheduler.resume_job(job_id)
            logger.info(f"恢复调度任务: {job_id}")
    
    @classmethod
    def load_triggers_from_db(cls):
        """从数据库加载触发器"""
        session = get_session()
        try:
            triggers = session.query(Trigger).filter(
                Trigger.enabled == 1
            ).all()

            logger.info(f"从数据库加载 {len(triggers)} 个启用的触发器")

            # 先清理所有现有的trigger任务
            existing_jobs = cls._scheduler.get_jobs()
            for job in existing_jobs:
                if job.id.startswith('trigger_'):
                    logger.info(f"清理现有任务: {job.id}")
                    cls._scheduler.remove_job(job.id)

            # 重新加载所有启用的触发器
            for trigger in triggers:
                try:
                    cls.add_job(
                        trigger.id,
                        trigger.flow_id,
                        trigger.cron_expression
                    )
                except Exception as e:
                    logger.error(f"加载触发器失败 {trigger.id}: {e}")
        finally:
            session.close()
    
    @classmethod
    def add_immediate_job(cls, flow_id: str, input_data: Optional[Dict[str, Any]] = None,
                         delay_seconds: int = 1, job_id: Optional[str] = None) -> dict:
        """添加即时任务

        Args:
            flow_id: 工作流ID
            input_data: 输入数据
            delay_seconds: 延迟执行秒数，默认1秒
            job_id: 自定义任务ID，如果不提供则自动生成

        Returns:
            包含任务ID和历史记录ID的字典
        """
        if cls._scheduler is None:
            cls.init_scheduler()
            if not cls._scheduler_started:
                cls.start_scheduler()

        if job_id is None:
            job_id = f"immediate_{flow_id}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"

        # 先创建FlowHistory记录
        session = get_session()
        flow_history = None
        try:
            flow_history = FlowHistory(
                flow_id=flow_id,
                status='pending',
                input_data=json.dumps(input_data or {}),
                created_at=datetime.now()
            )
            session.add(flow_history)
            session.commit()
            flow_history_id = flow_history.id
            logger.info(f"创建FlowHistory记录: {flow_history_id}")
        except Exception as e:
            session.rollback()
            logger.error(f"创建FlowHistory失败: {e}")
            raise
        finally:
            session.close()

        run_date = datetime.now() + timedelta(seconds=delay_seconds)
        trigger = DateTrigger(run_date=run_date)

        cls._scheduler.add_job(
            func=cls.execute_immediate_flow,
            trigger=trigger,
            id=job_id,
            args=[flow_id, input_data, flow_history_id],
            replace_existing=False
        )

        logger.info(f"添加即时任务: {job_id}, 流程: {flow_id}, 历史记录ID: {flow_history_id}, 执行时间: {run_date}")

        return {
            'job_id': job_id,
            'flow_history_id': flow_history_id
        }

    @classmethod
    def execute_immediate_flow(cls, flow_id: str, input_data: Optional[Dict[str, Any]] = None, flow_history_id: Optional[int] = None):
        """执行即时工作流

        Args:
            flow_id: 工作流ID
            input_data: 输入数据
            flow_history_id: 流程历史记录ID
        """
        try:
            # 构建元数据，包含flow_history_id
            metadata = {
                'trigger_type': 'immediate',
                'trigger_time': datetime.now().isoformat(),
                'flow_history_id': flow_history_id
            }

            logger.info(f"执行即时任务: 流程 {flow_id}, 历史记录ID: {flow_history_id}")

            # 执行工作流
            logger.info(f"准备执行工作流: {flow_id}")
            result = execute_flow(
                flow_id=flow_id,
                input_data=input_data or {},
                metadata=metadata
            )
            logger.info(f"工作流执行完成: {flow_id}, 结果: {result}")

            # 更新历史记录状态为完成
            if flow_history_id and result.get('success'):
                cls.update_flow_history_status(flow_history_id, 'completed', None)
                # 更新输出数据
                session = get_session()
                try:
                    history = session.query(FlowHistory).filter(FlowHistory.id == flow_history_id).first()
                    if history:
                        history.output_data = json.dumps(result)
                        session.commit()
                except Exception as e:
                    session.rollback()
                    logger.error(f"更新输出数据失败: {e}")
                finally:
                    session.close()
        except Exception as e:
            logger.error(f"执行即时任务失败 {flow_id}: {e}")
            # 更新历史记录状态为失败
            if flow_history_id:
                cls.update_flow_history_status(flow_history_id, 'failed', str(e))

    @classmethod
    def update_flow_history_status(cls, flow_history_id: int, status: str, error_msg: Optional[str] = None):
        """更新FlowHistory状态

        Args:
            flow_history_id: 历史记录ID
            status: 新状态
            error_msg: 错误信息（可选）
        """
        session = get_session()
        try:
            flow_history = session.query(FlowHistory).filter(FlowHistory.id == flow_history_id).first()
            if flow_history:
                flow_history.status = status
                if status == 'running':
                    flow_history.start_time = datetime.now()
                elif status in ['completed', 'failed']:
                    flow_history.end_time = datetime.now()
                    if error_msg:
                        # 将错误信息存储在output_data中
                        flow_history.output_data = json.dumps({'error': error_msg})
                session.commit()
                logger.info(f"更新FlowHistory {flow_history_id} 状态为: {status}")
        except Exception as e:
            session.rollback()
            logger.error(f"更新FlowHistory状态失败: {e}")
        finally:
            session.close()

    @classmethod
    def get_job_status(cls, job_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态

        Args:
            job_id: 任务ID

        Returns:
            任务信息字典，如果任务不存在则返回None
        """
        if cls._scheduler is None:
            return None

        job = cls._scheduler.get_job(job_id)
        if job:
            return {
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger),
                'pending': job.pending
            }
        return None

    @classmethod
    def execute_trigger(cls, trigger_id: int):
        """执行触发器

        Args:
            trigger_id: 触发器ID
        """
        session = get_session()
        try:
            trigger = session.query(Trigger).filter(
                Trigger.id == trigger_id
            ).first()

            if not trigger or not trigger.enabled:
                logger.warning(f"触发器 {trigger_id} 不存在或未启用")
                return

            # 解析触发参数
            trigger_params = {}
            if trigger.trigger_params:
                try:
                    trigger_params = json.loads(trigger.trigger_params)
                except Exception as e:
                    logger.error(f"解析触发参数失败 {trigger_id}: {e}")

            # 构建元数据
            metadata = {
                'trigger_id': trigger_id,
                'trigger_name': trigger.name,
                'trigger_type': 'scheduled'
            }

            logger.info(f"执行触发器 {trigger_id}: {trigger.name}")

            # 执行工作流
            execute_flow(
                flow_id=trigger.flow_id,
                input_data=trigger_params,
                metadata=metadata
            )
        except Exception as e:
            logger.error(f"执行触发器失败 {trigger_id}: {e}")
        finally:
            session.close()
