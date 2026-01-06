#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""调度服务模块"""

import logging
import os
import glob
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from flowy.core.db import get_session, Trigger, FlowHistory, TaskHistory
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
            from flowy.core.config import get_config

            config = get_config()
            executors = {
                'default': ThreadPoolExecutor(max_workers=config.scheduler_max_workers)
            }

            cls._scheduler = BackgroundScheduler(
                executors=executors,
                timezone=config.scheduler_timezone,
                job_defaults={
                    'coalesce': True,  # 合并错过的执行
                    'max_instances': 1  # 同一任务最多1个实例
                }
            )
            logger.info(
                f"调度器已初始化: max_workers={config.scheduler_max_workers}, "
                f"timezone={config.scheduler_timezone}"
            )
    
    @classmethod
    def start_scheduler(cls):
        """启动调度器"""
        if not cls._scheduler_started:
            cls.init_scheduler()
            cls.load_triggers_from_db()
            cls._scheduler.start()
            cls._scheduler_started = True
            logger.info("调度器已启动")

            # 添加孤儿任务检查定时任务（每30秒执行一次）
            cls.add_orphaned_job_checker()

            # 添加触发器同步定时任务（每60秒执行一次）
            cls.add_trigger_syncer()

            # 添加历史数据清理定时任务（每天0点执行，需配置启用）
            cls.add_history_cleanup_job()

    @classmethod
    def add_orphaned_job_checker(cls, interval_seconds: int = 30):
        """添加孤儿任务检查定时任务

        Args:
            interval_seconds: 检查间隔时间（秒），默认30秒
        """
        if cls._scheduler is None:
            return

        job_id = 'orphaned_job_checker'

        # 如果任务已存在，先移除
        if cls._scheduler.get_job(job_id):
            cls._scheduler.remove_job(job_id)

        cls._scheduler.add_job(
            func=cls.check_orphaned_jobs,
            trigger='interval',
            seconds=interval_seconds,
            id=job_id,
            name='孤儿任务检查器',
            replace_existing=True
        )
        logger.info(f"添加孤儿任务检查定时任务，间隔: {interval_seconds}秒")

    @classmethod
    def add_trigger_syncer(cls, interval_seconds: int = 60):
        """添加触发器同步定时任务

        定期对比数据库触发器配置与 scheduler 实际任务状态，自动同步不一致的部分。

        Args:
            interval_seconds: 同步间隔时间（秒），默认60秒
        """
        if cls._scheduler is None:
            return

        job_id = 'trigger_syncer'

        # 如果任务已存在，先移除
        if cls._scheduler.get_job(job_id):
            cls._scheduler.remove_job(job_id)

        cls._scheduler.add_job(
            func=cls.sync_triggers_from_db,
            trigger='interval',
            seconds=interval_seconds,
            id=job_id,
            name='触发器同步器',
            replace_existing=True
        )
        logger.info(f"添加触发器同步定时任务，间隔: {interval_seconds}秒")

    @classmethod
    def cleanup_history_data(cls, retention_days: Optional[int] = None):
        """清理过期的历史数据

        清理超过保留天数的 FlowHistory、TaskHistory 记录和日志文件。

        Args:
            retention_days: 保留天数，如果不指定则从配置读取

        Returns:
            清理结果统计字典
        """
        from flowy.core.config import get_config

        # 获取保留天数
        if retention_days is None:
            retention_days = get_config().history_retention_days

        session = get_session()
        now = datetime.now()
        cutoff_date = now - timedelta(days=retention_days)

        try:
            result = {
                'flow_history_deleted': 0,
                'task_history_deleted': 0,
                'log_files_deleted': 0,
                'retention_days': retention_days,
                'cutoff_date': cutoff_date.isoformat()
            }

            # 1. 删除过期的 FlowHistory 记录
            expired_flows = session.query(FlowHistory).filter(
                FlowHistory.created_at < cutoff_date
            ).all()

            # 收集需要删除的 flow_history_id
            flow_history_ids = [f.id for f in expired_flows]

            if flow_history_ids:
                # 先删除关联的 TaskHistory
                deleted_tasks = session.query(TaskHistory).filter(
                    TaskHistory.flow_history_id.in_(flow_history_ids)
                ).delete(synchronize_session=False)
                result['task_history_deleted'] = deleted_tasks

                # 再删除 FlowHistory
                for flow in expired_flows:
                    session.delete(flow)
                result['flow_history_deleted'] = len(expired_flows)

                session.commit()
                logger.info(f"清理历史数据: 删除 {len(expired_flows)} 条 FlowHistory, {deleted_tasks} 条 TaskHistory")

            # 2. 清理过期的日志文件
            log_dir = get_config().log_dir
            if os.path.exists(log_dir):
                # 查找所有 .log 文件
                log_files = glob.glob(os.path.join(log_dir, '*.log'))

                for log_file in log_files:
                    try:
                        # 获取文件修改时间
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(log_file))

                        # 如果文件超过保留天数，删除
                        if file_mtime < cutoff_date:
                            os.remove(log_file)
                            result['log_files_deleted'] += 1
                            logger.info(f"删除过期日志文件: {log_file}")
                    except Exception as e:
                        logger.error(f"删除日志文件失败 {log_file}: {e}")

            # 记录清理结果
            total_deleted = result['flow_history_deleted'] + result['task_history_deleted'] + result['log_files_deleted']
            if total_deleted > 0:
                logger.info(
                    f"历史数据清理完成: FlowHistory={result['flow_history_deleted']}, "
                    f"TaskHistory={result['task_history_deleted']}, "
                    f"日志文件={result['log_files_deleted']}, "
                    f"保留天数={retention_days}天"
                )
            else:
                logger.debug(f"历史数据清理完成，无需删除数据 (保留天数={retention_days}天)")

            return result

        except Exception as e:
            session.rollback()
            logger.error(f"清理历史数据时发生错误: {e}")
            return None
        finally:
            session.close()

    @classmethod
    def add_history_cleanup_job(cls, retention_days: Optional[int] = None):
        """添加历史数据清理定时任务

        每天0点执行一次，清理超过保留天数的历史数据。

        Args:
            retention_days: 保留天数，如果不指定则从配置读取
        """
        from flowy.core.config import get_config

        if cls._scheduler is None:
            return

        # 检查是否启用清理功能
        if not get_config().enable_history_cleanup:
            logger.info("历史数据清理功能未启用，跳过添加定时任务")
            return

        job_id = 'history_cleanup'

        # 如果任务已存在，先移除
        if cls._scheduler.get_job(job_id):
            cls._scheduler.remove_job(job_id)

        # 使用 cron 表达式：每天0点执行
        trigger = CronTrigger.from_crontab('0 0 * * *')

        cls._scheduler.add_job(
            func=cls.cleanup_history_data,
            trigger=trigger,
            id=job_id,
            args=[retention_days] if retention_days else [],
            name='历史数据清理器',
            replace_existing=True
        )

        days = retention_days or get_config().history_retention_days
        logger.info(f"添加历史数据清理定时任务，每天0点执行，保留{days}天数据")
    
    @classmethod
    def shutdown_scheduler(cls):
        """关闭调度器"""
        if cls._scheduler and cls._scheduler_started:
            cls._scheduler.shutdown(wait=False)
            cls._scheduler_started = False
            logger.info("调度器已关闭")
    
    @classmethod
    def add_job(cls, trigger_id: int, flow_id: str, cron_expression: str, max_instances: int = 1):
        """添加调度任务

        Args:
            trigger_id: 触发器ID
            flow_id: 工作流ID
            cron_expression: Cron表达式
            max_instances: 最大并发实例数，默认1
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
            max_instances=max_instances  # 根据触发器配置设置最大实例数
        )
        logger.info(f"添加调度任务: {job_id}, cron: {cron_expression}, max_instances: {max_instances}")
    
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
                    # 获取 max_instances，默认为 1（兼容旧数据）
                    max_instances = getattr(trigger, 'max_instances', 1) or 1
                    cls.add_job(
                        trigger.id,
                        trigger.flow_id,
                        trigger.cron_expression,
                        max_instances=max_instances
                    )
                except Exception as e:
                    logger.error(f"加载触发器失败 {trigger.id}: {e}")
        finally:
            session.close()

    @classmethod
    def sync_triggers_from_db(cls):
        """智能同步数据库触发器到 scheduler

        对比数据库中的触发器配置与 scheduler 中实际的 job，只更新不一致的部分。
        这解决了前端修改触发器后 scheduler 未及时更新的问题。
        优化：减少临时对象创建，使用更高效的查询方式。
        """
        if cls._scheduler is None:
            logger.warning("调度器未初始化，跳过同步")
            return

        session = get_session()
        try:
            sync_result = {
                'added': 0,
                'updated': 0,
                'removed': 0,
                'paused': 0,
                'resumed': 0,
                'unchanged': 0
            }

            # 获取 scheduler 中所有的 trigger 任务 ID（使用字典存储 job 信息，避免重复查询）
            scheduler_jobs_info = {}
            for job in cls._scheduler.get_jobs():
                if job.id.startswith('trigger_'):
                    try:
                        trigger_id = int(job.id.split('_')[1])
                        scheduler_jobs_info[trigger_id] = {
                            'job': job,
                            'paused': job.next_run_time is None,
                            'max_instances': getattr(job, 'max_instances', 1)
                        }
                    except (ValueError, IndexError):
                        continue

            db_trigger_ids = set()

            # 使用迭代器逐条处理触发器，避免一次性加载所有数据
            for trigger in session.query(Trigger).yield_per(50):
                db_trigger_ids.add(trigger.id)
                job_info = scheduler_jobs_info.get(trigger.id)
                max_instances = getattr(trigger, 'max_instances', 1) or 1

                if trigger.enabled:
                    if job_info is None:
                        # 任务不存在，需要添加
                        try:
                            cls.add_job(trigger.id, trigger.flow_id, trigger.cron_expression, max_instances=max_instances)
                            sync_result['added'] += 1
                            logger.info(f"同步添加触发器: {trigger.id} ({trigger.name})")
                        except Exception as e:
                            logger.error(f"同步添加触发器失败 {trigger.id}: {e}")
                    else:
                        job = job_info['job']
                        need_update = False

                        # 检查 cron 表达式是否变化
                        if hasattr(job.trigger, 'fields'):
                            field_values = [str(field) for field in job.trigger.fields[:5]]
                            reconstructed_cron = ' '.join(field_values)
                            if reconstructed_cron != trigger.cron_expression:
                                need_update = True

                        # 检查 max_instances 是否变化
                        if not need_update and job_info['max_instances'] != max_instances:
                            need_update = True

                        # 检查任务是否被暂停
                        if not need_update and job_info['paused']:
                            cls.resume_job(trigger.id)
                            sync_result['resumed'] += 1
                            logger.info(f"同步恢复触发器: {trigger.id} ({trigger.name})")

                        if need_update:
                            try:
                                cls.add_job(trigger.id, trigger.flow_id, trigger.cron_expression, max_instances=max_instances)
                                sync_result['updated'] += 1
                                logger.info(f"同步更新触发器: {trigger.id} ({trigger.name})")
                            except Exception as e:
                                logger.error(f"同步更新触发器失败 {trigger.id}: {e}")
                        else:
                            sync_result['unchanged'] += 1
                else:
                    # 触发器已禁用
                    if job_info is not None:
                        try:
                            cls.pause_job(trigger.id)
                            sync_result['paused'] += 1
                            logger.info(f"同步暂停触发器: {trigger.id} ({trigger.name})")
                        except Exception as e:
                            logger.error(f"同步暂停触发器失败 {trigger.id}: {e}")
                    else:
                        sync_result['unchanged'] += 1

            # 移除 scheduler 中存在但数据库中不存在的任务
            orphaned_job_ids = set(scheduler_jobs_info.keys()) - db_trigger_ids
            for trigger_id in orphaned_job_ids:
                try:
                    cls.remove_job(trigger_id)
                    sync_result['removed'] += 1
                    logger.info(f"同步移除孤儿触发器任务: trigger_{trigger_id}")
                except Exception as e:
                    logger.error(f"同步移除孤儿触发器失败 {trigger_id}: {e}")

            # 记录同步结果
            total_actions = sum(v for k, v in sync_result.items() if k != 'unchanged')
            if total_actions > 0:
                logger.info(
                    f"触发器同步完成: 添加={sync_result['added']}, "
                    f"更新={sync_result['updated']}, 暂停={sync_result['paused']}, "
                    f"恢复={sync_result['resumed']}, 移除={sync_result['removed']}, "
                    f"未变={sync_result['unchanged']}"
                )
            else:
                logger.debug("触发器同步完成，无需更改")

            return sync_result

        except Exception as e:
            logger.error(f"同步触发器时发生错误: {e}")
            return None
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

    @classmethod
    def check_orphaned_jobs(cls, pending_timeout_minutes: int = 5, running_timeout_hours: int = 24,
                            batch_size: int = 100):
        """检查孤儿任务（状态为 pending 或 running 但实际已丢失的任务）

        当 scheduler 重启时，等待中的即时任务会丢失。此方法定期检查并将这些任务标记为失败。
        使用分页查询避免一次性加载大量数据到内存。

        Args:
            pending_timeout_minutes: pending 状态超时时间（分钟），超过此时间仍在 pending 则认为丢失
            running_timeout_hours: running 状态超时时间（小时），超过此时间仍在 running 则认为可能卡死
            batch_size: 每批处理的记录数量，默认100
        """
        now = datetime.now()
        pending_timeout = timedelta(minutes=pending_timeout_minutes)
        running_timeout = timedelta(hours=running_timeout_hours)

        result = {
            'pending_count': 0,
            'running_count': 0,
            'task_count': 0
        }

        # 获取 scheduler 中所有即时任务的 job_id 前缀（一次性获取，避免重复查询）
        immediate_job_prefixes = set()
        if cls._scheduler:
            for job in cls._scheduler.get_jobs():
                if job.id.startswith('immediate_') and job.next_run_time and job.next_run_time > now:
                    # 提取 flow_id 部分: immediate_{flow_id}_{timestamp}
                    parts = job.id.split('_')
                    if len(parts) >= 2:
                        immediate_job_prefixes.add(f"immediate_{parts[1]}_")

        # 1. 分页处理 pending 状态超时的 FlowHistory
        session = get_session()
        try:
            offset = 0
            while True:
                pending_batch = session.query(FlowHistory).filter(
                    FlowHistory.status == 'pending',
                    FlowHistory.created_at < (now - pending_timeout)
                ).limit(batch_size).offset(offset).all()

                if not pending_batch:
                    break

                for history in pending_batch:
                    # 检查是否有对应的即时任务
                    task_lost = True
                    prefix = f"immediate_{history.flow_id}_"
                    if prefix in immediate_job_prefixes:
                        task_lost = False

                    if task_lost:
                        logger.warning(f"FlowHistory {history.id} (flow={history.flow_id}) 任务丢失，标记为失败")
                        history.status = 'failed'
                        history.end_time = now
                        history.output_data = json.dumps({
                            'error': '任务丢失：调度器重启或任务超时',
                            'original_status': 'pending',
                            'detected_at': now.isoformat()
                        })

                        # 更新关联的 running 状态的 TaskHistory
                        session.query(TaskHistory).filter(
                            TaskHistory.flow_history_id == history.id,
                            TaskHistory.status == 'running'
                        ).update({'status': 'failed', 'end_time': now}, synchronize_session=False)

                        result['pending_count'] += 1

                session.commit()
                offset += batch_size

        except Exception as e:
            session.rollback()
            logger.error(f"处理 pending 孤儿任务时发生错误: {e}")
        finally:
            session.close()

        # 2. 分页处理 running 状态超时的 FlowHistory
        session = get_session()
        try:
            offset = 0
            while True:
                running_batch = session.query(FlowHistory).filter(
                    FlowHistory.status == 'running',
                    FlowHistory.start_time < (now - running_timeout)
                ).limit(batch_size).offset(offset).all()

                if not running_batch:
                    break

                for history in running_batch:
                    # 检查是否有 TaskHistory 仍在运行
                    running_tasks_count = session.query(TaskHistory).filter(
                        TaskHistory.flow_history_id == history.id,
                        TaskHistory.status == 'running'
                    ).count()

                    if running_tasks_count == 0:
                        logger.warning(f"FlowHistory {history.id} 无运行中任务但状态为 running，标记为失败")
                        history.status = 'failed'
                        history.end_time = now
                        history.output_data = json.dumps({
                            'error': f'任务运行超时（超过 {running_timeout_hours} 小时）且无活动任务',
                            'original_status': 'running',
                            'detected_at': now.isoformat()
                        })
                        result['running_count'] += 1

                session.commit()
                offset += batch_size

        except Exception as e:
            session.rollback()
            logger.error(f"处理 running 孤儿任务时发生错误: {e}")
        finally:
            session.close()

        # 3. 分页处理 TaskHistory 中的孤儿任务
        session = get_session()
        try:
            offset = 0
            while True:
                orphaned_batch = session.query(TaskHistory).filter(
                    TaskHistory.status == 'running',
                    TaskHistory.start_time < (now - running_timeout)
                ).limit(batch_size).offset(offset).all()

                if not orphaned_batch:
                    break

                for task in orphaned_batch:
                    flow_history = session.query(FlowHistory).filter(
                        FlowHistory.id == task.flow_history_id
                    ).first()

                    if flow_history and flow_history.status in ['completed', 'failed']:
                        logger.warning(f"TaskHistory {task.id} 的 Flow 已结束，标记任务状态")
                        task.status = flow_history.status
                        task.end_time = flow_history.end_time
                        result['task_count'] += 1

                session.commit()
                offset += batch_size

        except Exception as e:
            session.rollback()
            logger.error(f"处理 TaskHistory 孤儿任务时发生错误: {e}")
        finally:
            session.close()

        if result['pending_count'] > 0 or result['running_count'] > 0 or result['task_count'] > 0:
            logger.info(
                f"孤儿任务检查完成: pending={result['pending_count']}, "
                f"running={result['running_count']}, task={result['task_count']}"
            )

        return result
