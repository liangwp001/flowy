#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy - 工作流管理框架

使用示例:
    from flowy import flow, task, run

    @task(name="my_task")
    def my_task(x):
        return x * 2

    @flow(flow_id="my_flow", name="My Flow")
    def my_flow(x):
        return my_task(x)

    if __name__ == '__main__':
        my_flow(10)  # 执行工作流
        run(host='127.0.0.1', port=5000)  # 启动Web管理界面

分布式模式使用示例:
    # Master 模式
    from flowy import run_master, configure
    configure(
        mode='master',
        external_database_url='postgresql://user:pass@host/db'
    )
    run_master(host='0.0.0.0', port=5000)

    # Worker 模式
    from flowy import run_worker, configure
    configure(
        mode='worker',
        external_database_url='postgresql://user:pass@host/db',
        worker_tags=['gpu', 'high-memory']
    )
    run_worker()
"""

from flowy.core.flow import flow
from flowy.core.task import task
from flowy.core.logger import get_flow_logger, get_logger
from flowy.core.context import get_flow_history_id
from flowy.core.config import configure, get_config, set_config, configure_from_env, FlowyConfig
from flowy.core.progress import set_progress
from flowy.core.db import run_pending_migrations, get_migration_history, get_current_db_version
from flowy.core import remark

__version__ = "0.1.0"
__all__ = [
    "flow",
    "task",
    "run",
    "run_standalone",
    "run_master",
    "run_worker",
    "get_flow_logger",
    "get_logger",
    "get_flow_history_id",
    "configure",
    "configure_from_env",
    "get_config",
    "set_config",
    "FlowyConfig",
    "set_progress",
    "run_pending_migrations",
    "get_migration_history",
    "get_current_db_version",
    "remark",
]


def run(host: str = "127.0.0.1", port: int = 5000, debug: bool = False, 
        config: FlowyConfig = None):
    """启动 Flowy（根据配置自动选择模式）

    根据配置中的 mode 字段自动选择运行模式：
    - standalone: 单机模式，Web + 调度 + 本地执行
    - master: 主节点模式，Web + 调度，可选本地执行
    - worker: 工作节点模式，仅执行任务

    Args:
        host: 服务器地址，默认 127.0.0.1（仅 standalone/master 模式有效）
        port: 端口号，默认 5000（仅 standalone/master 模式有效）
        debug: 是否开启调试模式，默认 False（仅 standalone/master 模式有效）
        config: 配置对象，如果不提供则使用全局配置
    """
    if config:
        set_config(config)
    
    current_config = get_config()
    
    if current_config.mode == 'standalone':
        run_standalone(host=host, port=port, debug=debug)
    elif current_config.mode == 'master':
        run_master(host=host, port=port, debug=debug)
    elif current_config.mode == 'worker':
        run_worker()
    else:
        raise ValueError(f"Invalid mode: {current_config.mode}")


def run_standalone(host: str = "127.0.0.1", port: int = 5000, debug: bool = False):
    """启动单机模式
    
    单机模式提供完整功能：Web UI、调度器、本地执行。
    这是默认模式，适用于单节点部署。

    Args:
        host: 服务器地址，默认 127.0.0.1
        port: 端口号，默认 5000
        debug: 是否开启调试模式，默认 False
    """
    import atexit
    import logging
    import os
    from flowy.web import create_app
    from flowy.web.services.scheduler_service import SchedulerService
    from flowy.core.logger import get_logger

    # 在debug模式下，避免重载器重复启动调度器
    if debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        print("""
    ========================================
    Flowy Web管理系统 (Standalone 模式)
    ========================================
    调试模式: 开启
    注意：调试模式下，触发器可能存在重复执行问题
    建议生产环境关闭调试模式
    访问地址: http://{host}:{port}
    ========================================
    """)

    app = create_app()
    app.config['JSON_AS_ASCII'] = False
    app.config['JSON_SORT_KEYS'] = False

    # 配置APScheduler日志
    scheduler_logger = logging.getLogger('apscheduler')
    scheduler_logger.setLevel(logging.INFO)

    # 将APScheduler日志输出到应用日志文件
    app_logger = get_logger('flow', console_output=True)
    for handler in app_logger.handlers:
        scheduler_logger.addHandler(handler)

    # 只在主进程中启动调度器（避免debug模式重载器重复启动）
    if not debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        # 启动调度器
        SchedulerService.start_scheduler()

        # 注册优雅关闭处理器
        atexit.register(SchedulerService.shutdown_scheduler)

    print(f"""
    ========================================
    Flowy Web管理系统 (Standalone 模式)
    ========================================
    调试模式: {'开启' if debug else '关闭'}
    访问地址: http://{host}:{port}
    ========================================
    """)

    app.run(host=host, port=port, debug=debug)



def run_master(host: str = "0.0.0.0", port: int = 5000, debug: bool = False,
               enable_local_execution: bool = True):
    """启动 Master 模式
    
    Master 模式提供 Web UI 和调度器，可选本地执行。
    适用于分布式部署的主节点。

    Args:
        host: 服务器地址，默认 0.0.0.0
        port: 端口号，默认 5000
        debug: 是否开启调试模式，默认 False
        enable_local_execution: 是否启用本地执行，默认 True
    """
    import atexit
    import logging
    import os
    from flowy.web import create_app
    from flowy.web.services.scheduler_service import SchedulerService
    from flowy.core.logger import get_logger
    
    config = get_config()
    
    # 验证配置
    config.validate()
    
    # 在debug模式下，避免重载器重复启动调度器
    if debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        print(f"""
    ========================================
    Flowy Web管理系统 (Master 模式)
    ========================================
    调试模式: 开启
    数据库: {config.external_database_url[:50]}...
    本地执行: {'启用' if enable_local_execution else '禁用'}
    注意：调试模式下，触发器可能存在重复执行问题
    建议生产环境关闭调试模式
    访问地址: http://{host}:{port}
    ========================================
    """)

    app = create_app()
    app.config['JSON_AS_ASCII'] = False
    app.config['JSON_SORT_KEYS'] = False

    # 配置APScheduler日志
    scheduler_logger = logging.getLogger('apscheduler')
    scheduler_logger.setLevel(logging.INFO)

    # 将APScheduler日志输出到应用日志文件
    app_logger = get_logger('flow', console_output=True)
    for handler in app_logger.handlers:
        scheduler_logger.addHandler(handler)

    # 只在主进程中启动调度器（避免debug模式重载器重复启动）
    if not debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
        # 启动调度器（包含健康检查定时任务）
        SchedulerService.start_scheduler()

        # 注册优雅关闭处理器
        atexit.register(SchedulerService.shutdown_scheduler)

    # 隐藏数据库密码用于显示
    db_url_display = config.external_database_url
    if db_url_display and '@' in db_url_display:
        # 隐藏密码部分
        parts = db_url_display.split('@')
        prefix = parts[0]
        if ':' in prefix:
            # 格式: protocol://user:password@host
            proto_user = prefix.rsplit(':', 1)[0]
            db_url_display = f"{proto_user}:****@{parts[1]}"
    
    print(f"""
    ========================================
    Flowy Web管理系统 (Master 模式)
    ========================================
    调试模式: {'开启' if debug else '关闭'}
    数据库: {db_url_display}
    本地执行: {'启用' if enable_local_execution else '禁用'}
    健康检查: 已启动
    访问地址: http://{host}:{port}
    ========================================
    """)

    app.run(host=host, port=port, debug=debug)


def run_worker(tags: list = None, worker_id: str = None, poll_interval: int = 2):
    """启动 Worker 模式
    
    Worker 模式仅轮询和执行匹配标签的任务。
    适用于分布式部署的工作节点。

    Args:
        tags: Worker 能力标签列表，如果不提供则从配置或环境变量读取
        worker_id: Worker ID，如果不提供则自动生成
        poll_interval: 轮询间隔（秒），默认 2 秒
    """
    import os
    from flowy.worker.service import WorkerService
    from flowy.core.logger import get_logger
    
    logger = get_logger('worker', console_output=True)
    config = get_config()
    
    # 验证配置
    config.validate()
    
    # 确定 Worker 标签
    if tags is None:
        # 优先从配置读取
        if config.worker_tags:
            tags = config.worker_tags
        else:
            # 从环境变量读取
            env_tags = os.environ.get('FLOWY_WORKER_TAGS', '')
            tags = [t.strip() for t in env_tags.split(',') if t.strip()]
    
    # 隐藏数据库密码用于显示
    db_url_display = config.external_database_url
    if db_url_display and '@' in db_url_display:
        parts = db_url_display.split('@')
        prefix = parts[0]
        if ':' in prefix:
            proto_user = prefix.rsplit(':', 1)[0]
            db_url_display = f"{proto_user}:****@{parts[1]}"
    
    print(f"""
    ========================================
    Flowy Worker (Worker 模式)
    ========================================
    数据库: {db_url_display}
    标签: {tags if tags else '(无标签)'}
    轮询间隔: {poll_interval} 秒
    心跳间隔: {config.worker_heartbeat_interval} 秒
    ========================================
    """)
    
    # 创建并启动 Worker 服务
    worker = WorkerService(
        tags=tags,
        worker_id=worker_id,
        poll_interval=poll_interval
    )
    
    logger.info(f"启动 Worker: tags={tags}, poll_interval={poll_interval}s")
    
    # 启动 Worker（阻塞调用，直到收到关闭信号）
    worker.start()
