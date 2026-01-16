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
"""

from flowy.core.flow import flow
from flowy.core.task import task
from flowy.core.logger import get_flow_logger, get_logger
from flowy.core.context import get_flow_history_id
from flowy.core.config import configure, get_config
from flowy.core.progress import set_progress
from flowy.core.db import run_pending_migrations, get_migration_history, get_current_db_version
from flowy.core import remark

__version__ = "0.1.0"
__all__ = [
    "flow",
    "task",
    "run",
    "get_flow_logger",
    "get_logger",
    "get_flow_history_id",
    "configure",
    "get_config",
    "set_progress",
    "run_pending_migrations",
    "get_migration_history",
    "get_current_db_version",
    "remark",
]


def run(
    host: str = "127.0.0.1",
    port: int = 5000,
    debug: bool = False,
    site_name: str = None,
    auth_username: str = None,
    auth_password: str = None
):
    """启动Flowy Web管理界面

    Args:
        host: 服务器地址，默认 127.0.0.1
        port: 端口号，默认 5000
        debug: 是否开启调试模式，默认 False
        site_name: 自定义站点名称，默认 'Flowy'
        auth_username: 认证用户名，设置后启用 Basic Auth
        auth_password: 认证密码，需与 auth_username 一起设置
    """
    import atexit
    import logging
    import os
    from flowy.web import create_app
    from flowy.web.services.scheduler_service import SchedulerService
    from flowy.core.logger import get_logger
    from flowy.core.config import configure, get_config

    # 配置站点名称和认证
    config = get_config()
    config_updates = {}

    if site_name is not None:
        config_updates['site_name'] = site_name

    # 如果同时提供了用户名和密码，则启用认证
    if auth_username is not None and auth_password is not None:
        config_updates['auth_enabled'] = True
        config_updates['auth_username'] = auth_username
        config_updates['auth_password'] = auth_password

    if config_updates:
        configure(
            data_dir=config.data_dir,
            enable_history_cleanup=config.enable_history_cleanup,
            history_retention_days=config.history_retention_days,
            scheduler_max_workers=config.scheduler_max_workers,
            scheduler_timezone=config.scheduler_timezone,
            **config_updates
        )

    config = get_config()
    display_site_name = config.site_name

    # 在debug模式下，避免重载器重复启动调度器
    if debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        print(f"""
    ========================================
    {display_site_name} Web管理系统
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

    auth_info = "已启用 (Basic Auth)" if config.auth_enabled else "未启用"
    print(f"""
    ========================================
    {display_site_name} Web管理系统
    ========================================
    调试模式: {'开启' if debug else '关闭'}
    认证状态: {auth_info}
    访问地址: http://{host}:{port}
    ========================================
    """)

    app.run(host=host, port=port, debug=debug)
