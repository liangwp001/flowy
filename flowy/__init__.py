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
]


def run(host: str = "127.0.0.1", port: int = 5000, debug: bool = False):
    """启动Flowy Web管理界面

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
    Flowy Web管理系统
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
    Flowy Web管理系统
    ========================================
    调试模式: {'开启' if debug else '关闭'}
    访问地址: http://{host}:{port}
    ========================================
    """)

    app.run(host=host, port=port, debug=debug)
