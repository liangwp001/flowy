#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""迁移: 添加任务进度跟踪字段

版本: 20250105000001
"""

import sqlite3
from pathlib import Path

from flowy.core.config import get_config
from flowy.core.migration_manager import Migration


class Migration001AddTaskProgress(Migration):
    """添加任务进度跟踪字段"""

    version = '20250105000001'
    name = 'add_task_progress'
    description = '添加 task_history 表的进度跟踪字段 (progress, progress_message, progress_updated_at)'

    def upgrade(self, session):
        """执行迁移"""
        config = get_config()
        db_path = Path(config.history_database_file)

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        try:
            # 获取现有列
            cursor.execute("PRAGMA table_info(task_history)")
            existing_columns = {row[1] for row in cursor.fetchall()}

            # 添加 progress 字段
            if 'progress' not in existing_columns:
                cursor.execute('ALTER TABLE task_history ADD COLUMN progress INTEGER')
                print('  - 添加 progress 字段')

            # 添加 progress_message 字段
            if 'progress_message' not in existing_columns:
                cursor.execute('ALTER TABLE task_history ADD COLUMN progress_message VARCHAR(256)')
                print('  - 添加 progress_message 字段')

            # 添加 progress_updated_at 字段
            if 'progress_updated_at' not in existing_columns:
                cursor.execute('ALTER TABLE task_history ADD COLUMN progress_updated_at DATETIME')
                print('  - 添加 progress_updated_at 字段')

            conn.commit()
        finally:
            conn.close()

    def downgrade(self, session):
        """回滚迁移"""
        # SQLite 不支持直接删除列，需要重建表
        print('警告: SQLite 不支持直接删除列，回滚操作需要手动执行')


# 导出迁移类，供迁移管理器使用
Migration = Migration001AddTaskProgress
