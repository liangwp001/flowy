#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""迁移: 添加分布式执行字段

版本: 20260106000001

此迁移为分布式执行功能添加必要的数据库字段：
- 创建 Worker 表（主数据库）
- 为 Trigger 表添加 target_tags, target_worker, priority 字段（主数据库）
- 为 FlowHistory 表添加 target_tags, target_worker, priority, claimed_by, claimed_at 字段（历史数据库）
"""

import sqlite3
from pathlib import Path

from flowy.core.config import get_config
from flowy.core.migration_manager import Migration


class Migration002AddDistributedFields(Migration):
    """添加分布式执行字段"""

    version = '20260106000001'
    name = 'add_distributed_fields'
    description = '添加分布式执行所需的 Worker 表和相关字段'

    def upgrade(self, session):
        """执行迁移"""
        config = get_config()
        
        # 迁移主数据库（Worker 表和 Trigger 字段）
        self._migrate_main_db(config)
        
        # 迁移历史数据库（FlowHistory 字段）
        self._migrate_history_db(config)

    def _migrate_main_db(self, config):
        """迁移主数据库"""
        db_path = Path(config.database_file)
        
        if not db_path.exists():
            print(f'  - 主数据库不存在，跳过迁移: {db_path}')
            return

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        try:
            # 创建 Worker 表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS worker (
                    id VARCHAR(64) PRIMARY KEY,
                    hostname VARCHAR(128) NOT NULL,
                    tags TEXT,
                    status VARCHAR(16) DEFAULT 'online',
                    last_heartbeat DATETIME,
                    registered_at DATETIME,
                    worker_metadata TEXT
                )
            ''')
            print('  - 创建 worker 表')

            # 获取 trigger 表现有列
            cursor.execute("PRAGMA table_info(trigger)")
            existing_columns = {row[1] for row in cursor.fetchall()}

            # 添加 target_tags 字段
            if 'target_tags' not in existing_columns:
                cursor.execute('ALTER TABLE trigger ADD COLUMN target_tags TEXT')
                print('  - 添加 trigger.target_tags 字段')

            # 添加 target_worker 字段
            if 'target_worker' not in existing_columns:
                cursor.execute('ALTER TABLE trigger ADD COLUMN target_worker VARCHAR(64)')
                print('  - 添加 trigger.target_worker 字段')

            # 添加 priority 字段
            if 'priority' not in existing_columns:
                cursor.execute('ALTER TABLE trigger ADD COLUMN priority INTEGER DEFAULT 50')
                print('  - 添加 trigger.priority 字段')

            conn.commit()
        finally:
            conn.close()

    def _migrate_history_db(self, config):
        """迁移历史数据库"""
        db_path = Path(config.history_database_file)

        if not db_path.exists():
            print(f'  - 历史数据库不存在，跳过迁移: {db_path}')
            return

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        try:
            # 获取 flow_history 表现有列
            cursor.execute("PRAGMA table_info(flow_history)")
            existing_columns = {row[1] for row in cursor.fetchall()}

            # 添加 target_tags 字段
            if 'target_tags' not in existing_columns:
                cursor.execute('ALTER TABLE flow_history ADD COLUMN target_tags TEXT')
                print('  - 添加 flow_history.target_tags 字段')

            # 添加 target_worker 字段
            if 'target_worker' not in existing_columns:
                cursor.execute('ALTER TABLE flow_history ADD COLUMN target_worker VARCHAR(64)')
                print('  - 添加 flow_history.target_worker 字段')

            # 添加 priority 字段
            if 'priority' not in existing_columns:
                cursor.execute('ALTER TABLE flow_history ADD COLUMN priority INTEGER DEFAULT 50')
                print('  - 添加 flow_history.priority 字段')

            # 添加 claimed_by 字段
            if 'claimed_by' not in existing_columns:
                cursor.execute('ALTER TABLE flow_history ADD COLUMN claimed_by VARCHAR(64)')
                print('  - 添加 flow_history.claimed_by 字段')

            # 添加 claimed_at 字段
            if 'claimed_at' not in existing_columns:
                cursor.execute('ALTER TABLE flow_history ADD COLUMN claimed_at DATETIME')
                print('  - 添加 flow_history.claimed_at 字段')

            conn.commit()
        finally:
            conn.close()

    def downgrade(self, session):
        """回滚迁移"""
        # SQLite 不支持直接删除列，需要重建表
        print('警告: SQLite 不支持直接删除列，回滚操作需要手动执行')
        print('需要手动删除以下内容:')
        print('  - worker 表')
        print('  - trigger 表的 target_tags, target_worker, priority 字段')
        print('  - flow_history 表的 target_tags, target_worker, priority, claimed_by, claimed_at 字段')


# 导出迁移类，供迁移管理器使用
Migration = Migration002AddDistributedFields
