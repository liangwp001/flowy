#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy数据库迁移管理模块"""

import hashlib
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import inspect, text
from sqlalchemy.schema import CreateTable, CreateIndex

from flowy.core.config import get_config
from flowy.core.db import _get_history_engine, _history_engine


class Migration:
    """迁移基类"""

    version: str = None  # 迁移版本号，格式: YYYYMMDDHHMMSS
    name: str = None     # 迁移名称
    description: str = None  # 迁移描述

    def upgrade(self, session):
        """执行迁移（升级）"""
        raise NotImplementedError

    def downgrade(self, session):
        """回滚迁移（降级）"""
        raise NotImplementedError


class MigrationManager:
    """迁移管理器"""

    def __init__(self):
        self.config = get_config()
        self.db_path = Path(self.config.history_database_file)
        self.migrations_table = '_schema_migrations'

    def get_current_version(self) -> Optional[str]:
        """获取当前数据库版本"""
        if not self.db_path.exists():
            return None

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        try:
            cursor.execute(
                f"SELECT version FROM {self.migrations_table} ORDER BY version DESC LIMIT 1"
            )
            result = cursor.fetchone()
            return result[0] if result else None
        except sqlite3.OperationalError:
            # 迁移表不存在，说明是全新数据库
            return None
        finally:
            conn.close()

    def get_migration_files(self) -> dict[str, type[Migration]]:
        """获取所有迁移文件"""
        migrations_dir = Path(__file__).parent / 'migration_files'
        migrations = {}

        if not migrations_dir.exists():
            return migrations

        for file_path in migrations_dir.glob('*.py'):
            if file_path.name.startswith('_'):
                continue

            # 动态加载迁移模块
            module_name = f'flowy.core.migration_files.{file_path.stem}'
            try:
                import importlib
                module = importlib.import_module(module_name)

                # 查找 Migration 类
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (isinstance(attr, type)
                            and issubclass(attr, Migration)
                            and attr is not Migration
                            and attr.version):
                        migrations[attr.version] = attr
            except ImportError as e:
                print(f'警告: 无法加载迁移文件 {file_path.name}: {e}')

        return migrations

    def get_pending_migrations(self) -> list[type[Migration]]:
        """获取待执行的迁移"""
        current_version = self.get_current_version()
        all_migrations = self.get_migration_files()

        pending = []
        for version in sorted(all_migrations.keys()):
            if current_version is None or version > current_version:
                pending.append(all_migrations[version])

        return pending

    def create_migrations_table(self, session=None):
        """创建迁移记录表"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        try:
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.migrations_table} (
                    version VARCHAR(14) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
        finally:
            conn.close()

    def backup_database(self) -> Optional[Path]:
        """备份数据库文件"""
        if not self.db_path.exists():
            return None

        backup_dir = self.db_path.parent / 'backups'
        backup_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = backup_dir / f'{self.db_path.stem}_backup_{timestamp}.db'

        try:
            shutil.copy2(self.db_path, backup_path)
            return backup_path
        except Exception as e:
            print(f'备份数据库失败: {e}')
            return None

    def restore_database(self, backup_path: Path) -> bool:
        """从备份恢复数据库"""
        if not backup_path.exists():
            print(f'备份文件不存在: {backup_path}')
            return False

        try:
            shutil.copy2(backup_path, self.db_path)
            return True
        except Exception as e:
            print(f'恢复数据库失败: {e}')
            return False

    def record_migration(self, session, migration: Migration):
        """记录已执行的迁移"""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        try:
            cursor.execute(f'''
                INSERT INTO {self.migrations_table} (version, name, description, applied_at)
                VALUES (?, ?, ?, ?)
            ''', (
                migration.version,
                migration.name,
                migration.description,
                datetime.now().isoformat()
            ))
            conn.commit()
        finally:
            conn.close()

    def run_migration(self, migration: Migration) -> bool:
        """执行单个迁移"""
        from flowy.core.db import get_session

        print(f'执行迁移: {migration.version} - {migration.name}')

        session = get_session()
        try:
            # 执行迁移
            migration.upgrade(session)

            # 记录迁移
            self.record_migration(session, migration)

            print(f'迁移 {migration.version} 执行成功')
            return True
        except Exception as e:
            session.rollback()
            print(f'迁移 {migration.version} 执行失败: {e}')
            return False
        finally:
            session.close()

    def run_migrations(self) -> tuple[int, int, list[str]]:
        """执行所有待执行的迁移

        Returns:
            (成功数量, 失败数量, 错误信息列表)
        """
        pending = self.get_pending_migrations()

        if not pending:
            print('数据库已是最新版本')
            return 0, 0, []

        print(f'发现 {len(pending)} 个待执行的迁移')

        # 确保迁移表存在
        self.create_migrations_table()

        # 备份数据库
        backup_path = self.backup_database()
        if backup_path:
            print(f'数据库已备份到: {backup_path}')

        success_count = 0
        failed_count = 0
        errors = []

        for migration_cls in pending:
            # 实例化迁移类
            migration = migration_cls()
            if self.run_migration(migration):
                success_count += 1
            else:
                failed_count += 1
                errors.append(f'迁移 {migration.version} 失败')

                # 尝试回滚
                if backup_path:
                    print('尝试恢复数据库备份...')
                    if self.restore_database(backup_path):
                        print('数据库已恢复到迁移前的状态')
                    else:
                        print('警告: 数据库恢复失败，数据库可能处于不一致状态')

                # 清除引擎缓存，确保下次连接使用恢复的数据库
                global _history_engine
                _history_engine = None

                break  # 停止后续迁移

        return success_count, failed_count, errors

    def get_migration_history(self) -> list[dict]:
        """获取迁移历史"""
        if not self.db_path.exists():
            return []

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        try:
            cursor.execute(
                f'SELECT version, name, description, applied_at FROM {self.migrations_table} ORDER BY version'
            )
            rows = cursor.fetchall()
            return [
                {
                    'version': row[0],
                    'name': row[1],
                    'description': row[2],
                    'applied_at': row[3]
                }
                for row in rows
            ]
        except sqlite3.OperationalError:
            return []
        finally:
            conn.close()


# 全局迁移管理器实例
_migration_manager: Optional[MigrationManager] = None


def get_migration_manager() -> MigrationManager:
    """获取迁移管理器单例"""
    global _migration_manager
    if _migration_manager is None:
        _migration_manager = MigrationManager()
    return _migration_manager


def run_pending_migrations() -> tuple[int, int, list[str]]:
    """执行待处理的迁移

    Returns:
        (成功数量, 失败数量, 错误信息列表)
    """
    manager = get_migration_manager()
    return manager.run_migrations()
