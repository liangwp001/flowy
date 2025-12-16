#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Flowy核心模块"""

from flowy.core.config import get_config

# 获取配置的目录
_config = get_config()
DATA_DIR = _config.data_dir
DATABASE_DIR = _config.database_dir
LOG_DIR = _config.log_dir
