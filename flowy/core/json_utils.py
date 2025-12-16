"""
JSON序列化工具类 - 基于orjson实现

提供兼容jsonplus的API，同时利用orjson的高性能特性。
"""

import orjson
from typing import Any, Union, Dict, List
from datetime import datetime, date
import uuid


class JSONUtil:
    """基于orjson的JSON序列化工具类"""

    @staticmethod
    def dumps(obj: Any, option: int = None, ensure_ascii: bool = False, **kwargs) -> str:
        """
        将Python对象序列化为JSON字符串

        Args:
            obj: 要序列化的Python对象
            option: orjson序列化选项，默认使用OPT_SERIALIZE_NUMPY
            ensure_ascii: 是否确保ASCII编码（为了兼容标准json模块）
            **kwargs: 其他兼容性参数（会被忽略）

        Returns:
            JSON字符串
        """
        if option is None:
            # 默认选项：序列化numpy、美化输出、处理非ASCII字符
            option = orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_INDENT_2 | orjson.OPT_SERIALIZE_UUID

        # orjson 默认使用UTF-8，所以 ensure_ascii 参数实际上不影响输出
        # 这里保留参数是为了兼容性

        # 处理特殊类型
        obj = JSONUtil._prepare_object(obj)

        return orjson.dumps(obj, option=option).decode('utf-8')

    @staticmethod
    def loads(s: Union[str, bytes]) -> Any:
        """
        将JSON字符串反序列化为Python对象

        Args:
            s: JSON字符串或字节

        Returns:
            Python对象
        """
        if isinstance(s, str):
            s = s.encode('utf-8')
        return orjson.loads(s)

    @staticmethod
    def _prepare_object(obj: Any) -> Any:
        """
        预处理对象，处理orjson不直接支持的类型

        Args:
            obj: 要预处理的Python对象

        Returns:
            预处理后的对象
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, set):
            return list(obj)
        elif hasattr(obj, '__dict__'):
            # 对于自定义对象，尝试序列化其__dict__
            return JSONUtil._prepare_object(obj.__dict__)
        elif isinstance(obj, dict):
            return {k: JSONUtil._prepare_object(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [JSONUtil._prepare_object(item) for item in obj]

        return obj

    @staticmethod
    def json_loads(s: Union[str, bytes]) -> Any:
        """
        兼容jsonplus的json_loads方法

        Args:
            s: JSON字符串或字节

        Returns:
            Python对象
        """
        return JSONUtil.loads(s)

    @staticmethod
    def json_dumps(obj: Any, ensure_ascii: bool = False, **kwargs) -> str:
        """
        兼容jsonplus的json_dumps方法

        Args:
            obj: 要序列化的Python对象
            ensure_ascii: 是否确保ASCII编码（为了兼容性）
            **kwargs: 其他参数（为了兼容性保留）

        Returns:
            JSON字符串
        """
        # 忽略不支持的参数，只处理基本序列化
        return JSONUtil.dumps(obj, ensure_ascii=ensure_ascii)

    @staticmethod
    def safe_loads(s: Union[str, bytes], default: Any = None) -> Any:
        """
        安全的反序列化，出错时返回默认值

        Args:
            s: JSON字符串或字节
            default: 出错时的默认值

        Returns:
            反序列化的对象或默认值
        """
        try:
            return JSONUtil.loads(s)
        except (orjson.JSONDecodeError, TypeError, ValueError):
            return default if default is not None else {}

    @staticmethod
    def safe_dumps(obj: Any, default: str = '{}') -> str:
        """
        安全的序列化，出错时返回默认值

        Args:
            obj: 要序列化的Python对象
            default: 出错时的默认值

        Returns:
            JSON字符串或默认值
        """
        try:
            return JSONUtil.dumps(obj)
        except (TypeError, ValueError):
            return default


# 创建默认实例，兼容原有的使用方式
json = JSONUtil

# 导出常用函数，兼容 import jsonplus as json 的用法
loads = JSONUtil.loads
dumps = JSONUtil.dumps
json_loads = JSONUtil.json_loads
json_dumps = JSONUtil.json_dumps
safe_loads = JSONUtil.safe_loads
safe_dumps = JSONUtil.safe_dumps