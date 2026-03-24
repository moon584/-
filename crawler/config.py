from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
from typing import Optional

from dotenv import load_dotenv


@dataclass(slots=True)
class Settings:
    """数据库配置项（中文注释，来源于环境变量）。"""

    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str

    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> "Settings":
        """从环境变量/.env文件构建配置（中文注释）。"""
        env_path: Optional[Path] = None
        if env_file:
            env_path = Path(env_file)
        else:
            candidate = Path(".env")
            if candidate.exists():
                env_path = candidate
        if env_path and env_path.exists():
            load_dotenv(env_path)
        return cls(
            db_host=os.getenv("DB_HOST", "127.0.0.1"),
            db_port=int(os.getenv("DB_PORT", "3306")),
            db_user=os.getenv("DB_USER", "root"),
            db_password=os.getenv("DB_PASSWORD", ""),
            db_name=os.getenv("DB_NAME", "job_system"),
        )
