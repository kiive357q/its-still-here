"""Command-line interface for *its-still-here*."""

import logging
import threading
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from its_still_here.config import AppConfig, load_config
from its_still_here.core.db import StateDB
from its_still_here.core.pipeline import SyncPipeline
from its_still_here.sources import BaseSourceAdapter, WatchFolderAdapter, WeChatAdapter
from its_still_here.storages import BaseStorageProvider, RcloneProvider, WebDAVProvider

app = typer.Typer(
    help="It's still here - 跨客户端、跨网盘、支持独立代理隔离的 IM 文件防过期自动归档工具"
)
console = Console()

_DEFAULT_CONFIG_TEMPLATE = """\
# It's still here 默认配置文件
staging_dir: ./staging
db_path: ./data/state.db
retry_interval_seconds: 300
max_retries: 15

sources:
  - name: local_inbox
    type: folder
    enabled: true
    watch_dir: ./inbox
    stabilize_seconds: 2.0

  # - name: wechat_pc
  #   type: wechat
  #   enabled: false
  #   listen_chats:
  #     - "文件传输助手"
  #   wechat_version: "4.0"
  #   stabilize_seconds: 2.0

storages:
  - name: local_webdav
    type: webdav
    enabled: true
    base_url: "https://dav.example.com/remote.php/dav/files/user"
    username: "user"
    password: "password"
    root_path: "ItIsStillHere"
    proxy:
      enabled: false
      url: "http://127.0.0.1:7890"

  # - name: onedrive_rclone
  #   type: rclone
  #   enabled: false
  #   remote_name: "myonedrive"
  #   root_path: "ItIsStillHere"
  #   chunk_size: "64M"
  #   retries: 10
  #   tpslimit: 8
  #   proxy:
  #     enabled: false
  #     url: "http://127.0.0.1:7890"

rules:
  - name: archive_all
    enabled: true
    sources: []
    include_chats: []
    exclude_chats: []
    include_exts: []
    exclude_exts: []
    min_size_mb: 0.0
    max_size_mb: null
    target: local_webdav
    path_template: "{source}/{chat_name}/{YYYY}-{MM}/{filename}"
    mode: move
"""

_SOURCE_ADAPTERS: dict[str, type[BaseSourceAdapter]] = {
    "folder": WatchFolderAdapter,
    "wechat": WeChatAdapter,
}

_STORAGE_PROVIDERS: dict[str, type[BaseStorageProvider]] = {
    "rclone": RcloneProvider,
    "webdav": WebDAVProvider,
}


def _load_or_exit(config_path: Path) -> AppConfig:
    """Load *config_path* or print a clear error and exit with code 1."""
    try:
        return load_config(config_path)
    except FileNotFoundError:
        console.print(f"[red]配置文件不存在:[/red] {config_path}")
        raise typer.Exit(code=1) from None
    except Exception as exc:
        console.print(f"[red]加载配置失败 ({config_path}):[/red] {exc}")
        raise typer.Exit(code=1) from None


def _find_example_config() -> Path | None:
    """Locate ``config.example.yaml`` in CWD or the project root."""
    candidates = [
        Path("config.example.yaml"),
        Path(__file__).resolve().parents[2] / "config.example.yaml",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


@app.command()
def init(
    output: Path = typer.Option(
        Path("config.yaml"),
        "--output",
        "-o",
        help="生成的配置文件输出路径",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="若目标文件已存在则覆盖写入",
    ),
) -> None:
    """初始化生成默认 YAML 配置文件。"""
    if output.exists() and not force:
        console.print(
            f"[yellow]配置文件已存在:[/yellow] {output}（如需覆盖请添加 --force）"
        )
        raise typer.Exit(code=1)

    example_path = _find_example_config()
    if example_path is not None:
        content = example_path.read_text(encoding="utf-8")
    else:
        content = _DEFAULT_CONFIG_TEMPLATE

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")
    console.print(f"[green]已生成配置文件:[/green] {output}")


@app.command()
def check(
    config: Path = typer.Option(
        Path("config.yaml"),
        "--config",
        "-c",
        help="配置文件路径",
    ),
) -> None:
    """检查所有已启用数据源与存储后端的健康状态。"""
    cfg = _load_or_exit(config)

    table = Table(title="健康检查结果")
    table.add_column("类别", style="cyan", no_wrap=True)
    table.add_column("名称", style="bold")
    table.add_column("类型", style="magenta")
    table.add_column("状态", no_wrap=True)
    table.add_column("详情")

    any_failed = False

    for src_cfg in cfg.sources:
        if not src_cfg.enabled:
            continue
        adapter_cls = _SOURCE_ADAPTERS.get(src_cfg.type)
        if adapter_cls is None:
            any_failed = True
            table.add_row(
                "Source",
                src_cfg.name,
                src_cfg.type,
                "[red]FAIL[/red]",
                f"未知数据源类型: {src_cfg.type}",
            )
            continue
        ok, msg = adapter_cls(src_cfg).check_health()
        if not ok:
            any_failed = True
        status_text = "[green]OK[/green]" if ok else "[red]FAIL[/red]"
        table.add_row("Source", src_cfg.name, src_cfg.type, status_text, msg)

    for stg_cfg in cfg.storages:
        if not stg_cfg.enabled:
            continue
        provider_cls = _STORAGE_PROVIDERS.get(stg_cfg.type)
        if provider_cls is None:
            any_failed = True
            table.add_row(
                "Storage",
                stg_cfg.name,
                stg_cfg.type,
                "[red]FAIL[/red]",
                f"未知存储类型: {stg_cfg.type}",
            )
            continue
        ok, msg = provider_cls(stg_cfg).check_health()
        if not ok:
            any_failed = True
        status_text = "[green]OK[/green]" if ok else "[red]FAIL[/red]"
        table.add_row("Storage", stg_cfg.name, stg_cfg.type, status_text, msg)

    console.print(table)
    if any_failed:
        raise typer.Exit(code=1)


@app.command()
def run(
    config: Path = typer.Option(
        Path("config.yaml"),
        "--config",
        "-c",
        help="配置文件路径",
    ),
) -> None:
    """启动文件监听与自动归档流水线。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    cfg = _load_or_exit(config)
    pipeline = SyncPipeline(cfg)

    console.print(
        f"[green]启动 SyncPipeline[/green]（数据源: {len(pipeline.sources)}，存储: {len(pipeline.storages)}）"
    )
    pipeline.start()
    try:
        stop_wait = threading.Event()
        while not stop_wait.wait(timeout=1.0):
            pass
    except KeyboardInterrupt:
        console.print("\n[yellow]收到中断信号，正在优雅停止...[/yellow]")
    finally:
        pipeline.stop()
        console.print("[green]已停止。[/green]")


@app.command()
def status(
    config: Path = typer.Option(
        Path("config.yaml"),
        "--config",
        "-c",
        help="配置文件路径",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        "-n",
        min=1,
        help="显示的最近任务数量上限",
    ),
) -> None:
    """查看最近的文件传输任务状态。"""
    cfg = _load_or_exit(config)
    db = StateDB(cfg.db_path)
    try:
        tasks = db.list_recent_tasks(limit)
    finally:
        db.close()

    table = Table(title=f"最近传输任务（最多 {limit} 条）")
    table.add_column("Task ID", style="dim", no_wrap=True)
    table.add_column("文件名", style="bold")
    table.add_column("来源")
    table.add_column("会话")
    table.add_column("目标存储", style="cyan")
    table.add_column("状态", no_wrap=True)
    table.add_column("重试", justify="right")
    table.add_column("更新时间", no_wrap=True)
    table.add_column("错误信息")

    for task in tasks:
        table.add_row(
            task.task_id[:8],
            task.file_name,
            task.source_name,
            task.chat_name,
            task.remote_target,
            task.status.value,
            str(task.retry_count),
            task.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            task.error_message or "",
        )

    console.print(table)
