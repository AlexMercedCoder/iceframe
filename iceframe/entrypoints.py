"""Console entry points with actionable optional-dependency errors."""


def cli() -> None:
    """Run the administrative CLI."""
    try:
        from iceframe.cli import app
    except ImportError as exc:
        raise SystemExit(
            "The IceFrame CLI requires optional dependencies. "
            "Install them with: pip install 'iceframe[cli]'"
        ) from exc
    app()


def chat() -> None:
    """Run the agent chat CLI."""
    try:
        from iceframe.agent_cli import app
    except ImportError as exc:
        raise SystemExit(
            "The IceFrame chat CLI requires optional dependencies. "
            "Install them with: pip install 'iceframe[agent,cli]'"
        ) from exc
    app()
