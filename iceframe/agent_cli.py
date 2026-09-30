"""
CLI Chat Interface for IceFrame AI Agent.
"""

import os

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.prompt import Prompt

app = typer.Typer(help="IceFrame AI Chat - Interactive AI assistant for Iceberg tables")
console = Console()


@app.command()
def chat():
    """Start interactive chat with IceFrame AI Agent"""
    load_dotenv()

    # Check for catalog config
    if not os.getenv("ICEBERG_CATALOG_URI"):
        console.print(
            "[red]Error: ICEBERG_CATALOG_URI not set. Configure your catalog in .env[/red]"
        )
        raise typer.Exit(code=1)

    # Check for LLM API key
    has_llm = any(
        [
            os.getenv("OPENAI_API_KEY"),
            os.getenv("ANTHROPIC_API_KEY"),
            os.getenv("GOOGLE_API_KEY"),
            os.getenv("GEMINI_API_KEY"),
        ]
    )

    if not has_llm:
        console.print("[red]Error: No LLM API key found.[/red]")
        console.print("Set one of: OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY")
        raise typer.Exit(code=1)

    # Initialize IceFrame
    try:
        from iceframe.agent.core import IceFrameAgent
        from iceframe.core import IceFrame
        from iceframe.utils import load_catalog_config_from_env

        config = load_catalog_config_from_env()

        ice = IceFrame(config)
        agent = IceFrameAgent(ice)

        console.print(
            Panel.fit(
                "[bold cyan]IceFrame AI Chat[/bold cyan]\n"
                "Ask questions about your Iceberg tables in natural language.\n"
                "Type 'exit' or 'quit' to end the session.",
                border_style="cyan",
            )
        )

        # Detect LLM
        llm_provider = agent.llm.config.provider
        llm_model = agent.llm.config.model
        console.print(f"[dim]Using: {llm_provider} ({llm_model})[/dim]\n")

        while True:
            try:
                user_input = Prompt.ask("[bold green]You[/bold green]")

                if user_input.lower() in ["exit", "quit", "q"]:
                    console.print("[yellow]Goodbye![/yellow]")
                    break

                if user_input.lower() in ["clear", "reset"]:
                    agent.reset_conversation()
                    console.print("[yellow]Conversation reset.[/yellow]")
                    continue

                if not user_input.strip():
                    continue

                # Get response
                console.print("[dim]Thinking...[/dim]")
                response = agent.chat(user_input)

                # Display response
                console.print(
                    Panel(
                        Markdown(response),
                        title="[bold blue]Assistant[/bold blue]",
                        border_style="blue",
                    )
                )
                console.print()

            except KeyboardInterrupt:
                console.print("\n[yellow]Goodbye![/yellow]")
                break
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")

    except Exception as e:
        console.print(f"[red]Failed to initialize: {e}[/red]")
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    app()
