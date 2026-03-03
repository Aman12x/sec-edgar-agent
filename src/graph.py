"""
graph.py — Assembles the AFIP LangGraph pipeline.

Graph topology:
  START → ingest → parse → mapping_agent → judge ─── verified ──→ storage → END
                                  ↑                └── retry (n<MAX) ─┘

State object: PipelineState (Pydantic model, serializable to JSON)
Checkpointer: SqliteSaver — persists state after each node for crash recovery
"""

from __future__ import annotations

from langgraph.graph import END, START, StateGraph

try:
    from langgraph.checkpoint.sqlite import SqliteSaver
except ImportError:
    from langgraph.checkpoint.sqlite.sync import SqliteSaver

from config.settings import config
from src.schemas import PipelineState
from src.nodes.ingest_node import ingest_node
from src.nodes.parse_node import parse_node
from src.nodes.mapping_agent import mapping_agent_node
from src.nodes.judge_node import judge_node, should_retry
from src.nodes.storage_node import storage_node
from src.utils.logger import get_logger

logger = get_logger("graph")


def build_graph(use_checkpointer: bool = True):
    """
    Build and compile the AFIP LangGraph pipeline.

    Args:
        use_checkpointer: Disable for unit tests to avoid SQLite file creation.

    Returns:
        Compiled CompiledStateGraph — call .invoke() or .stream() to run.
    """
    builder = StateGraph(PipelineState)

    # Register all 5 nodes
    builder.add_node("ingest", ingest_node)
    builder.add_node("parse", parse_node)
    builder.add_node("mapping_agent", mapping_agent_node)
    builder.add_node("judge", judge_node)
    builder.add_node("storage", storage_node)

    # Linear edges: ingest → parse → mapping_agent → judge
    builder.add_edge(START, "ingest")
    builder.add_edge("ingest", "parse")
    builder.add_edge("parse", "mapping_agent")
    builder.add_edge("mapping_agent", "judge")

    # Conditional edge: judge decides retry or proceed
    builder.add_conditional_edges(
        "judge",
        should_retry,
        {
            "retry": "mapping_agent",
            "proceed": "storage",
        },
    )

    builder.add_edge("storage", END)

    if use_checkpointer:
        config.ensure_dirs()
        import sqlite3

        conn = sqlite3.connect(config.CHECKPOINT_DB, check_same_thread=False)
        checkpointer = SqliteSaver(conn)
        graph = builder.compile(checkpointer=checkpointer)
        logger.debug(
            f"Graph compiled with SqliteSaver checkpoint: {config.CHECKPOINT_DB}"
        )
    else:
        graph = builder.compile()
        logger.debug("Graph compiled without checkpointer (test mode)")

    return graph


def run_pipeline(ticker: str, filing_type: str = "10-K") -> PipelineState:
    """
    Run the full pipeline for a single ticker.

    The thread_id scopes the LangGraph checkpoint so multiple tickers
    can run concurrently without checkpoint conflicts.

    Args:
        ticker:      Stock ticker symbol (e.g., "AAPL")
        filing_type: SEC form type (default "10-K")

    Returns:
        Final PipelineState with output_path populated on success.
    """
    logger.info(f"Starting pipeline for {ticker} ({filing_type})")
    config.validate()

    app = build_graph()

    initial_state = PipelineState(
        ticker=ticker.upper(),
        filing_type=filing_type,
    )

    run_config = {"configurable": {"thread_id": f"{ticker.upper()}_{filing_type}"}}

    result = app.invoke(initial_state, config=run_config)
    state = PipelineState.model_validate(result)

    if state.output_path:
        logger.info(f"Pipeline complete for {ticker}: {state.output_path}")
    else:
        logger.warning(f"Pipeline complete for {ticker} but no output path set")

    return state
