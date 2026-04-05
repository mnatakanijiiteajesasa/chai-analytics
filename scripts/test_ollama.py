#!/usr/bin/env python3
"""
Step 6b — Ollama LLM integration.

Passes the rule engine's structured JSON to a locally-running Mistral/LLaMA model
and returns a plain-language farm narrative. The LLM explains what the ML models
found — it does not do any analysis of its own.

Run on WSL host (where Ollama is running):
    python scripts/test_ollama.py --member KTD-13033

Or from inside the container (if OLLAMA_HOST is reachable):
    docker compose exec ml_engine python scripts/test_ollama.py --member KTD-13033
"""

import os
import sys
import json
import argparse
import time
from pathlib import Path

import requests
from pymongo import MongoClient

# Rule engine import — works whether run from repo root or scripts/ dir
sys.path.insert(0, str(Path(__file__).resolve().parent))
from rule_engine import run_pipeline

MONGODB_URI  = os.getenv("MONGODB_URI",  "mongodb://localhost:27017/chaimterics")
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")

# ── Prompt template ───────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a farm advisor assistant for KTDA smallholder tea farmers in Kenya.
You will be given a structured JSON object containing data about a specific farm:
its current season performance, a yield prediction, a 6-month yield forecast, and a list of recommendations.

Your task is to write a clear, plain-language farm narrative in 3-4 short paragraphs that a farmer
can understand on their phone. Follow these rules strictly:

1. Only use facts from the JSON provided. Do not invent figures, percentages, or advice not in the data.
2. Do not perform any mathematical analysis yourself — the numbers are already computed.
3. Write in a warm, direct, encouraging tone suitable for a smallholder farmer.
4. Mention the farm name and owner name in the opening sentence.
5. Cover: current season performance, what the forecast says, and the top 1-2 recommendations.
6. End with one sentence about the most important action the farmer should take now.
7. Keep the total response under 200 words.
8. Do not use bullet points, headers, or markdown — plain paragraphs only.
"""


def build_prompt(pipeline_result: dict) -> str:
    """Strip heavy/redundant fields before sending to LLM to keep prompt concise."""
    farm     = pipeline_result.get("farm", {})
    current  = pipeline_result.get("current_season", {})
    xgb      = pipeline_result.get("xgb_prediction", {})
    sarima   = pipeline_result.get("sarima_forecast")
    recs     = pipeline_result.get("recommendations", [])
    score    = pipeline_result.get("performance_score", 0)

    # Compact representation — only what the LLM needs for the narrative
    compact = {
        "farm_name":          farm.get("name"),
        "owner_name":         farm.get("owner_name"),
        "factory":            farm.get("factory_code"),
        "collection_centre":  farm.get("collection_centre"),
        "hectares":           farm.get("hectares"),
        "fairtrade":          farm.get("fairtrade"),
        "season_year":        current.get("season_year"),
        "total_kg_so_far":    round(current.get("total_kg", 0), 1),
        "months_complete":    current.get("months_complete"),
        "season_avg_monthly_kg": round(current.get("season_avg_kg", 0), 1) if current.get("season_avg_kg") else None,
        "xgb_predicted_kg":   round(xgb.get("predicted_kg", 0), 1) if xgb.get("predicted_kg") else None,
        "xgb_month":          xgb.get("month_name"),
        "sarima_forecast_6mo": [round(v, 1) for v in sarima["forecast_6mo"]] if sarima else None,
        "performance_score":  score,
        "recommendations":    [
            {"priority": r["priority"], "title": r["title"], "action": r["action"]}
            for r in recs
        ],
    }

    return SYSTEM_PROMPT + "\n\nFarm data:\n" + json.dumps(compact, indent=2)


def call_ollama(prompt: str, model: str = OLLAMA_MODEL,
                host: str = OLLAMA_HOST) -> tuple[str, float]:
    """
    POST to Ollama generate endpoint. Returns (narrative_text, elapsed_seconds).
    """
    url     = f"{host}/api/generate"
    payload = {
        "model":  model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature":    0.3,   # low temperature for factual narrative
            "num_predict":    400,   # ~200 words max
            "top_p":          0.9,
        }
    }
    t0 = time.time()
    try:
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        data      = resp.json()
        narrative = data.get("response", "").strip()
        elapsed   = time.time() - t0
        return narrative, elapsed
    except requests.Timeout:
        return "[ERROR] Ollama timed out after 60s. Is the model loaded?", time.time() - t0
    except requests.ConnectionError as e:
        return f"[ERROR] Cannot reach Ollama at {host}: {e}", time.time() - t0
    except Exception as e:
        return f"[ERROR] {e}", time.time() - t0


def generate_narrative(member_no: str, month_idx: int = 6,
                        model: str = OLLAMA_MODEL) -> dict:
    """
    Full pipeline: rule engine → prompt → Ollama → narrative.
    Returns dict with narrative + pipeline result + timing.
    """
    pipeline_result = run_pipeline(member_no, month_idx)
    if "error" in pipeline_result:
        return {"error": pipeline_result["error"]}

    prompt    = build_prompt(pipeline_result)
    narrative, elapsed = call_ollama(prompt, model=model)

    return {
        "member_no":        member_no,
        "narrative":        narrative,
        "elapsed_seconds":  round(elapsed, 2),
        "pipeline":         pipeline_result,
    }


def check_ollama_health(host: str = OLLAMA_HOST) -> bool:
    try:
        resp = requests.get(f"{host}/api/tags", timeout=5)
        resp.raise_for_status()
        models = [m["name"] for m in resp.json().get("models", [])]
        print(f"  Ollama reachable at {host}")
        print(f"  Available models: {models}")
        if not any(OLLAMA_MODEL in m for m in models):
            print(f"  [WARN] '{OLLAMA_MODEL}' not found. Run: ollama pull {OLLAMA_MODEL}")
            return False
        return True
    except Exception as e:
        print(f"  [FAIL] Ollama unreachable: {e}")
        print(f"  Make sure Ollama is running: OLLAMA_HOST=0.0.0.0 ollama serve")
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--member",    type=str, required=True,
                        help="ktda_member_no, e.g. KTD-13033")
    parser.add_argument("--month-idx", type=int, default=6)
    parser.add_argument("--model",     type=str, default=OLLAMA_MODEL,
                        help="Ollama model name (default: mistral)")
    parser.add_argument("--prompt-only", action="store_true",
                        help="Print the prompt that would be sent, without calling Ollama")
    args = parser.parse_args()

    print("ChaiMetrics — Ollama LLM narrative test")
    print(f"  Member   : {args.member}")
    print(f"  Model    : {args.model}")
    print(f"  Ollama   : {OLLAMA_HOST}")
    print(f"  MongoDB  : {MONGODB_URI}\n")

    if not args.prompt_only:
        print("Checking Ollama ...")
        if not check_ollama_health():
            sys.exit(1)

    print(f"\nRunning rule engine for {args.member} ...")
    pipeline_result = run_pipeline(args.member, args.month_idx)
    if "error" in pipeline_result:
        print(f"[ERROR] {pipeline_result['error']}")
        sys.exit(1)

    prompt = build_prompt(pipeline_result)

    if args.prompt_only:
        print("\n-- Prompt that would be sent to Ollama --\n")
        print(prompt)
        return

    print(f"\nCalling Ollama ({args.model}) ...")
    narrative, elapsed = call_ollama(prompt, model=args.model)

    print(f"\n{'='*60}")
    print("FARM NARRATIVE")
    print('='*60)
    print(narrative)
    print('='*60)
    print(f"\nGenerated in {elapsed:.1f}s")

    if elapsed > 8:
        print(f"[NOTE] Response took {elapsed:.0f}s — consider llama3 if Mistral is slow on your hardware.")

    # Cache result in MongoDB model_outputs
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000)
        db     = client.get_default_database()
        from datetime import datetime
        db.model_outputs.replace_one(
            {"ktda_member_no": args.member},
            {
                "ktda_member_no":  args.member,
                "narrative":       narrative,
                "pipeline_result": {k: v for k, v in pipeline_result.items()
                                    if k not in ("xgb_prediction",)},
                "last_computed":   datetime.utcnow().isoformat(),
                "model_used":      args.model,
                "elapsed_seconds": elapsed,
            },
            upsert=True,
        )
        print(f"Cached to model_outputs collection.")
    except Exception as e:
        print(f"[WARN] Could not cache to MongoDB: {e}")


if __name__ == "__main__":
    main()