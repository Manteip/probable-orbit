# pr-bot/review.py
import os, re, json, requests, textwrap
from typing import List, Dict, Optional
from openai import AzureOpenAI

# --- GitHub context ---
GITHUB_API = "https://api.github.com"
REPO = os.getenv("REPO")
PR_NUMBER = os.getenv("PR_NUMBER")
TOKEN = os.getenv("GITHUB_TOKEN")
RUN_MODE = os.getenv("RUN_MODE", "summary").lower()  # "summary" | "inline"
FILTER_PREFIX = os.getenv("FILTER_PREFIX", "").strip()  # e.g., "testingazure/"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
}

PROMPT_PATH = "prompts/azure_cost_review.md"

# --- Azure OpenAI ---
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-01-01-preview")
AZURE_OPENAI_CHAT_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_NAME", "gpt-4o-mini")

# hidden marker so we can update same comment each run
BOT_MARKER = "<!-- pr-cost-review-bot -->"

# files we never want to review
SKIP_REGEX = re.compile(
    r"^(?:\.github/|pr-bot/|prompts/)", re.IGNORECASE
)

# ---------------- GitHub helpers ----------------
def gh_get(path, params=None):
    r = requests.get(f"{GITHUB_API}{path}", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def gh_post(path, payload):
    r = requests.post(f"{GITHUB_API}{path}", headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()

def gh_patch(full_url, payload):
    r = requests.patch(full_url, headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()

# ---------------- PR files ----------------
def get_pr_files() -> List[Dict]:
    files = []
    page = 1
    while True:
        batch = gh_get(
            f"/repos/{REPO}/pulls/{PR_NUMBER}/files",
            params={"per_page": 100, "page": page},
        )
        files.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    # apply folder filter and skip-list
    filtered = []
    for f in files:
        name = f["filename"]
        if SKIP_REGEX.search(name):
            continue
        if FILTER_PREFIX and not name.startswith(FILTER_PREFIX):
            continue
        filtered.append(f)
    return filtered

# ---------------- Prompt + model IO ----------------
def load_prompt() -> str:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()

def build_model_input(files: List[Dict]) -> str:
    base = load_prompt()
    parts = [base.strip(), "\n\n=== CODE DIFF (with limited context) ===\n"]
    for f in files:
        filename = f["filename"]
        patch = f.get("patch") or ""
        if not patch:
            continue
        if len(patch) > 30_000:
            patch = patch[:30_000] + "\n... [patch truncated]"
        parts.append(f"\n# File: {filename}\n{patch}")
    if len(parts) == 2:
        parts.append("\n# (No matching changed files in this PR after filtering.)\n")
    return "\n".join(parts)

def initialize_llm() -> AzureOpenAI:
    if not (AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT):
        raise RuntimeError("Azure OpenAI env vars missing.")
    return AzureOpenAI(
        api_key=AZURE_OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )

def call_model(prompt: str) -> str:
    client = initialize_llm()

    inline_contract = textwrap.dedent("""
      If (and only if) you are asked to produce inline comments, return a pure JSON array:
      [
        {"file": "<relative file path from repo root>", "line": <int line number on new code>, "body": "<short actionable comment>"}
      ]
      Do not wrap in markdown. Do not include extra keys. Keep bodies concise and cost-focused.
    """).strip()

    summary_contract = textwrap.dedent("""
      If producing a summary, group findings by file. For each file start with:
      "### File: <relative path>"
      Then include:
      - Problem
      - Cost impact (specific to Azure services)
      - Recommended fix
      - Reference (brief best-practice note)
    """).strip()

    mode_hint = "Produce inline JSON comments only." if RUN_MODE == "inline" else "Produce a single concise markdown summary comment."
    content = f"{mode_hint}\n\n{summary_contract}\n\n{inline_contract}\n\n---\n{prompt}"

    resp = client.chat.completions.create(
        model=AZURE_OPENAI_CHAT_DEPLOYMENT_NAME,
        messages=[
            {"role": "system", "content": "You are a senior Azure cost optimization reviewer. Be precise, cost-focused, and practical."},
            {"role": "user", "content": content},
        ],
        temperature=0.2,
    )
    return resp.choices[0].message.content.strip()

# ---------------- Single-comment helpers ----------------
def find_existing_bot_comment_id() -> Optional[int]:
    page = 1
    while True:
        comments = gh_get(f"/repos/{REPO}/issues/{PR_NUMBER}/comments",
                          params={"per_page": 100, "page": page})
        if not comments:
            break
        for c in comments:
            if BOT_MARKER in (c.get("body") or ""):
                return c["id"]
        if len(comments) < 100:
            break
        page += 1
    return None

def upsert_summary_comment(body_md: str):
    body_with_marker = f"{BOT_MARKER}\n{body_md}"
    existing_id = find_existing_bot_comment_id()
    if existing_id:
        gh_patch(f"{GITHUB_API}/repos/{REPO}/issues/comments/{existing_id}", {"body": body_with_marker})
    else:
        gh_post(f"/repos/{REPO}/issues/{PR_NUMBER}/comments", {"body": body_with_marker})

# ---------------- Inline review posting ----------------
def post_inline_review(comments: List[Dict]):
    review_comments = []
    for c in comments:
        if not all(k in c for k in ("file", "line", "body")):
            continue
        review_comments.append({
            "path": c["file"],
            "line": int(c["line"]),
            "side": "RIGHT",
            "body": c["body"],
        })

    if not review_comments:
        bullets = "• " + "\n• ".join([c.get("body","") for c in comments if c.get("body")])
        upsert_summary_comment("> Inline mapping failed, posting summary instead.\n\n" + bullets)
        return

    gh_post(f"/repos/{REPO}/pulls/{PR_NUMBER}/reviews",
            {"event": "COMMENT", "comments": review_comments})

# ---------------- Main ----------------
def main():
    files = get_pr_files()
    model_input = build_model_input(files)
    analysis = call_model(model_input)

    header = []
    header.append("## Automated Cost Review")
    if FILTER_PREFIX:
        header.append(f"- Filter: `{FILTER_PREFIX}`")
    header.append(f"- Files analyzed (after filter/skip): **{len(files)}**")
    header.append("")
    header_md = "\n".join(header)

    if RUN_MODE == "inline":
        try:
            parsed = json.loads(analysis)
            if isinstance(parsed, list):
                post_inline_review(parsed)
                return
        except json.JSONDecodeError:
            pass

    upsert_summary_comment(header_md + "\n" + analysis)

if __name__ == "__main__":
    main()
