# llm.py
import json
import os
from typing import List, Dict

from openai import OpenAI

# Make sure OPENAI_API_KEY is set in your env
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Updated to currently available high-intelligence models
SQL_MODEL = "gpt-4o"
ANSWER_MODEL = "gpt-4o"

# To keep prompts efficient
MAX_ROWS_FOR_ANSWER = 120

DB_SCHEMA = """
We have these DuckDB tables and views:

1) candidates(
  state TEXT,
  ac_no INT,
  ac_name TEXT,
  sn INT,
  candidate TEXT,
  party TEXT,
  evm_votes INT,
  postal_votes INT,
  total_votes INT,
  vote_percent DOUBLE,
  is_winner BOOLEAN
)

2) ac_totals(
  state TEXT,
  ac_no INT,
  ac_name TEXT,
  total_evm_votes INT,
  total_postal_votes INT,
  total_votes INT
)

3) party_summary(
  state TEXT,
  party TEXT,
  seats_won INT,
  total_votes BIGINT,
  vote_share DOUBLE
)

4) party_map(
  party_name TEXT,      -- as it appears in the CSV
  canonical_name TEXT,  -- nice standardized name
  short_code TEXT,      -- e.g. 'BJP', 'RJD', 'VIP', 'JDU'
  alliance TEXT         -- e.g. 'NDA', 'MGB', or NULL/OTHER
)

5) candidates_enriched (VIEW) as:
  SELECT
    c.*,
    pm.canonical_name AS party_canonical,
    pm.short_code     AS party_short,
    COALESCE(pm.alliance, 'OTHER') AS alliance
  FROM candidates c
  LEFT JOIN party_map pm ON c.party = pm.party_name;

  Columns:
  state, ac_no, ac_name, sn, candidate, party,
  evm_votes, postal_votes, total_votes, vote_percent, is_winner,
  party_canonical, party_short, alliance

6) party_summary_enriched (VIEW) as:
  SELECT
    ps.state,
    ps.party,
    pm.canonical_name AS party_canonical,
    pm.short_code     AS party_short,
    COALESCE(pm.alliance, 'OTHER') AS alliance,
    ps.seats_won,
    ps.total_votes,
    ps.vote_share
  FROM party_summary ps
  LEFT JOIN party_map pm ON ps.party = pm.party_name;

  Columns:
  state, party, party_canonical, party_short, alliance,
  seats_won, total_votes, vote_share

7) alliance_summary(
  alliance TEXT,        -- 'NDA', 'MGB', 'OTHER'
  seats_won INT,
  total_votes BIGINT,
  vote_share DOUBLE
)

Notes:
- alliance is typically 'NDA', 'MGB', or 'OTHER'.
- party_short contains short codes like 'BJP', 'RJD', 'VIP', 'JDU', etc.
- For queries about alliances (NDA vs MGB), use alliance_summary or group by alliance
  in candidates_enriched or party_summary_enriched.
- For queries that use party short codes (e.g. 'BJP', 'RJD'), filter on party_short
  in candidates_enriched or party_summary_enriched.
"""

# Context to prevent hallucinations (e.g. JSP = Jan Suraaj, not Janata Socialist)
DOMAIN_CONTEXT = """
IMPORTANT PARTY ABBREVIATIONS & CONTEXT:
- JSP = Jan Suraaj Party (Founded by Prashant Kishor). IT IS NOT "Janata Socialist Party".
- RJD = Rashtriya Janata Dal
- JDU = Janata Dal (United)
- VIP = Vikassheel Insaan Party
- HAM = Hindustani Awam Morcha
- CPIML / CPI(ML) = Communist Party of India (Marxist–Leninist) Liberation
- AIMIM = All India Majlis-e-Ittehadul Muslimeen
- IND = Independent candidates (not a political party)
"""


def generate_sql(question: str) -> str:
    """
    Ask the LLM to generate a single SELECT SQL query for DuckDB.
    """
    prompt = f"""
You are a data analyst writing SQL for DuckDB against the following schema:

{DB_SCHEMA}

{DOMAIN_CONTEXT}

Write a SINGLE SQL SELECT query that answers the user's question.

Rules:
- Only use SELECT (no INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/TRUNCATE).
- Do not modify data.
- Prefer concise results (include LIMIT where appropriate).
- For vague questions like "Top candidates", default LIMIT 50.
- Use correct column and table/view names.
- Prefer using candidates_enriched, party_summary_enriched, and alliance_summary
  when working with alliances or party short codes.
- Be careful with table aliases: if you alias a table as "ce", use "ce" consistently.
- Do NOT wrap the query in backticks.
- Return only the SQL, nothing else.

User question: {question}
""".strip()

    resp = client.chat.completions.create(
        model=SQL_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    sql = resp.choices[0].message.content.strip()
    
    # Clean up potential markdown backticks if the model ignored the rule
    if sql.startswith("```sql"):
        sql = sql[6:]
    if sql.startswith("```"):
        sql = sql[3:]
    if sql.endswith("```"):
        sql = sql[:-3]
        
    return sql.strip()


def sql_safe(sql: str) -> bool:
    """
    Very basic safety: ensure it's a SELECT and doesn't contain dangerous keywords.
    """
    lowered = sql.strip().lower()
    if not lowered.startswith("select") and not lowered.startswith("with"):
        return False
    bad = ["insert", "update", "delete", "drop", "alter", "create", "truncate"]
    return not any(b in lowered for b in bad)


def generate_answer_text(question: str, sql: str, rows: List[Dict]) -> str:
    """
    Turn raw table data into a nice natural-language answer.
    We only send up to MAX_ROWS_FOR_ANSWER rows into the prompt to keep it efficient.
    """
    total_rows = len(rows)
    rows_for_llm = rows[:MAX_ROWS_FOR_ANSWER]
    rows_json = json.dumps(rows_for_llm, ensure_ascii=False, indent=2)

    truncation_note = ""
    if total_rows > MAX_ROWS_FOR_ANSWER:
        truncation_note = (
            f"\nNOTE: Only the first {MAX_ROWS_FOR_ANSWER} of {total_rows} rows "
            "are shown in Result rows. Base your answer on these, and you may "
            "describe overall patterns without listing every row."
        )

    prompt = f"""
User question:
{question}

SQL used:
{sql}

{DOMAIN_CONTEXT}

Result rows (JSON, up to {MAX_ROWS_FOR_ANSWER} rows):
{rows_json}
{truncation_note}

Instructions:
- Base your answer ONLY on the information in the result rows.
- Explain the answer clearly in 1–3 short paragraphs.
- Highlight key numbers (vote shares, total votes, seats won, margins, etc.) when relevant.
- If the result is a list (e.g., top candidates or constituencies), summarise patterns
  such as which parties or alliances dominate.
- If there are no rows, say that no matching data was found.
- Do NOT invent data that is not present in the rows.
- STRICTLY use the party abbreviations defined in the CONTEXT (e.g., JSP is Jan Suraaj Party).
""".strip()

    resp = client.chat.completions.create(
        model=ANSWER_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
    )
    text = resp.choices[0].message.content.strip()
    return text