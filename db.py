# db.py
from pathlib import Path
import duckdb

# Use in-memory DuckDB
conn = duckdb.connect(database=":memory:")


def init_db():
    base = Path(__file__).parent

    candidates_csv = base / "bihar_2025_candidates.csv"
    ac_totals_csv = base / "bihar_2025_ac_totals.csv"

    # -------- Load base tables from CSV --------
    conn.execute(f"""
        CREATE TABLE candidates AS
        SELECT *
        FROM read_csv_auto('{candidates_csv.as_posix()}', header = TRUE);
    """)

    conn.execute(f"""
        CREATE TABLE ac_totals AS
        SELECT *
        FROM read_csv_auto('{ac_totals_csv.as_posix()}', header = TRUE);
    """)

    # -------- Winner flag per AC --------
    conn.execute("""
        ALTER TABLE candidates ADD COLUMN is_winner BOOLEAN;
    """)

    conn.execute("""
        UPDATE candidates
        SET is_winner = (total_votes = (
            SELECT MAX(total_votes)
            FROM candidates c2
            WHERE c2.state = candidates.state
              AND c2.ac_no = candidates.ac_no
        ));
    """)

    # -------- Party-level summary (per party) --------
    conn.execute("""
        CREATE TABLE party_summary AS
        SELECT
            state,
            party,
            COUNT_IF(is_winner) AS seats_won,
            SUM(total_votes)    AS total_votes
        FROM candidates
        GROUP BY state, party;
    """)

    conn.execute("""
        ALTER TABLE party_summary ADD COLUMN vote_share DOUBLE;
    """)

    conn.execute("""
        UPDATE party_summary ps
        SET vote_share = total_votes * 100.0 / (
            SELECT SUM(total_votes)
            FROM party_summary
            WHERE state = ps.state
        );
    """)

    # -------- Party map: canonical names, short codes, alliances --------
    conn.execute("""
        CREATE TABLE party_map AS
        SELECT *
        FROM (
            VALUES
              -- NDA
              ('Bharatiya Janata Party',              'Bharatiya Janata Party',             'BJP',    'NDA'),
              ('Janata Dal (United)',                 'Janata Dal (United)',                'JDU',    'NDA'),
              ('Lok Janshakti Party (Ram Vilas)',     'Lok Janshakti Party (Ram Vilas)',    'LJP(RV)','NDA'),
              ('Hindustani Awam Morcha (Secular)',    'Hindustani Awam Morcha',             'HAM',    'NDA'),
              ('Rashtriya Lok Morcha',                'Rashtriya Lok Morcha',               'RLM',    'NDA'),

              -- MGB
              ('Rashtriya Janata Dal',                'Rashtriya Janata Dal',               'RJD',    'MGB'),
              ('Indian National Congress',            'Indian National Congress',           'INC',    'MGB'),
              ('Communist Party of India (Marxist-Leninist) (Liberation)',
                                                    'Communist Party of India (Marxist–Leninist)', 'CPI(ML)', 'MGB'),
              ('Communist Party of India (Marxist)',  'Communist Party of India (Marxist)', 'CPI(M)', 'MGB'),
              ('Indian Inclusive Party',              'Indian Inclusive Party',             'IIP',    'MGB'),
              ('Communist Party of India',            'Communist Party of India',           'CPI',    'MGB'),
              ('Vikassheel Insaan Party',             'Vikassheel Insaan Party',            'VIP',    'MGB'),

              -- Other parties
              ('Jan Suraaj Party',                    'Jan Suraaj Party',                   'JSP',    'OTHER'),
              ('Bahujan Samaj Party',                 'Bahujan Samaj Party',                'BSP',    'OTHER'),
              ('All India Majlis-E-Ittehadul Muslimeen',
                                                    'All India Majlis-E-Ittehadul Muslimeen','AIMIM', 'OTHER'),
              ('Janshakti Janta Dal',                 'Janshakti Janta Dal',                'JJD',    'OTHER'),
              ('Aam Aadmi Party',                     'Aam Aadmi Party',                    'AAP',    'OTHER'),

              -- Independents & NOTA
              ('Independent',                         'Independent',                        'IND',    'IND'),
              ('None of the Above',                   'None of the Above',                  'NOTA',   'NOTA')
        ) AS t(party_name, canonical_name, short_code, alliance);
    """)

    # -------- Enriched views with COALESCE fallbacks --------
    # FIX: If party is not in map, fallback to original name instead of NULL
    conn.execute("""
        CREATE OR REPLACE VIEW candidates_enriched AS
        SELECT
            c.*,
            COALESCE(pm.canonical_name, c.party)     AS party_canonical,
            COALESCE(pm.short_code, c.party)         AS party_short,
            COALESCE(pm.alliance, 'OTHER')           AS alliance
        FROM candidates c
        LEFT JOIN party_map pm
          ON c.party = pm.party_name;
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW party_summary_enriched AS
        SELECT
            ps.state,
            ps.party,
            COALESCE(pm.canonical_name, ps.party)    AS party_canonical,
            COALESCE(pm.short_code, ps.party)        AS party_short,
            COALESCE(pm.alliance, 'OTHER')           AS alliance,
            ps.seats_won,
            ps.total_votes,
            ps.vote_share
        FROM party_summary ps
        LEFT JOIN party_map pm
          ON ps.party = pm.party_name;
    """)

    # -------- Alliance-level summary --------
    conn.execute("""
        CREATE TABLE alliance_summary AS
        SELECT
            COALESCE(pm.alliance, 'OTHER') AS alliance,
            COUNT_IF(c.is_winner)          AS seats_won,
            SUM(c.total_votes)             AS total_votes
        FROM candidates c
        LEFT JOIN party_map pm
          ON c.party = pm.party_name
        GROUP BY alliance;
    """)

    conn.execute("""
        ALTER TABLE alliance_summary ADD COLUMN vote_share DOUBLE;
    """)

    conn.execute("""
        UPDATE alliance_summary a
        SET vote_share = total_votes * 100.0 / (
            SELECT SUM(total_votes) FROM alliance_summary
        );
    """)

    conn.execute("""
        ALTER TABLE alliance_summary ADD COLUMN seat_share DOUBLE;
    """)
    conn.execute("""
        ALTER TABLE alliance_summary ADD COLUMN seat_vote_gap DOUBLE;
    """)

    conn.execute("""
        UPDATE alliance_summary a
        SET seat_share = seats_won * 100.0 / (
                SELECT COUNT(DISTINCT ac_no) FROM ac_totals
            );
    """)

    conn.execute("""
        UPDATE alliance_summary a
        SET seat_vote_gap = seat_share - vote_share;
    """)

    # -------- Party performance table (2nd-level metrics) --------
    conn.execute("""
        CREATE TABLE party_performance AS
        WITH party_contested AS (
            SELECT
                ce.party_short,
                ce.party_canonical,
                ce.alliance,
                COUNT(DISTINCT ce.ac_no)       AS seats_contested,
                COUNT_IF(ce.is_winner)         AS seats_won,
                SUM(ce.total_votes)            AS total_votes_contested,
                SUM(act.total_votes)           AS total_votes_in_those_acs
            FROM candidates_enriched ce
            JOIN ac_totals act
              ON act.state = ce.state
             AND act.ac_no = ce.ac_no
            WHERE ce.party_short IS NOT NULL
              AND ce.party_short <> 'NOTA'
            GROUP BY ce.party_short, ce.party_canonical, ce.alliance
        ),
        totals AS (
            SELECT COUNT(DISTINCT ac_no) AS total_seats
            FROM ac_totals
        ),
        ps AS (
            SELECT
                party_short,
                vote_share
            FROM party_summary_enriched
        )
        SELECT
            pc.party_short,
            pc.party_canonical,
            pc.alliance,
            pc.seats_contested,
            pc.seats_won,
            CASE
                WHEN pc.seats_contested > 0 THEN pc.seats_won * 100.0 / pc.seats_contested
                ELSE NULL
            END AS strike_rate,
            CASE
                WHEN pc.seats_contested > 0 THEN pc.total_votes_contested * 1.0 / pc.seats_contested
                ELSE NULL
            END AS avg_votes_per_seat,
            CASE
                WHEN pc.total_votes_in_those_acs > 0 THEN pc.total_votes_contested * 100.0 / pc.total_votes_in_those_acs
                ELSE NULL
            END AS vote_pct_contested,
            ps.vote_share                      AS state_vote_share,
            CASE
                WHEN t.total_seats > 0 THEN pc.seats_won * 100.0 / t.total_seats
                ELSE NULL
            END AS seat_share,
            CASE
                WHEN t.total_seats > 0 THEN
                    pc.seats_won * 100.0 / t.total_seats - COALESCE(ps.vote_share, 0)
                ELSE NULL
            END AS seat_vote_gap
        FROM party_contested pc
        LEFT JOIN ps
          ON pc.party_short = ps.party_short
        CROSS JOIN totals t;
    """)

    # -------- Constituency margins (for Nail-biters & Landslides) --------
    conn.execute("""
        CREATE TABLE constituency_margins AS
        WITH ranked AS (
            SELECT
                ce.state,
                ce.ac_no,
                ce.ac_name,
                ce.candidate,
                ce.party_short,
                ce.party_canonical,
                ce.alliance,
                ce.total_votes,
                ce.vote_percent,
                ROW_NUMBER() OVER (
                    PARTITION BY ce.state, ce.ac_no
                    ORDER BY ce.total_votes DESC
                ) AS rn
            FROM candidates_enriched ce
        )
        SELECT
            r1.state,
            r1.ac_no,
            r1.ac_name,
            r1.candidate         AS winner_candidate,
            r1.party_short       AS winner_party_short,
            r1.party_canonical   AS winner_party_canonical,
            r1.alliance          AS winner_alliance,
            r1.total_votes       AS winner_votes,
            r1.vote_percent      AS winner_vote_percent,
            r2.candidate         AS runner_candidate,
            r2.party_short       AS runner_party_short,
            r2.party_canonical   AS runner_party_canonical,
            r2.alliance          AS runner_alliance,
            r2.total_votes       AS runner_votes,
            r2.vote_percent      AS runner_vote_percent,
            (r1.total_votes - COALESCE(r2.total_votes, 0)) AS margin_votes,
            CASE
                WHEN act.total_votes > 0 THEN
                    (r1.total_votes - COALESCE(r2.total_votes, 0)) * 100.0 / act.total_votes
                ELSE NULL
            END AS margin_percent
        FROM ranked r1
        LEFT JOIN ranked r2
          ON r1.state = r2.state
         AND r1.ac_no = r2.ac_no
         AND r2.rn = 2
        JOIN ac_totals act
          ON act.state = r1.state
         AND act.ac_no = r1.ac_no
        WHERE r1.rn = 1;
    """)

    # -------- NOTA views --------
    conn.execute("""
        CREATE OR REPLACE VIEW nota_by_ac AS
        SELECT
            c.state,
            c.ac_no,
            c.ac_name,
            SUM(CASE WHEN c.party = 'None of the Above' THEN c.total_votes ELSE 0 END) AS nota_votes,
            SUM(c.total_votes) AS ac_total_votes,
            CASE
                WHEN SUM(c.total_votes) > 0 THEN
                    SUM(CASE WHEN c.party = 'None of the Above' THEN c.total_votes ELSE 0 END)
                    * 100.0 / SUM(c.total_votes)
                ELSE NULL
            END AS nota_percent
        FROM candidates c
        GROUP BY c.state, c.ac_no, c.ac_name;
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW nota_summary AS
        SELECT
            SUM(nota_votes)        AS total_nota_votes,
            SUM(ac_total_votes)    AS total_votes,
            CASE
                WHEN SUM(ac_total_votes) > 0 THEN
                    SUM(nota_votes) * 100.0 / SUM(ac_total_votes)
                ELSE NULL
            END AS nota_vote_share,
            COUNT(*) FILTER (WHERE nota_percent > 2.0) AS num_acs_over_2pct,
            COUNT(*) FILTER (WHERE nota_percent > 5.0) AS num_acs_over_5pct
        FROM nota_by_ac;
    """)

    # -------- Independents summary --------
    conn.execute("""
        CREATE OR REPLACE VIEW independents_summary AS
        SELECT
            SUM(CASE WHEN ce.party_short = 'IND' THEN ce.total_votes ELSE 0 END) AS total_ind_votes,
            SUM(ce.total_votes)                                                   AS total_votes,
            CASE
                WHEN SUM(ce.total_votes) > 0 THEN
                    SUM(CASE WHEN ce.party_short = 'IND' THEN ce.total_votes ELSE 0 END)
                    * 100.0 / SUM(ce.total_votes)
                ELSE NULL
            END AS ind_vote_share,
            SUM(CASE WHEN ce.party_short = 'IND' AND ce.is_winner THEN 1 ELSE 0 END) AS seats_won_by_ind
        FROM candidates_enriched ce;
    """)

    print(
        "DuckDB initialized with candidates, ac_totals, party_summary, "
        "party_map, enriched views (with fallbacks), alliance_summary, party_performance, "
        "constituency_margins, nota views, independents_summary."
    )