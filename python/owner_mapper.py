import re
import uuid
from typing import Optional, Tuple

from sf_bootstrap import ensure_copilot_bootstrap, get_conn


def parse_tag(query_tag: Optional[str]) -> Tuple[str, str]:
    if not query_tag:
        return "owner-unowned", "UNOWNED"

    tag = query_tag.strip()
    patterns = [
        (r"^dbt:([^:]+):model=([^:]+)", "dbt"),
        (r"^snowpark:([^:]+):job=([^:]+)", "snowpark"),
        (r"^airflow:([^:]+):dag=([^:]+):task=([^:]+)", "airflow"),
    ]
    for pat, source in patterns:
        match = re.search(pat, tag)
        if match:
            key = ":".join([source] + [g for g in match.groups() if g])
            owner_id = "owner-" + re.sub(r"[^a-zA-Z0-9_]", "_", key.lower())
            return owner_id, source
    return "owner-unowned", "UNOWNED"


def refresh_owner_mapping() -> int:
    conn = get_conn()
    inserted = 0
    try:
        ensure_copilot_bootstrap(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT query_tag
                FROM COST_COPILOT.FACT_QUERY
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
                """
            )
            for (query_tag,) in cur.fetchall():
                owner_id, source = parse_tag(query_tag)
                if owner_id == "owner-unowned":
                    continue
                cur.execute(
                    """
                    MERGE INTO COST_COPILOT.DIM_OWNER t
                    USING (SELECT %(owner_id)s AS owner_id) s
                    ON t.owner_id = s.owner_id
                    WHEN NOT MATCHED THEN
                      INSERT (owner_id, team_name, owner_name, severity_policy)
                      VALUES (%(owner_id)s, %(team_name)s, %(owner_name)s, 'MEDIUM')
                    """,
                    {
                        "owner_id": owner_id,
                        "team_name": source.upper(),
                        "owner_name": f"{source} owner",
                    },
                )
                cur.execute(
                    """
                    MERGE INTO COST_COPILOT.JOB_OWNER_MAP t
                    USING (SELECT %(map_id)s AS map_id) s
                    ON t.map_id = s.map_id
                    WHEN NOT MATCHED THEN
                      INSERT (map_id, query_tag_pattern, owner_id, priority, is_active)
                      VALUES (%(map_id)s, %(query_tag_pattern)s, %(owner_id)s, 100, TRUE)
                    """,
                    {
                        "map_id": str(uuid.uuid4()),
                        "query_tag_pattern": query_tag or "",
                        "owner_id": owner_id,
                    },
                )
                inserted += 1
            conn.commit()
    finally:
        conn.close()
    return inserted
