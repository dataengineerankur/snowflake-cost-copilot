import json
import uuid
from typing import Any, Dict


def emit_event(cur, event_type: str, payload: Dict[str, Any]):
    cur.execute(
        """
        INSERT INTO COST_COPILOT.EVENT_STREAM (event_id, event_type, event_payload)
        SELECT %(event_id)s, %(event_type)s, PARSE_JSON(%(event_payload)s)
        """,
        {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_payload": json.dumps(payload, default=str),
        },
    )
