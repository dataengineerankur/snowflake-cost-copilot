from typing import Dict, List, Tuple


def evaluate_policy(rec: Dict, approvals: List[Dict], allowlist: List[str]) -> Tuple[bool, str]:
    risk = (rec.get("risk") or "").upper()
    object_name = (rec.get("object_name") or "").upper()
    rec_id = rec.get("rec_id")

    if risk != "LOW":
        return False, "RISK_NOT_LOW"
    if object_name not in {x.upper() for x in allowlist}:
        return False, "NOT_ALLOWLISTED"

    matching = [a for a in approvals if a.get("rec_id") == rec_id and a.get("approved")]
    if not matching:
        return False, "NOT_APPROVED"

    if rec.get("requires_two_person", False):
        approvers = {a.get("approved_by") for a in matching if a.get("approved_by")}
        if len(approvers) < 2:
            return False, "TWO_PERSON_APPROVAL_REQUIRED"

    return True, "OK"
