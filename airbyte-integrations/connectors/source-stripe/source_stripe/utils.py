from typing import Any, Mapping

def invoice_event_filter(item: Mapping[str, Any]) -> bool:
    return not (item.get("total") == 0 or item.get("status") == "draft")
