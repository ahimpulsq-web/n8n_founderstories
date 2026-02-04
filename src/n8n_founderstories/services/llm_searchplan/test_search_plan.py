from __future__ import annotations

import json
from uuid import uuid4

from n8n_founderstories.services.llm_searchplan.service import interpret_prompt
from n8n_founderstories.services.openrouter.openrouter_client import set_run_context


def test_interpretation():
    request_id = str(uuid4())
    raw_prompt = "Bio Vegan Protein in Germany"

    set_run_context(module="llm_searchplan", request_id=request_id)

    result = interpret_prompt(
        request_id=request_id,
        raw_prompt=raw_prompt,
    )

    print(json.dumps(result.model_dump(), indent=2, ensure_ascii=False))

test_interpretation()
