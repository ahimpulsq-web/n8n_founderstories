"""Strategy implementations for multi-model LLM execution."""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Type, TypeVar

from pydantic import BaseModel

from .errors import OpenRouterAllModelsFailedError
from .parsing import canonicalize_string, is_list_of_dicts
from .types import LLMRunSpec

T = TypeVar("T", bound=BaseModel)


def execute_fallback(
    models: list[str],
    call_fn: Callable[[str], T],
) -> T:
    """Execute fallback strategy: try models sequentially until success.
    
    Args:
        models: List of model names to try
        call_fn: Function that calls a single model and returns result
        
    Returns:
        First successful result
        
    Raises:
        OpenRouterAllModelsFailedError: If all models fail
    """
    errors: dict[str, str] = {}
    
    for model in models:
        try:
            return call_fn(model)
        except Exception as e:
            errors[model] = str(e)
    
    raise OpenRouterAllModelsFailedError(errors)


def execute_vote(
    models: list[str],
    spec: LLMRunSpec,
    schema_model: Type[T],
    call_fn: Callable[[str], T],
) -> T:
    """Execute vote strategy: parallel multi-model call with deterministic merge.
    
    This implements the exact field-level merge logic from the original runner.py:
    - For scalar fields: majority by normalized string with vote_min_wins
    - For list of dicts: per-key majority per index based on default model length
    - For lists with vote="union": union with canonicalized item identity
    - For other lists: item-level majority with vote_min_wins
    
    Args:
        models: List of model names to use
        spec: LLM run specification with vote parameters
        schema_model: Pydantic model class for validation
        call_fn: Function that calls a single model and returns result
        
    Returns:
        Merged result validated against schema_model
        
    Raises:
        ValueError: If models list is empty
        OpenRouterAllModelsFailedError: If all models fail
    """
    if not models:
        raise ValueError("LLMRunSpec.models is empty")
    
    # Limit to vote_k models
    models = models[:spec.vote_k]
    results: dict[str, dict[str, Any]] = {}
    
    # Execute all models in parallel
    def _task(m: str) -> tuple[str, dict[str, Any]]:
        obj = call_fn(m)
        return m, obj.model_dump()
    
    with ThreadPoolExecutor(max_workers=len(models)) as executor:
        futures = {executor.submit(_task, m): m for m in models}
        for future in as_completed(futures):
            model = futures[future]
            try:
                _, output = future.result()
                results[model] = output
            except Exception as exc:
                results[model] = {"__error__": str(exc)}
    
    # Check if any models succeeded
    successful_models = [m for m, out in results.items() if "__error__" not in out]
    if not successful_models:
        errors = {m: out.get("__error__") for m, out in results.items() if "__error__" in out}
        raise OpenRouterAllModelsFailedError(errors)
    
    default_model = models[0]
    final: dict[str, Any] = {}
    
    # Process each field with voting logic
    for field_name in schema_model.model_fields.keys():
        field_info = schema_model.model_fields[field_name]
        
        # Skip excluded fields
        if field_info.json_schema_extra and field_info.json_schema_extra.get("llm_exclude"):
            continue
        
        # Collect values from successful models
        values = {
            m: out.get(field_name)
            for m, out in results.items()
            if "__error__" not in out
        }
        
        vote_mode = (
            field_info.json_schema_extra.get("vote")
            if field_info.json_schema_extra
            else None
        )
        
        # Handle list fields
        if any(isinstance(v, list) for v in values.values()):
            listish = [v for v in values.values() if isinstance(v, list)]
            is_list_dicts = any(is_list_of_dicts(v) for v in listish) and all(
                is_list_of_dicts(v) for v in listish
            )
            
            # List of dicts: per-key majority per index
            if is_list_dicts:
                final[field_name] = _vote_list_of_dicts(
                    values, results, default_model, field_name, spec.vote_min_wins
                )
                continue
            
            # Union mode: collect unique items
            if vote_mode == "union":
                final[field_name] = _vote_union(values)
                continue
            
            # Item-level majority voting
            final[field_name] = _vote_list_items(values, spec.vote_min_wins)
            continue
        
        # Scalar field: majority voting
        final[field_name] = _vote_scalar(
            values, results, default_model, field_name, field_info, successful_models, spec.vote_min_wins
        )
    
    return schema_model.model_validate(final)


def _vote_list_of_dicts(
    values: dict[str, Any],
    results: dict[str, dict[str, Any]],
    default_model: str,
    field_name: str,
    vote_min_wins: int,
) -> list[dict[str, Any]]:
    """Vote on list of dicts: per-key majority per index."""
    default_list = results.get(default_model, {}).get(field_name) or []
    if not isinstance(default_list, list):
        default_list = []
    
    max_len = len(default_list)
    final_list: list[dict[str, Any]] = []
    
    for i in range(max_len):
        # Collect dicts at index i from all models
        dicts_i: dict[str, dict[str, Any]] = {}
        for m, v in values.items():
            if not isinstance(v, list) or i >= len(v):
                continue
            if isinstance(v[i], dict):
                dicts_i[m] = v[i]
        
        default_dict = {}
        if i < len(default_list) and isinstance(default_list[i], dict):
            default_dict = default_list[i]
        
        # Collect all keys
        keys = set(default_dict.keys())
        for d in dicts_i.values():
            keys.update(d.keys())
        
        # Vote on each key
        out_i: dict[str, Any] = {}
        for k in keys:
            canon_map: defaultdict[str, list[str]] = defaultdict(list)
            for m, d in dicts_i.items():
                canon_map[canonicalize_string(str(d.get(k)))].append(m)
            
            winner_value = None
            for canon, ms in canon_map.items():
                if len(ms) >= vote_min_wins:
                    winner_value = dicts_i[ms[0]].get(k)
                    break
            
            if winner_value is None:
                winner_value = default_dict.get(k)
            
            out_i[k] = winner_value
        
        final_list.append(out_i)
    
    return final_list


def _vote_union(values: dict[str, Any]) -> list[Any]:
    """Vote union: collect unique items across all models."""
    seen: dict[str, Any] = {}
    final_list = []
    
    for m, v in values.items():
        if not isinstance(v, list):
            continue
        for item in v:
            canon = canonicalize_string(str(item))
            if canon not in seen:
                seen[canon] = item
                final_list.append(item)
    
    return final_list


def _vote_list_items(values: dict[str, Any], vote_min_wins: int) -> list[Any]:
    """Vote on list items: item-level majority."""
    item_votes: defaultdict[str, list[str]] = defaultdict(list)
    
    for m, v in values.items():
        if not isinstance(v, list):
            continue
        for item in v:
            item_votes[canonicalize_string(str(item))].append(m)
    
    winners = [
        canon
        for canon, ms in item_votes.items()
        if len(ms) >= vote_min_wins
    ]
    
    final_list = []
    for canon in winners:
        # Get original item from first model that had it
        model = item_votes[canon][0]
        for m, v in values.items():
            if m == model and isinstance(v, list):
                for item in v:
                    if canonicalize_string(str(item)) == canon:
                        final_list.append(item)
                        break
                break
    
    return final_list


def _vote_scalar(
    values: dict[str, Any],
    results: dict[str, dict[str, Any]],
    default_model: str,
    field_name: str,
    field_info: Any,
    successful_models: list[str],
    vote_min_wins: int,
) -> Any:
    """Vote on scalar field: majority by normalized string."""
    canon_map: defaultdict[str, list[str]] = defaultdict(list)
    
    for m, v in values.items():
        canon_map[canonicalize_string(str(v))].append(m)
    
    winner_value = None
    for canon, ms in canon_map.items():
        if len(ms) >= vote_min_wins:
            winner_value = results[ms[0]][field_name]
            break
    
    if winner_value is None:
        if field_name in results.get(default_model, {}):
            winner_value = results[default_model][field_name]
        else:
            if field_info.is_required():
                raise ValueError(
                    f"Required field '{field_name}' not returned by any LLM model. "
                    f"Successful models: {successful_models}. "
                    f"Check LLM prompt instructions and schema."
                )
            winner_value = None
    
    return winner_value