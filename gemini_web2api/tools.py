"""Message-to-prompt conversion, tool-calling injection/parsing and image extraction.

Gemini web has no native multi-turn or tool API, so:
- every request is a fresh single turn; history is flattened into one prompt
- tools are described in the prompt; the model answers with fenced
  ```tool_call {"name": ..., "arguments": ...}``` blocks which we parse back
  into OpenAI-style tool_calls (or Google-style functionCall parts).
"""

import base64
import json
import re
import uuid

TOOL_CALL_TAG = "tool_call"        # OpenAI-flavoured fence tag
FUNCTION_CALL_TAG = "function_call"  # Google native fence tag


class PromptError(ValueError):
    """Raised for malformed client input (bad base64, unsupported parts...)."""


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _safe_json(raw):
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


def _fence(tag, obj):
    return f"```{tag}\n{json.dumps(obj)}\n```"


def _b64decode(data):
    try:
        return base64.b64decode(re.sub(r"\s+", "", data))
    except (ValueError, TypeError):
        raise PromptError("invalid base64 image data")


_FENCED_BLOCK_RE = re.compile(r"```([A-Za-z0-9_+#-]*)\s*(\{.*?\})\s*```", re.DOTALL)


# --------------------------------------------------------------------------
# Content part parsing (text + images)
# --------------------------------------------------------------------------

def _content_to_text(content):
    """Returns (text, image_specs) for str | list-of-parts | None content."""
    if content is None:
        return "", []
    if isinstance(content, str):
        return content, []
    if isinstance(content, list):
        texts, images = [], []
        for part in content:
            if isinstance(part, str):
                texts.append(part)
                continue
            if not isinstance(part, dict):
                continue
            ptype = part.get("type")
            if ptype in ("text", "input_text", "output_text"):
                texts.append(str(part.get("text", "")))
            elif ptype in ("image_url", "input_image", "image"):
                url = part.get("image_url") or part.get("url") or ""
                if isinstance(url, dict):
                    url = url.get("url", "")
                if url:
                    images.append({"source": str(url), "data": None, "mime": None})
            elif ptype in ("inlineData", "inline_data"):
                blob = part.get("inlineData") or part.get("inline_data") or {}
                if blob.get("data"):
                    images.append({
                        "source": "inlineData",
                        "data": _b64decode(blob["data"]),
                        "mime": blob.get("mimeType") or blob.get("mime_type"),
                    })
        return "\n".join(t for t in texts if t), images
    return str(content), []


def resolve_image(spec, timeout=30):
    """Turns an image spec {source|data, mime} into (bytes, mime_or_None)."""
    if spec.get("data"):
        return spec["data"], spec.get("mime")
    src = spec.get("source") or ""
    if not src:
        raise PromptError("image part has no source or data")
    if src.startswith("data:"):
        header, _, b64 = src.partition(",")
        if not b64:
            raise PromptError("data-URL image has no payload")
        mime = header[5:].split(";", 1)[0] or None
        return _b64decode(b64), mime
    if src.startswith(("http://", "https://")):
        return _download_image(src, timeout), None
    cleaned = re.sub(r"\s+", "", src)
    if len(cleaned) >= 64:  # raw base64 blob without a data: prefix
        try:
            return _b64decode(cleaned), None
        except PromptError:
            pass
    raise PromptError(f"unsupported image source: {src[:80]}")


def _download_image(url, timeout=30):
    try:
        import httpx
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.content
    except ImportError:
        pass
    except PromptError:
        raise
    except Exception as exc:
        raise PromptError(f"failed to download image {url[:80]}: {exc}")
    import urllib.request
    opener = urllib.request.build_opener()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with opener.open(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as exc:
        raise PromptError(f"failed to download image {url[:80]}: {exc}")


# --------------------------------------------------------------------------
# Tool definitions / tool_choice
# --------------------------------------------------------------------------

def normalize_tools(tools):
    """Flattens OpenAI/Responses/Google tool formats into {name, description, parameters}."""
    out = []
    if not tools:
        return out
    if isinstance(tools, dict):
        tools = tools.get("functionDeclarations") or []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function") if isinstance(tool.get("function"), dict) else tool
        name = fn.get("name")
        if not name:
            continue
        out.append({
            "name": name,
            "description": fn.get("description", ""),
            "parameters": fn.get("parameters") or {"type": "object", "properties": {}},
        })
    return out


def _choice_name(choice):
    if isinstance(choice, dict):
        fn = choice.get("function") or {}
        name = fn.get("name") or choice.get("name")
        if name:
            return name
        names = choice.get("allowedFunctionNames") or []
        if names:
            return names[0]
    return None


def tool_prompt(tools, tool_choice="auto", google_format=False):
    tag = FUNCTION_CALL_TAG if google_format else TOOL_CALL_TAG
    key = "args" if google_format else "arguments"
    example = {"name": "function_name", key: {"param": "value"}}
    lines = [
        "# Tool Use",
        "You can call the following tools. Call format:",
        f"```{tag}",
        json.dumps(example, indent=2),
        "```",
        "Available tools:",
        json.dumps(tools, indent=2),
        "When the conversation contains a line like "
        "\"[Tool result for <name>]: <output>\", that output is the authoritative "
        "real-time result of that tool call — base your answer on it exactly and "
        "never invent or substitute data.",
    ]
    choice = tool_choice or "auto"
    if choice == "none":
        lines.append("Do NOT call any tools in this response.")
    elif choice == "required":
        lines.append("You MUST call at least one tool in this response.")
    else:
        name = _choice_name(choice)
        if name:
            lines.append(f'Call the tool "{name}" in this response.')
    return "\n".join(lines)


# --------------------------------------------------------------------------
# OpenAI messages -> flat prompt
# --------------------------------------------------------------------------

def _tool_call_block(name, args, google_format=False):
    if google_format:
        return _fence(FUNCTION_CALL_TAG, {"name": name, "args": _as_dict(args)})
    return _fence(TOOL_CALL_TAG, {"name": name, "arguments": _as_dict(args)})


def _as_dict(args):
    if isinstance(args, str):
        parsed = _safe_json(args)
        return parsed if isinstance(parsed, dict) else {"raw": args}
    return args if isinstance(args, dict) else {"raw": args}


def messages_to_prompt(messages, tools=None, tool_choice="auto", google_format=False):
    """Flattens chat messages into one prompt. Returns (prompt, image_specs)."""
    images = []
    system_lines = []
    segments = []  # (role, text)

    for msg in messages or []:
        role = (msg.get("role") or "user").lower()
        text, msg_images = _content_to_text(msg.get("content"))
        images.extend(msg_images)
        if role in ("system", "developer"):
            if text:
                system_lines.append(text)
        elif role == "assistant":
            block = text or ""
            for tc in msg.get("tool_calls") or []:
                if not isinstance(tc, dict):
                    continue
                fn = tc.get("function") or {}
                block += "\n\n" + _tool_call_block(
                    fn.get("name") or tc.get("name") or "function",
                    fn.get("arguments", tc.get("arguments", {})),
                    google_format)
            segments.append(("assistant", block))
        elif role == "tool":
            name = msg.get("name") or "function"
            segments.append(("tool", f"[Tool result for {name}]: {text}"))
        else:
            segments.append(("user", text))

    parts = []
    if system_lines:
        parts.append("[System instruction]: " + "\n".join(system_lines))
    if tools:
        parts.append(tool_prompt(tools, tool_choice, google_format))
    for role, text in segments:
        if not text:
            continue
        if role == "assistant":
            parts.append("[Assistant]: " + text)
        else:
            parts.append(text)
    if any(role == "tool" for role, _ in segments):
        parts.append(
            "NOTE: The lines beginning with \"[Tool result for ...]\" above are the "
            "real outputs of tool calls that were already executed. They override "
            "anything you think you know (including live/current data you may have "
            "access to). Answer the user's latest question using ONLY those results.")
    return "\n\n".join(parts), images


# --------------------------------------------------------------------------
# Tool call parsing (model output back to structured calls)
# --------------------------------------------------------------------------

def parse_tool_calls(text, google_format=False):
    """Extracts tool/function calls from model text.

    Returns (clean_text, calls). calls are OpenAI-style
    {id, type, function:{name, arguments:str}} unless google_format, in which
    case {name, args:dict}. Accepts fenced blocks and (Google mode) raw JSON.
    """
    calls = []

    def _consume(match):
        tag = (match.group(1) or "").lower()
        data = _safe_json(match.group(2))
        is_call = False
        if isinstance(data, dict) and data.get("name"):
            if tag in (TOOL_CALL_TAG, FUNCTION_CALL_TAG):
                is_call = True
            elif google_format and tag in ("", "json") and ("args" in data or "arguments" in data):
                is_call = True
        if is_call:
            calls.append(data)
            return ""
        return match.group(0)

    cleaned = _FENCED_BLOCK_RE.sub(_consume, text or "")

    if google_format and not calls:
        stripped = cleaned.strip()
        if stripped.startswith("{"):
            data = _safe_json(stripped)
            if isinstance(data, dict) and data.get("name"):
                calls.append(data)
                cleaned = ""

    formatted = []
    for call in calls:
        if google_format:
            formatted.append({"name": call.get("name"),
                              "args": call.get("args", call.get("arguments", {})) or {}})
        else:
            args = call.get("arguments", call.get("args", {}))
            if not isinstance(args, str):
                args = json.dumps(args or {})
            formatted.append({
                "id": "call_" + uuid.uuid4().hex[:8],
                "type": "function",
                "function": {"name": call.get("name"), "arguments": args},
            })

    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned, formatted


# --------------------------------------------------------------------------
# Google native (contents / systemInstruction / toolConfig)
# --------------------------------------------------------------------------

def _google_instruction_text(si):
    if not si:
        return ""
    if isinstance(si, str):
        return si
    parts = si.get("parts") or []
    return "\n".join(str(p.get("text", "")) for p in parts
                     if isinstance(p, dict) and p.get("text"))


def _tool_choice_from_config(tool_config):
    if not tool_config:
        return "auto"
    ccfg = (tool_config.get("functionCallingConfig")
            or tool_config.get("function_calling_config") or {})
    mode = (ccfg.get("mode") or "AUTO").upper()
    names = ccfg.get("allowedFunctionNames") or []
    if mode == "NONE":
        return "none"
    if mode == "ANY":
        return names[0] if len(names) == 1 else "required"
    return "auto"


def google_contents_to_messages(contents, system_instruction=None, tool_config=None):
    """Converts Google-native contents into chat messages.

    Returns (messages, image_specs, tool_choice).
    """
    messages, images = [], []
    sys_text = _google_instruction_text(system_instruction)
    if sys_text:
        messages.append({"role": "system", "content": sys_text})

    for entry in contents or []:
        if not isinstance(entry, dict):
            continue
        role = "assistant" if entry.get("role") in ("model", "assistant") else "user"
        segs = []
        for part in entry.get("parts") or []:
            if not isinstance(part, dict):
                continue
            if part.get("text") is not None:
                segs.append(str(part["text"]))
            elif "inlineData" in part or "inline_data" in part:
                blob = part.get("inlineData") or part.get("inline_data") or {}
                if blob.get("data"):
                    images.append({
                        "source": "inlineData",
                        "data": _b64decode(blob["data"]),
                        "mime": blob.get("mimeType") or blob.get("mime_type"),
                    })
            elif "functionCall" in part or "function_call" in part:
                fc = part.get("functionCall") or part.get("function_call") or {}
                segs.append(_fence(FUNCTION_CALL_TAG,
                                   {"name": fc.get("name"), "args": fc.get("args", {}) or {}}))
            elif "functionResponse" in part or "function_response" in part:
                fr = part.get("functionResponse") or part.get("function_response") or {}
                segs.append(f"[Tool result for {fr.get('name', 'function')}]: "
                            f"{json.dumps(fr.get('response', {}) or {})}")
        messages.append({"role": role, "content": "\n\n".join(s for s in segs if s)})

    return messages, images, _tool_choice_from_config(tool_config)


# --------------------------------------------------------------------------
# OpenAI Responses API input (/v1/responses, Codex CLI)
# --------------------------------------------------------------------------

def responses_input_to_messages(input_obj, instructions=None):
    """Converts Responses-API `input` (string or item list) into chat messages."""
    messages = []
    if instructions:
        messages.append({"role": "system", "content": instructions})
    if isinstance(input_obj, str):
        messages.append({"role": "user", "content": input_obj})
        return messages
    call_names = {}
    for item in input_obj or []:
        if isinstance(item, str):
            messages.append({"role": "user", "content": item})
            continue
        if not isinstance(item, dict):
            continue
        itype = item.get("type", "message")
        if itype == "message":
            messages.append({"role": item.get("role", "user"),
                             "content": item.get("content", "")})
        elif itype == "function_call":
            name = item.get("name", "function")
            args = item.get("arguments", "{}")
            if not isinstance(args, str):
                args = json.dumps(args or {})
            call_names[item.get("call_id") or item.get("id") or name] = name
            messages.append({"role": "assistant",
                             "content": _fence(TOOL_CALL_TAG,
                                               {"name": name, "arguments": _safe_json(args) or {}})})
        elif itype == "function_call_output":
            call_id = item.get("call_id", "")
            name = call_names.get(call_id) or call_id or "function"
            output = item.get("output", "")
            if not isinstance(output, str):
                output = json.dumps(output or {})
            messages.append({"role": "tool", "name": name, "content": output})
    return messages
