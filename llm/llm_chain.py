# MMEAN/llm_chain.py
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

import requests


log = logging.getLogger("MMEAN.LLM")

# -------------------------------------------------------------------
# 상수 (env override 가능)
# -------------------------------------------------------------------
LLM_MAX_TOKENS: int         = int(os.getenv("LLM_MAX_TOKENS",          "500"))
MAX_PROVIDER_RETRIES: int   = max(1, int(os.getenv("LLM_MAX_RETRIES",   "3")))
MAX_TOTAL_ATTEMPTS: int     = int(os.getenv("LLM_MAX_TOTAL_ATTEMPTS",   "5"))
RETRY_BACKOFF_BASE: float   = 2.0          # 초: 1차=2s, 2차=4s, 3차=8s
LLM_DAILY_MAX_CALLS: int    = int(os.getenv("LLM_DAILY_MAX_CALLS",     "144"))
LLM_DAILY_MAX_TOKENS: int   = int(os.getenv("LLM_DAILY_MAX_TOKENS",    "500000"))
LLM_MIN_INTERVAL_SEC: float = float(os.getenv("LLM_MIN_INTERVAL_SEC",  "60"))


# -------------------------------------------------------------------
# 예외 타입
# -------------------------------------------------------------------
class LLMError(Exception):
    """LLM 체인 공통 예외."""


class LLMHTTPError(LLMError):
    """HTTP 상태코드 오류."""

    def __init__(self, provider: str, status_code: int, message: str = "") -> None:
        self.provider = provider
        self.status_code = status_code
        self.message = message.strip()
        super().__init__(f"{provider} HTTP {status_code} {self.message}".strip())


class LLMParseError(LLMError):
    """응답 파싱 오류."""


class LLMValidationError(LLMError):
    """스키마 검증 오류."""


# -------------------------------------------------------------------
# 공급자 베이스
# -------------------------------------------------------------------
class LLMProvider:
    """LLM 공급자 기본 클래스."""

    def __init__(self, name: str, api_key: str, model: str) -> None:
        self.name = name
        self.api_key = api_key
        self.model = model
        self.session = requests.Session()
        self.last_tokens: int = 0   # 직전 호출의 총 토큰(input+output)

    async def call(self, prompt: str, system_prompt: str, timeout: tuple[float, float]) -> str:
        """
        비동기 wrapper.
        timeout: (connect_timeout, read_timeout)
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._call_sync(prompt, system_prompt, timeout),
        )

    def _call_sync(self, prompt: str, system_prompt: str, timeout: tuple[float, float]) -> str:
        raise NotImplementedError

    @staticmethod
    def _safe_json(resp: requests.Response) -> Dict[str, Any]:
        try:
            return resp.json()
        except Exception as e:
            raise LLMParseError(f"응답 JSON 파싱 실패: {e} | body={resp.text[:400]}") from e

    @staticmethod
    def _trim_text(text: str, limit: int = 400) -> str:
        text = (text or "").replace("\n", " ").replace("\r", " ").strip()
        return text[:limit]


# -------------------------------------------------------------------
# Claude
# -------------------------------------------------------------------
class ClaudeProvider(LLMProvider):
    """1차: Anthropic Claude."""

    def _call_sync(self, prompt: str, system_prompt: str, timeout: tuple[float, float]) -> str:
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        data = {
            "model": self.model,
            "max_tokens": LLM_MAX_TOKENS,
            "system": system_prompt,
            "messages": [{"role": "user", "content": prompt}],
        }

        resp = self.session.post(url, headers=headers, json=data, timeout=timeout)
        if resp.status_code != 200:
            raise LLMHTTPError(self.name, resp.status_code, self._trim_text(resp.text))

        payload = self._safe_json(resp)
        try:
            content = payload.get("content") or []
            if not content:
                raise KeyError("content 비어 있음")
            text = content[0].get("text", "")
            if not text:
                raise KeyError("content[0].text 비어 있음")
            usage = payload.get("usage") or {}
            self.last_tokens = int(usage.get("input_tokens", 0)) + int(usage.get("output_tokens", 0))
            return text
        except Exception as e:
            raise LLMParseError(f"{self.name} 응답 구조 파싱 실패: {e} | payload={str(payload)[:400]}") from e


# -------------------------------------------------------------------
# GPT
# -------------------------------------------------------------------
class GPTProvider(LLMProvider):
    """2차: OpenAI GPT."""

    def _call_sync(self, prompt: str, system_prompt: str, timeout: tuple[float, float]) -> str:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        data = {
            "model": self.model,
            "max_tokens": LLM_MAX_TOKENS,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_object"},
        }

        resp = self.session.post(url, headers=headers, json=data, timeout=timeout)
        if resp.status_code != 200:
            raise LLMHTTPError(self.name, resp.status_code, self._trim_text(resp.text))

        payload = self._safe_json(resp)
        try:
            choices = payload.get("choices") or []
            if not choices:
                raise KeyError("choices 비어 있음")
            msg = choices[0].get("message") or {}
            text = msg.get("content", "")
            if not text:
                raise KeyError("choices[0].message.content 비어 있음")
            usage = payload.get("usage") or {}
            self.last_tokens = int(usage.get("total_tokens", 0))
            return text
        except Exception as e:
            raise LLMParseError(f"{self.name} 응답 구조 파싱 실패: {e} | payload={str(payload)[:400]}") from e


# -------------------------------------------------------------------
# Gemini
# -------------------------------------------------------------------
class GeminiProvider(LLMProvider):
    """3차: Google Gemini."""

    def _call_sync(self, prompt: str, system_prompt: str, timeout: tuple[float, float]) -> str:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"
        headers = {"Content-Type": "application/json"}
        data = {
            "contents": [
                {
                    "parts": [
                        {"text": f"{system_prompt}\n\nUser: {prompt}"}
                    ]
                }
            ],
            "generationConfig": {
                "response_mime_type": "application/json",
                "maxOutputTokens": LLM_MAX_TOKENS,
            },
        }

        resp = self.session.post(url, headers=headers, json=data, timeout=timeout)
        if resp.status_code != 200:
            raise LLMHTTPError(self.name, resp.status_code, self._trim_text(resp.text))

        payload = self._safe_json(resp)
        try:
            candidates = payload.get("candidates") or []
            if not candidates:
                raise KeyError("candidates 비어 있음")
            content = candidates[0].get("content") or {}
            parts = content.get("parts") or []
            if not parts:
                raise KeyError("content.parts 비어 있음")
            text = parts[0].get("text", "")
            if not text:
                raise KeyError("parts[0].text 비어 있음")
            meta = payload.get("usageMetadata") or {}
            self.last_tokens = int(meta.get("totalTokenCount", 0)) or (
                int(meta.get("promptTokenCount", 0)) + int(meta.get("candidatesTokenCount", 0))
            )
            return text
        except Exception as e:
            raise LLMParseError(f"{self.name} 응답 구조 파싱 실패: {e} | payload={str(payload)[:400]}") from e


# -------------------------------------------------------------------
# 체인
# -------------------------------------------------------------------
class LLMChain:
    """
    MMEAN LLM 폴백 체인.
    - Claude → GPT → Gemini 순차 폴백
    - JSON 정리/추출
    - schema_class.validate_and_clamp() 적용
    - 모든 실패 시 schema_class.default() 반환
    """

    def __init__(
        self,
        timeout_sec: float = 3.0,
        connect_timeout_sec: float = 1.0,
    ) -> None:
        self.timeout_sec = float(timeout_sec)
        self.connect_timeout_sec = float(connect_timeout_sec)
        self.providers: List[LLMProvider] = []
        # 안전 제한 (일일 & 간격)
        self._daily_lock = threading.Lock()
        self._daily_date: str = ""
        self._daily_count: int = 0
        self._daily_tokens: int = 0
        self._last_call_time: float = 0.0
        self._daily_max: int = LLM_DAILY_MAX_CALLS
        self._daily_max_tokens: int = LLM_DAILY_MAX_TOKENS
        self._min_interval_sec: float = LLM_MIN_INTERVAL_SEC
        self.last_provider: str = ""   # 마지막으로 성공한 공급자 이름 (Claude/GPT/Gemini)
        self._setup_providers()

    # ------------------------------------------------------------------
    # 공급자 구성
    # ------------------------------------------------------------------
    def _setup_providers(self) -> None:
        claude_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        gpt_key = os.getenv("OPENAI_API_KEY", "").strip()
        gemini_key = os.getenv("GEMINI_API_KEY", "").strip()

        claude_model = os.getenv("LLM_MODEL_CLAUDE", "claude-sonnet-4-20250514").strip()
        gpt_model = os.getenv("LLM_MODEL_GPT", "gpt-4o-mini").strip()
        gemini_model = os.getenv("LLM_MODEL_GEMINI", "gemini-1.5-flash").strip()

        def _key_hint(key: str) -> str:
            """API 키 앞 6자 + 마지막 4자만 노출 (보안)."""
            if not key:
                return "(없음)"
            if len(key) <= 10:
                return f"(길이={len(key)}, 너무 짧음 — 유효하지 않을 수 있음)"
            return f"{key[:6]}...{key[-4:]} (길이={len(key)})"

        if claude_key:
            self.providers.append(ClaudeProvider("Claude", claude_key, claude_model))
            log.info("Claude 활성 | model=%s | key=%s", claude_model, _key_hint(claude_key))
        else:
            log.warning("Claude 비활성: ANTHROPIC_API_KEY 없음 — .env 확인 필요")

        if gpt_key:
            self.providers.append(GPTProvider("GPT", gpt_key, gpt_model))
            log.info("GPT 활성 | model=%s | key=%s", gpt_model, _key_hint(gpt_key))
        else:
            log.warning("GPT 비활성: OPENAI_API_KEY 없음")

        if gemini_key:
            self.providers.append(GeminiProvider("Gemini", gemini_key, gemini_model))
            log.info("Gemini 활성 | model=%s | key=%s", gemini_model, _key_hint(gemini_key))
        else:
            log.warning("Gemini 비활성: GEMINI_API_KEY 없음")

        if not self.providers:
            log.error(
                "활성화된 LLM 공급자가 없습니다 — ANTHROPIC_API_KEY / OPENAI_API_KEY / GEMINI_API_KEY "
                "중 하나 이상을 .env에 설정하세요. 모든 LLM 호출이 schema.default()로 폴백됩니다."
            )
        else:
            log.info(
                "LLM 체인 구성 완료 | 공급자=%d개 | timeout=(connect=%.1fs, read=%.1fs) "
                "| max_retries=%d | min_interval=%.0fs",
                len(self.providers),
                self.connect_timeout_sec, self.timeout_sec,
                MAX_PROVIDER_RETRIES, self._min_interval_sec,
            )

    # ------------------------------------------------------------------
    # 공개 API
    # ------------------------------------------------------------------
    def _check_limits(self, layer: str) -> bool:
        """
        호출 허용 여부 통합 체크 (thread-safe, 원자적 슬롯 예약).
        통과 시 _daily_count +1, _last_call_time 갱신.
        """
        now = time.time()
        today = time.strftime("%Y-%m-%d")
        with self._daily_lock:
            # 자정 넘으면 카운터 리셋
            if self._daily_date != today:
                self._daily_date = today
                self._daily_count = 0
                self._daily_tokens = 0

            # ① 최소 호출 간격
            elapsed = now - self._last_call_time
            if self._last_call_time > 0 and elapsed < self._min_interval_sec:
                log.warning(
                    "LLM 최소 호출 간격 미충족 | layer=%s | %.1fs 남음",
                    layer, self._min_interval_sec - elapsed,
                )
                return False

            # ② 일일 호출 횟수
            if self._daily_count >= self._daily_max:
                log.error(
                    "LLM 일일 호출 한도 초과 | layer=%s | count=%d/%d",
                    layer, self._daily_count, self._daily_max,
                )
                return False

            # ③ 일일 토큰
            if self._daily_tokens >= self._daily_max_tokens:
                log.error(
                    "LLM 일일 토큰 한도 초과 | layer=%s | tokens=%d/%d",
                    layer, self._daily_tokens, self._daily_max_tokens,
                )
                return False

            # 슬롯 예약
            self._daily_count += 1
            self._last_call_time = now
        return True

    def _record_tokens(self, tokens: int) -> None:
        """성공한 호출의 토큰 수를 일일 누계에 추가."""
        if tokens > 0:
            with self._daily_lock:
                self._daily_tokens += tokens

    def get_safety_stats(self) -> Dict[str, Any]:
        """대시보드 / 로그용 안전 제한 현황."""
        with self._daily_lock:
            return {
                "date": self._daily_date,
                "daily_calls": self._daily_count,
                "daily_calls_max": self._daily_max,
                "daily_tokens": self._daily_tokens,
                "daily_tokens_max": self._daily_max_tokens,
                "last_call_ago_sec": round(time.time() - self._last_call_time, 1) if self._last_call_time else None,
                "min_interval_sec": self._min_interval_sec,
            }

    def _fallback_result(
        self, schema_class: Any, layer: str, attempts: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        default_val = self._default_with_schema(schema_class)
        if not isinstance(default_val, dict):
            default_val = {}
        result = dict(default_val)
        result["_meta"] = {
            "provider": "default",
            "status": "fallback",
            "latency_ms": 0,
            "layer": layer,
            "raw_response": "",
            "attempt_count": len(attempts),
            "model": "default",
            "fallback_depth": len(attempts),
        }
        result["_attempts"] = attempts
        return result

    async def ask(self, system: str, user: str) -> str:
        """FlowEngine LLMCaller 전용 — raw text 반환.

        provider 순서대로 시도 (Claude → GPT → Gemini), 첫 성공 즉시 반환.
        전부 실패하면 마지막 예외를 re-raise.

        schema 검증·DB 기록은 호출자(LLMCaller)가 직접 처리.
        rate-limiting·daily limit은 LLMCallDedup + TTL이 담당하므로 여기선 생략.
        """
        if not self.providers:
            raise RuntimeError("활성화된 LLM 공급자 없음 — API 키 확인 필요")

        timeout_tuple = (self.connect_timeout_sec, self.timeout_sec)
        last_exc: Optional[Exception] = None

        for provider in self.providers:
            try:
                result = await provider.call(
                    prompt=user,
                    system_prompt=system,
                    timeout=timeout_tuple,
                )
                self.last_provider = provider.name
                return result
            except Exception as e:
                last_exc = e
                log.warning(
                    "LLMChain.ask provider=%s 실패 → 다음 공급자 시도 | error=%s",
                    provider.name, e,
                )

        raise last_exc or RuntimeError("모든 LLM 공급자 실패")

    async def query(
        self,
        prompt: str,
        system_prompt: str,
        schema_class: Any,
        layer: str = "unknown",
    ) -> Dict[str, Any]:
        """
        반환 형식:
        {
            ...validated schema fields...,
            "_meta": {
                "provider": "...",
                "status": "success|fallback",
                "latency_ms": ...,
                "layer": "...",
                "raw_response": "...",
                "attempt_count": n
            },
            "_attempts": [
                {"provider":"Claude","status":"success|error","latency_ms":123,
                 "error_type":"...","error":"...","retry_n":1},
                ...
            ]
        }
        """
        # ① 통합 안전 제한 (간격 · 일일호출 · 일일토큰)
        with self._daily_lock:
            _stats_snap = {
                "daily_calls": self._daily_count,
                "daily_max": self._daily_max,
                "last_call_ago_sec": round(time.time() - self._last_call_time, 1) if self._last_call_time else None,
                "min_interval_sec": self._min_interval_sec,
            }
        log.info(
            "LLM query 시작 | layer=%s | providers=%d | daily=%d/%d | last_call_ago=%ss | min_interval=%.0fs",
            layer,
            len(self.providers),
            _stats_snap["daily_calls"], _stats_snap["daily_max"],
            _stats_snap["last_call_ago_sec"],
            _stats_snap["min_interval_sec"],
        )
        if not self._check_limits(layer):
            log.error(
                "LLM query 차단됨 (안전 제한) | layer=%s | daily=%d/%d | last_call_ago=%ss | min_interval=%.0fs",
                layer,
                _stats_snap["daily_calls"], _stats_snap["daily_max"],
                _stats_snap["last_call_ago_sec"],
                _stats_snap["min_interval_sec"],
            )
            return self._fallback_result(schema_class, layer, attempts=[])

        attempts: List[Dict[str, Any]] = []
        timeout_tuple = (self.connect_timeout_sec, self.timeout_sec)
        total_attempts = 0

        for idx, provider in enumerate(self.providers, start=1):
            # ② 총 시도 횟수 cap (모든 공급자 합산)
            if total_attempts >= MAX_TOTAL_ATTEMPTS:
                log.error(
                    "LLM 총 시도 한도 도달 | layer=%s | max=%d | 지금까지 시도=%s → fallback",
                    layer, MAX_TOTAL_ATTEMPTS,
                    [(a.get("provider"), a.get("error_type"), a.get("error", "")[:80]) for a in attempts],
                )
                break

            last_err_info: Optional[Dict[str, Any]] = None

            # ③ 공급자별 retry 루프 (타임아웃·네트워크 오류에만 재시도)
            for retry_n in range(1, MAX_PROVIDER_RETRIES + 1):
                if total_attempts >= MAX_TOTAL_ATTEMPTS:
                    break
                total_attempts += 1
                start = time.perf_counter()
                retryable = False
                err_type = ""
                err_str = ""

                try:
                    log.info(
                        "LLM 요청 | layer=%s | provider=%s | model=%s | try=%d/%d",
                        layer, provider.name, provider.model, retry_n, MAX_PROVIDER_RETRIES,
                    )
                    raw_resp = await provider.call(prompt, system_prompt, timeout_tuple)
                    cleaned = self._extract_json_text(raw_resp)
                    cleaned = self._normalize_json_text(cleaned)
                    try:
                        parsed = json.loads(cleaned)
                    except json.JSONDecodeError as _je:
                        log.warning(
                            "LLM JSON 파싱 실패 | layer=%s | provider=%s | err=%s"
                            " | raw_preview=%s",
                            layer, provider.name, _je,
                            raw_resp[:300].replace("\n", "↵"),
                        )
                        raise LLMParseError(f"JSON 파싱 실패: {_je}") from _je
                    validated = self._validate_with_schema(schema_class, parsed)

                    if not isinstance(validated, dict):
                        raise LLMValidationError("validate_and_clamp() 반환값이 dict가 아닙니다.")

                    latency_ms = int((time.perf_counter() - start) * 1000)
                    attempts.append({
                        "provider": provider.name,
                        "status": "success",
                        "latency_ms": latency_ms,
                        "error_type": "",
                        "error": "",
                        "model": provider.model,
                        "fallback_depth": idx - 1,
                        "retry_n": retry_n,
                    })
                    log.info("LLM 성공 | layer=%s | provider=%s | %dms | try=%d | tokens=%d",
                             layer, provider.name, latency_ms, retry_n, provider.last_tokens)
                    self._record_tokens(provider.last_tokens)

                    result = dict(validated)
                    result["_meta"] = {
                        "provider": provider.name,
                        "status": "success",
                        "latency_ms": latency_ms,
                        "layer": layer,
                        "raw_response": raw_resp[:1000],
                        "attempt_count": len(attempts),
                        "model": provider.model,
                        "fallback_depth": idx - 1,
                    }
                    result["_attempts"] = attempts
                    return result

                except (asyncio.TimeoutError, requests.Timeout) as e:
                    retryable = True
                    err_type = "timeout"
                    err_str = str(e)
                    log.warning("LLM 타임아웃 | layer=%s | provider=%s | try=%d", layer, provider.name, retry_n)

                except requests.RequestException as e:
                    retryable = True
                    err_type = "network_error"
                    err_str = str(e)
                    log.warning("LLM 네트워크 오류 | layer=%s | provider=%s | try=%d | %s", layer, provider.name, retry_n, e)

                except LLMHTTPError as e:
                    # 503 Service Unavailable 만 재시도, 429/4xx 는 재시도 불필요
                    retryable = (e.status_code == 503)
                    err_type = f"http_{e.status_code}"
                    err_str = str(e)
                    log.warning("LLM HTTP 오류 | layer=%s | provider=%s | %s", layer, provider.name, e)

                except LLMParseError as e:
                    retryable = False
                    err_type = "parse_error"
                    err_str = str(e)
                    log.warning("LLM 파싱 오류 | layer=%s | provider=%s | %s", layer, provider.name, e)

                except LLMValidationError as e:
                    retryable = False
                    err_type = "validation_error"
                    err_str = str(e)
                    log.warning("LLM 검증 실패 | layer=%s | provider=%s | %s", layer, provider.name, e)

                except Exception as e:
                    retryable = False
                    err_type = "unknown_error"
                    err_str = str(e)
                    log.exception("LLM 예기치 못한 오류 | layer=%s | provider=%s", layer, provider.name)

                # --- 오류 처리 ---
                latency_ms = int((time.perf_counter() - start) * 1000)
                last_err_info = {
                    "provider": provider.name,
                    "status": "error",
                    "latency_ms": latency_ms,
                    "error_type": err_type,
                    "error": err_str,
                    "model": provider.model,
                    "fallback_depth": idx - 1,
                    "retry_n": retry_n,
                }

                if retryable and retry_n < MAX_PROVIDER_RETRIES:
                    backoff = RETRY_BACKOFF_BASE * (2 ** (retry_n - 1))
                    log.warning(
                        "LLM 재시도 %d/%d | provider=%s | %.1fs 후",
                        retry_n, MAX_PROVIDER_RETRIES, provider.name, backoff,
                    )
                    await asyncio.sleep(backoff)
                else:
                    break  # 이 공급자 포기 → 다음 공급자로 폴백

            if last_err_info:
                attempts.append(last_err_info)

        # ------------------------------------------------------------------
        # 모든 공급자 실패 → 기본값
        # ------------------------------------------------------------------
        failure_summary = [
            f"{a.get('provider')}[{a.get('retry_n')}] {a.get('error_type')}: {str(a.get('error',''))[:120]}"
            for a in attempts
        ]
        log.error(
            "모든 LLM 공급자 호출 실패 | layer=%s | 총시도=%d | 실패요약=%s → schema.default() 적용",
            layer, len(attempts), failure_summary,
        )
        return self._fallback_result(schema_class, layer, attempts)

    # ------------------------------------------------------------------
    # 내부 유틸
    # ------------------------------------------------------------------
    def _validate_with_schema(self, schema_class: Any, parsed: Dict[str, Any]) -> Dict[str, Any]:
        if hasattr(schema_class, "validate_and_clamp"):
            try:
                return schema_class.validate_and_clamp(parsed)
            except Exception as e:
                raise LLMValidationError(f"validate_and_clamp 실패: {e}") from e

        if hasattr(schema_class, "validate"):
            try:
                out = schema_class.validate(parsed)
                if out is False:
                    raise LLMValidationError("schema.validate 결과가 False입니다.")
                return parsed if out is True else out
            except Exception as e:
                raise LLMValidationError(f"validate 실패: {e}") from e

        # 스키마 메서드가 없으면 원본 반환
        return parsed

    def _default_with_schema(self, schema_class: Any) -> Dict[str, Any]:
        if hasattr(schema_class, "default"):
            try:
                return schema_class.default()
            except Exception as e:
                log.exception("schema_class.default() 실패: %s", e)
        return {}

    @staticmethod
    def _normalize_json_text(text: str) -> str:
        """
        LLM이 유효하지 않은 JSON 문법을 사용한 경우 정규화.
        - Python 리터럴: None→null, True→true, False→false
        - trailing comma: ,} / ,] 제거
        - 양수 부호: +숫자 → 숫자 (JSON은 +5 허용 안 함)
        """
        # Python 리터럴 → JSON
        text = re.sub(r'\bNone\b', 'null', text)
        text = re.sub(r'\bTrue\b', 'true', text)
        text = re.sub(r'\bFalse\b', 'false', text)
        # trailing comma before } or ]
        text = re.sub(r',\s*([}\]])', r'\1', text)
        # +숫자 → 숫자 (예: "enter_score_adj": +5  →  "enter_score_adj": 5)
        text = re.sub(r'(?<=[:,\[{])\s*\+(\d)', r' \1', text)
        return text

    @staticmethod
    def _extract_json_text(raw: str) -> str:
        """
        LLM이 코드블록/설명 포함 응답을 준 경우에도 JSON 부분만 추출.
        """
        if raw is None:
            raise LLMParseError("raw response가 None입니다.")

        text = str(raw).strip()
        if not text:
            raise LLMParseError("빈 응답입니다.")

        # ```json ... ``` 제거
        text = re.sub(r"^```json\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"^```\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

        # 이미 JSON이면 바로 반환
        if (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]")):
            return text

        # 첫 JSON object 추출
        obj = LLMChain._extract_balanced_block(text, "{", "}")
        if obj:
            return obj

        # 첫 JSON array 추출
        arr = LLMChain._extract_balanced_block(text, "[", "]")
        if arr:
            return arr

        raise LLMParseError(f"JSON 본문을 찾지 못했습니다. raw={text[:500]}")

    @staticmethod
    def _extract_balanced_block(text: str, open_ch: str, close_ch: str) -> Optional[str]:
        start = text.find(open_ch)
        if start < 0:
            return None

        depth = 0
        in_string = False
        escape = False

        for i in range(start, len(text)):
            ch = text[i]

            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
                continue

            if ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return text[start:i + 1]

        return None

    def close(self) -> None:
        for provider in self.providers:
            try:
                provider.session.close()
            except Exception:
                pass