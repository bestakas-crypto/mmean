# [Directory] MME
# [File] prompt_manager.py
"""
MMEAN Prompt Manager

역할:
  - prompts/*.txt 파일 로드 + mtime 기반 즉시 반영
  - defaults/ 폴백 -> 하드코드 최종 폴백
  - hash 계산 (llm_calls DB 추적용)
  - save / reset / restore / snapshot 이력 관리
  - thread-safe 캐시

정책:
  - 현재 유효 프롬프트 source는 file | default | hardcode 만 사용
  - board / reset / restore 는 현재 source가 아니라 snapshot history(source_type)로만 저장
  - UI badge는 현재 source(file/default/hardcode)만 표시
  - 이력 탭은 action source(board/reset/restore/file)를 그대로 표시

파일 구조:
  prompts/
    strategy_system.txt
    strategy_user_suffix.txt
    opportunity_system.txt
    opportunity_user_suffix.txt
    defaults/
      strategy_system.default.txt
      strategy_user_suffix.default.txt
      opportunity_system.default.txt
      opportunity_user_suffix.default.txt
    archive/           <- save 시 자동 백업
"""
from __future__ import annotations

import hashlib
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("MMEAN.PromptManager")

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
PROMPTS_DIR  = os.path.join(BASE_DIR, "prompts")
DEFAULTS_DIR = os.path.join(PROMPTS_DIR, "defaults")
ARCHIVE_DIR  = os.path.join(PROMPTS_DIR, "archive")

VALID_NAMES = {
    "strategy_system",
    "strategy_user_suffix",
    "opportunity_system",
    "opportunity_user_suffix",
}

# -------------------------------------------------------------------
# 하드코드 최종 폴백 (파일도 defaults도 없을 때)
# -------------------------------------------------------------------
_HARDCODED: Dict[str, str] = {
    "strategy_system": (
        "당신은 한국 선물시장 장전 전략 조정 엔진이다.\n"
        "반드시 JSON 객체만 응답하라. 설명 문장, 마크다운, 코드블록 금지.\n"
        "허용 필드만 반환하라: "
        "{mode, max_trades_per_day, daily_loss_limit, long_weight, short_weight, "
        "enter_score_adj, tp_ticks_adj, sl_ticks_adj, reason}"
    ),
    "strategy_user_suffix": (
        "과도한 공격적 조정은 피하고 delta는 작게 유지하라."
    ),
    "opportunity_system": (
        "당신은 한국 선물시장 장중 기회 필터 엔진이다.\n"
        "반드시 JSON 객체만 응답하라. 설명, 마크다운, 코드블록 금지.\n"
        "당신은 신호 생성자가 아니라 gatekeeper이다.\n"
        "허용 필드만 반환하라: "
        "{opportunity_score, direction_bias, tp_ticks, sl_ticks, valid_minutes, reason}"
    ),
    "opportunity_user_suffix": (
        "애매하면 차단 쪽으로 판단하라."
    ),
}

# combined 조합 정의 (group -> [system_name, suffix_name])
_GROUPS: Dict[str, Tuple[str, str]] = {
    "strategy":    ("strategy_system",    "strategy_user_suffix"),
    "opportunity": ("opportunity_system", "opportunity_user_suffix"),
}


def _sha1(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()[:12]


def _prompt_path(name: str) -> str:
    return os.path.join(PROMPTS_DIR, f"{name}.txt")


def _default_path(name: str) -> str:
    return os.path.join(DEFAULTS_DIR, f"{name}.default.txt")


def _archive_write(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


# -------------------------------------------------------------------
# 캐시 항목
# -------------------------------------------------------------------
class _CacheEntry:
    __slots__ = ("text", "hash", "source", "mtime", "loaded_at")

    def __init__(self, text: str, hash_: str, source: str, mtime: float) -> None:
        self.text      = text
        self.hash      = hash_
        self.source    = source    # "file" | "default" | "hardcode"
        self.mtime     = mtime
        self.loaded_at = time.monotonic()


# -------------------------------------------------------------------
# PromptManager
# -------------------------------------------------------------------
class PromptManager:

    def __init__(
        self,
        db_path: str,
        reload_mode: Optional[str] = None,
        cache_ttl_sec: float = 2.0,
    ) -> None:
        self.db_path     = db_path
        self.cache_ttl   = float(cache_ttl_sec)
        self.reload_mode = (
            reload_mode
            or os.getenv("PROMPT_RELOAD_MODE", "immediate")
        ).lower()

        self._lock  = threading.Lock()
        self._cache: Dict[str, _CacheEntry] = {}

        os.makedirs(PROMPTS_DIR,  exist_ok=True)
        os.makedirs(DEFAULTS_DIR, exist_ok=True)
        os.makedirs(ARCHIVE_DIR,  exist_ok=True)

        self._conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._setup_tables()

        self._write_defaults_if_missing()
        log.info(
            "PromptManager 초기화 | reload_mode=%s | prompts_dir=%s",
            self.reload_mode,
            PROMPTS_DIR,
        )

    # ------------------------------------------------------------------
    # public — 읽기
    # ------------------------------------------------------------------
    def get_prompt(self, name: str) -> str:
        return self._load(name).text

    def get_meta(self, name: str) -> Dict[str, Any]:
        if name not in VALID_NAMES:
            raise ValueError(f"유효하지 않은 prompt name: {name}")
        e = self._load(name)
        path = _prompt_path(name)
        return {
            "name": name,
            "hash": e.hash,
            "source": e.source,   # file | default | hardcode
            "content": e.text,
            "path": path,
            "exists": os.path.exists(path),
        }

    def get_all_info(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for name in VALID_NAMES:
            result[name] = self.get_meta(name)
        return result

    def get_combined_prompt(self, group: str) -> str:
        sys_name, sfx_name = _GROUPS.get(group, ("", ""))
        if not sys_name:
            return ""
        system = self.get_prompt(sys_name)
        suffix = self.get_prompt(sfx_name)
        return f"{system}\n\n{suffix}".strip() if suffix else system

    def get_combined_meta(self, group: str) -> Dict[str, Any]:
        sys_name, sfx_name = _GROUPS.get(group, ("", ""))
        if not sys_name:
            return {"hash": "", "source": "", "name": group}

        e_sys = self._load(sys_name)
        e_sfx = self._load(sfx_name)
        combined_text = self.get_combined_prompt(group)
        combined_hash = _sha1(combined_text)

        # combined source는 "실제 현재 유효 source" 기준으로만 표현
        # system / suffix가 다르면 mixed 로 표시
        if e_sys.source == e_sfx.source:
            combined_source = e_sys.source
        else:
            combined_source = f"mixed({e_sys.source}+{e_sfx.source})"

        return {
            "hash": combined_hash,
            "source": combined_source,
            "name": group,
            "system_hash": e_sys.hash,
            "suffix_hash": e_sfx.hash,
            "system_source": e_sys.source,
            "suffix_source": e_sfx.source,
        }

    # ------------------------------------------------------------------
    # public — 쓰기
    # ------------------------------------------------------------------
    def save_prompt(
        self,
        name: str,
        content: str,
        note: str = "",
        source: str = "board",
    ) -> Dict[str, Any]:
        """
        source 파라미터는 "현재 source"가 아니라
        snapshot history에 기록할 action source(board/reset/restore/file)이다.
        반환값은 항상 실제 현재 meta(file/default/hardcode) 기준으로 돌려준다.
        """
        if name not in VALID_NAMES:
            raise ValueError(f"유효하지 않은 prompt name: {name}")

        content = (content or "").strip()
        path = _prompt_path(name)

        # archive 백업
        ts_str = time.strftime("%Y-%m-%d_%H%M%S")
        archive_path = os.path.join(ARCHIVE_DIR, f"{ts_str}_{name}.txt")
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    _archive_write(archive_path, f.read())
            except Exception:
                pass

        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

        with self._lock:
            self._cache.pop(name, None)

        # 저장 이력에는 action source 저장
        self._insert_snapshot(
            name=name,
            hash_=_sha1(content),
            content=content,
            source_type=(source or "board"),
            note=note,
        )

        # 반환은 현재 실제 meta 기준
        meta = self.get_meta(name)
        log.info(
            "프롬프트 저장 | name=%s | hash=%s | action_source=%s | effective_source=%s",
            name,
            meta["hash"],
            source,
            meta["source"],
        )
        return {"ok": True, **meta}

    def reset_prompt(self, name: str) -> Dict[str, Any]:
        if name not in VALID_NAMES:
            raise ValueError(f"유효하지 않은 prompt name: {name}")

        default_path = _default_path(name)
        if os.path.exists(default_path):
            with open(default_path, encoding="utf-8") as f:
                content = f.read().strip()
        else:
            content = _HARDCODED.get(name, "")

        return self.save_prompt(
            name=name,
            content=content,
            note="reset to default",
            source="reset",
        )

    def list_versions(self, name: str, limit: int = 20) -> List[Dict[str, Any]]:
        if name not in VALID_NAMES:
            raise ValueError(f"유효하지 않은 prompt name: {name}")

        rows = self._conn.execute(
            """
            SELECT id, ts, prompt_hash, source_type, note
            FROM prompt_snapshots
            WHERE prompt_name = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (name, int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]

    def restore_version(self, name: str, snapshot_id: int) -> Dict[str, Any]:
        if name not in VALID_NAMES:
            raise ValueError(f"유효하지 않은 prompt name: {name}")

        row = self._conn.execute(
            """
            SELECT content
            FROM prompt_snapshots
            WHERE id = ? AND prompt_name = ?
            """,
            (int(snapshot_id), name),
        ).fetchone()
        if not row:
            raise ValueError(f"snapshot_id={snapshot_id} 없음")

        return self.save_prompt(
            name=name,
            content=row["content"],
            note=f"restored from #{snapshot_id}",
            source="restore",
        )

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 내부 — 로드/캐시
    # ------------------------------------------------------------------
    def _load(self, name: str) -> _CacheEntry:
        if not name:
            return _CacheEntry("", "", "hardcode", 0.0)

        with self._lock:
            entry = self._cache.get(name)

        if entry is None:
            return self._reload(name)

        if self.reload_mode == "manual":
            return entry

        if self.reload_mode == "ttl":
            if (time.monotonic() - entry.loaded_at) <= self.cache_ttl:
                return entry
            return self._reload(name)

        # default: immediate
        path = _prompt_path(name)
        try:
            mtime = os.path.getmtime(path) if os.path.exists(path) else 0.0
        except Exception:
            mtime = 0.0

        if mtime != entry.mtime:
            return self._reload(name)
        return entry

    def _reload(self, name: str) -> _CacheEntry:
        path = _prompt_path(name)

        # 1) runtime file
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    text = f.read().strip()
                mtime = os.path.getmtime(path)
                entry = _CacheEntry(text, _sha1(text), "file", mtime)
                with self._lock:
                    self._cache[name] = entry
                return entry
            except Exception as e:
                log.warning("프롬프트 파일 읽기 실패 | name=%s | %s", name, e)

        # 2) defaults file
        dpath = _default_path(name)
        if os.path.exists(dpath):
            try:
                with open(dpath, encoding="utf-8") as f:
                    text = f.read().strip()
                entry = _CacheEntry(text, _sha1(text), "default", 0.0)
                with self._lock:
                    self._cache[name] = entry
                return entry
            except Exception as e:
                log.warning("default 파일 읽기 실패 | name=%s | %s", name, e)

        # 3) hardcode fallback
        text = _HARDCODED.get(name, "")
        entry = _CacheEntry(text, _sha1(text), "hardcode", 0.0)
        with self._lock:
            self._cache[name] = entry
        log.warning("프롬프트 hardcode 폴백 | name=%s", name)
        return entry

    # ------------------------------------------------------------------
    # 내부 — DB
    # ------------------------------------------------------------------
    def _setup_tables(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS prompt_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ts          TEXT    NOT NULL,
                prompt_name TEXT    NOT NULL,
                prompt_hash TEXT    NOT NULL,
                source_type TEXT    NOT NULL,
                content     TEXT    NOT NULL,
                note        TEXT
            )
        """)
        self._conn.commit()

    def _insert_snapshot(
        self,
        name: str,
        hash_: str,
        content: str,
        source_type: str,
        note: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO prompt_snapshots
            (ts, prompt_name, prompt_hash, source_type, content, note)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                time.strftime("%Y-%m-%d %H:%M:%S"),
                name,
                hash_,
                source_type,
                content,
                note or None,
            ),
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # 내부 — defaults 초기 생성
    # ------------------------------------------------------------------
    def _write_defaults_if_missing(self) -> None:
        defaults_content = {
            "strategy_system": (
                "당신은 한국 선물시장 장전 전략 조정 엔진이다.\n"
                "반드시 JSON 객체만 응답하라. 설명 문장, 마크다운, 코드블록 금지.\n"
                "오늘 하루 전략 강도를 조정할 때 가장 중요한 해석 변수는 외국인 수급이다.\n"
                "허용 필드만 반환하라: "
                "{mode, max_trades_per_day, daily_loss_limit, long_weight, short_weight, "
                "enter_score_adj, tp_ticks_adj, sl_ticks_adj, reason}"
            ),
            "strategy_user_suffix": (
                "외국인 수급과 OI, Basis, Volume의 동조를 중요하게 보라.\n"
                "과도한 공격적 조정은 피하고 delta는 작게 유지하라."
            ),
            "opportunity_system": (
                "당신은 한국 선물시장 장중 기회 필터 엔진이다.\n"
                "반드시 JSON 객체만 응답하라. 설명, 마크다운, 코드블록 금지.\n"
                "당신은 신호 생성자가 아니라 gatekeeper이다.\n"
                "현재 진입 기회의 질을 평가할 때 외국인 수급을 중요하게 보라.\n"
                "허용 필드만 반환하라: "
                "{opportunity_score, direction_bias, tp_ticks, sl_ticks, valid_minutes, reason}"
            ),
            "opportunity_user_suffix": (
                "애매하면 차단 쪽으로 판단하라.\n"
                "외국인 수급과 OI, Basis, Volume이 동조할 때만 점수를 높여라."
            ),
        }

        for name, text in defaults_content.items():
            dpath = _default_path(name)
            if not os.path.exists(dpath):
                try:
                    with open(dpath, "w", encoding="utf-8") as f:
                        f.write(text.strip())
                except Exception as e:
                    log.warning("default 파일 생성 실패 | name=%s | %s", name, e)