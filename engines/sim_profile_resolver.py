# engines/sim_profile_resolver.py
"""
시뮬레이션 프로파일 리졸버 — Easy / Expert 2모드 지원

Easy Mode:
  selected_level (1~20) → level JSON 로드 → fixed 병합 → 최종 config
  사용자가 개별 옵션 패널에서 무엇을 건드려도 실행에 반영되지 않음.

Expert Mode:
  ConfigManager 현재 값 그대로 사용.
  level_no = None

병합 규칙 (levels_v2.json merge_rule):
  runtime_config = fixed ← level_config[level_id]
  level 값이 fixed 값보다 우선, fixed 값이 current_config 보다 우선.
  즉: current_config → fixed override → level override

CONSERVATIVE 호환:
  levels_v2.json의 LEVEL 13~20은 premarket_manual_mode="CONSERVATIVE" 사용.
  ConfigManager 허용값(MANIA_2/MANIA_1/NORMAL/FEAR_1/FEAR_2)에 없으므로
  자동으로 FEAR_1 로 변환한다.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

log = logging.getLogger("MMEAN.SimProfileResolver")

SIM_MODE_EASY   = "easy"
SIM_MODE_EXPERT = "expert"

# levels_v2.json 기본 경로:  workspace/levels_v2.json  (engines/ → mmean/ → workspace/)
_DEFAULT_LEVEL_JSON = os.path.join(
    os.path.dirname(  # mmean/
        os.path.dirname(os.path.abspath(__file__))  # engines/
    ),
    "workspace", "levels_v2.json",
)

# level JSON에서 올 수 있는 premarket 값 중 ConfigManager 허용값 외의 것 → 변환
_PM_COMPAT: Dict[str, str] = {
    "CONSERVATIVE": "FEAR_1",
}

# level JSON에서 resolved_config에 넣으면 안 되는 메타 키
_META_KEYS = {"label", "style", "desc"}


class SimProfileResolver:
    """
    시뮬레이션 실행 설정 리졸버.

    Easy Mode 병합 규칙 (명령서 원칙 그대로):
        DEFAULT_CONFIG → fixed → level_raw
        ← current_config(수동 expert 설정)는 Easy mode에서 완전히 무시.

    Expert Mode:
        current_config 그대로 반환.

    사용 예:
        resolver = SimProfileResolver(default_config=DEFAULT_CONFIG)
        result   = resolver.resolve(
            mode="easy", selected_level=7,
            current_config=cfg_mgr.get()   # expert mode 전용; easy mode는 무시됨
        )
        runtime_config  = result["resolved_config"]
        level_no        = result["level_no"]        # easy=7, expert=None
        sim_mode        = result["simulation_mode"] # "easy" | "expert"
    """

    def __init__(
        self,
        level_json_path: Optional[str] = None,
        default_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._path   = level_json_path or os.environ.get(
            "SIM_LEVEL_JSON_PATH", _DEFAULT_LEVEL_JSON
        )
        # Easy mode 병합 기반: 반드시 DEFAULT_CONFIG를 주입해야 함.
        # None이면 빈 dict로 폴백(level JSON만 적용).
        self._default_config: Dict[str, Any] = dict(default_config) if default_config else {}
        self._schema = self._load_schema()

    # ──────────────────────────────────────────────────────────────
    # 내부 로더
    # ──────────────────────────────────────────────────────────────
    def _load_schema(self) -> Dict[str, Any]:
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                schema = json.load(f)
            n = len(schema.get("levels", {}))
            log.info(
                "SimProfileResolver: level JSON 로드 | path=%s | levels=%d",
                self._path, n,
            )
            return schema
        except FileNotFoundError:
            log.error(
                "SimProfileResolver: level JSON 없음 — easy mode 사용 불가 | path=%s",
                self._path,
            )
            return {"fixed": {}, "levels": {}}
        except json.JSONDecodeError as e:
            log.error("SimProfileResolver: level JSON 파싱 실패 | %s", e)
            return {"fixed": {}, "levels": {}}

    def reload(self) -> None:
        """런타임 중 JSON 파일 재로드 (운영 중 레벨 수정 시 사용)."""
        self._schema = self._load_schema()

    # ──────────────────────────────────────────────────────────────
    # 공개 API
    # ──────────────────────────────────────────────────────────────
    def get_level_config(self, level: int) -> Optional[Dict[str, Any]]:
        """특정 레벨 원본 딕셔너리 반환 (메타 키 포함). 없으면 None."""
        raw = self._schema.get("levels", {}).get(str(level))
        return dict(raw) if raw else None

    def available_levels(self) -> list[int]:
        """사용 가능한 레벨 번호 목록 (정렬)."""
        return sorted(int(k) for k in self._schema.get("levels", {}).keys())

    def resolve(
        self,
        mode: str,
        selected_level: Optional[int],
        current_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        최종 실행 설정 resolve.

        Args:
            mode:           "easy" | "expert"
            selected_level: 이지모드 레벨 번호 (1~20). expert일 때는 무시.
            current_config: Expert mode에서 그대로 반환할 현재 값.
                            Easy mode에서는 완전히 무시됨 —
                            병합 기반은 생성자에서 받은 default_config 고정.

        Returns:
            {
                "simulation_mode":  "easy" | "expert",
                "level_no":         int | None,
                "resolved_config":  Dict[str, Any],  # 엔진에 주입할 최종 설정
                "level_label":      str,              # "LEVEL_07" (expert="" )
                "level_style":      str,              # "balanced" 등
                "level_desc":       str,              # 한글 설명
            }
        """
        mode = str(mode).lower().strip()
        if mode not in (SIM_MODE_EASY, SIM_MODE_EXPERT):
            log.warning(
                "SimProfileResolver: 알 수 없는 mode=%r → expert 처리", mode
            )
            mode = SIM_MODE_EXPERT

        # ── Expert mode: current_config 그대로 ──────────────────────
        if mode == SIM_MODE_EXPERT:
            return {
                "simulation_mode": SIM_MODE_EXPERT,
                "level_no":        None,
                "resolved_config": dict(current_config),
                "level_label":     "",
                "level_style":     "",
                "level_desc":      "",
            }

        # ── Easy mode ────────────────────────────────────────────────
        if not isinstance(selected_level, int) or not (1 <= selected_level <= 20):
            log.warning(
                "SimProfileResolver: easy mode인데 selected_level=%r 유효하지 않음 "
                "→ level 10(balanced) 기본값 적용",
                selected_level,
            )
            selected_level = 10

        fixed     = dict(self._schema.get("fixed", {}))
        level_raw = self.get_level_config(selected_level) or {}

        # 병합: DEFAULT_CONFIG 기반 → fixed 오버라이드 → level 오버라이드
        # current_config(수동 expert 설정)는 Easy mode에서 완전히 무시.
        resolved = dict(self._default_config)
        resolved.update(fixed)
        resolved.update(level_raw)

        # 메타 키 추출 후 제거
        label = str(resolved.pop("label", f"LEVEL_{selected_level:02d}"))
        style = str(resolved.pop("style", ""))
        desc  = str(resolved.pop("desc",  ""))
        for k in _META_KEYS:          # 혹시 남은 메타 키 방어
            resolved.pop(k, None)

        # premarket_manual_mode 호환 변환 (CONSERVATIVE → FEAR_1)
        pm = str(resolved.get("premarket_manual_mode", "NORMAL")).upper()
        if pm in _PM_COMPAT:
            resolved["premarket_manual_mode"] = _PM_COMPAT[pm]
            log.debug(
                "SimProfileResolver: premarket_manual_mode %s → %s (compat)",
                pm, _PM_COMPAT[pm],
            )

        log.info(
            "SimProfileResolver resolved | mode=easy | level=%d | label=%s | style=%s",
            selected_level, label, style,
        )
        return {
            "simulation_mode": SIM_MODE_EASY,
            "level_no":        selected_level,
            "resolved_config": resolved,
            "level_label":     label,
            "level_style":     style,
            "level_desc":      desc,
        }
