# MMEAN/routes_core.py
from __future__ import annotations

import hmac
import os
from typing import Any, Dict

from flask import jsonify, render_template, request

from app_bootstrap import apply_runtime_config
from app_state import AppRuntime
from engine_runtime import sync_trend_to_param_store


def register_core_routes(app, runtime: AppRuntime) -> None:
    @app.route("/")
    def index():
        return render_template("dashboard.html")

    @app.route("/api/status")
    def api_status():
        with runtime.state_obj.engine_lock:
            return jsonify({
                **runtime.state,
                "history": runtime.regime_engine.get_recent_history(120),
                "night_history": runtime.night_engine.get_recent_history(limit=120),
            })

    @app.route("/api/mode", methods=["POST"])
    def api_mode():
        payload = request.get_json(silent=True) or {}
        mode = str(payload.get("mode", "smooth")).lower()
        with runtime.state_obj.engine_lock:
            runtime.regime_engine.set_mode(mode)
            runtime.state["mode"] = runtime.regime_engine.mode
        return jsonify({"ok": True, "mode": runtime.regime_engine.mode})

    @app.route("/api/config", methods=["GET"])
    def api_config_get():
        return jsonify({
            "ok": True,
            "hash": runtime.cfg_mgr.get_hash(),
            "config": runtime.cfg_mgr.get(),
            "snapshots": runtime.cfg_mgr.get_snapshots(10),
        })

    @app.route("/api/config", methods=["POST"])
    def api_config_post():
        # Easy Mode에서는 개별 파라미터 수동 변경 차단 — 레벨 JSON이 유일한 설정 소스
        if str(runtime.state.get("simulation_mode", "expert")).lower() == "easy":
            _lvl = runtime.state.get("selected_level")
            return jsonify({
                "ok": False,
                "error": f"Easy Mode(레벨 {_lvl}) 활성 중 — 개별 config 변경 불가. Expert 모드로 전환 후 수정하세요.",
                "simulation_mode": "easy",
                "selected_level": _lvl,
            }), 409

        patch = request.get_json(silent=True) or {}
        if not patch:
            return jsonify({"ok": False, "error": "빈 요청"}), 400
        updated = runtime.cfg_mgr.update(patch)
        with runtime.state_obj.engine_lock:
            apply_runtime_config(runtime)
        sync_trend_to_param_store(runtime, runtime.cfg_mgr.get(), source="api_config")
        return jsonify({"ok": True, "hash": runtime.cfg_mgr.get_hash(), "config": updated})

    @app.route("/api/config/reset", methods=["POST"])
    def api_config_reset():
        # Easy Mode에서는 리셋도 차단
        if str(runtime.state.get("simulation_mode", "expert")).lower() == "easy":
            _lvl = runtime.state.get("selected_level")
            return jsonify({
                "ok": False,
                "error": f"Easy Mode(레벨 {_lvl}) 활성 중 — config 리셋 불가. Expert 모드로 전환 후 리셋하세요.",
                "simulation_mode": "easy",
                "selected_level": _lvl,
            }), 409

        runtime.cfg_mgr.reset_to_default()
        with runtime.state_obj.engine_lock:
            apply_runtime_config(runtime)
        sync_trend_to_param_store(runtime, runtime.cfg_mgr.get(), source="api_config_reset")
        runtime.log.info("CONFIG RESET | hash=%s", runtime.cfg_mgr.get_hash())
        return jsonify({"ok": True, "hash": runtime.cfg_mgr.get_hash(), "config": runtime.cfg_mgr.get()})

    # ------------------------------------------------------------------
    # 선물 종목 / 매매 종목 선택 (노멀·미니)
    # ------------------------------------------------------------------

    @app.route("/api/symbol/list")
    def api_symbol_list():
        """MST 파일에서 KOSPI200 지수선물·미니선물 목록 반환."""
        try:
            from mst_parser import load_contracts
            contracts = [c.to_dict() for c in load_contracts(
                info_types=("1", "B"), unas_filter="KOSPI200"
            )]
        except Exception as e:
            contracts = []
            runtime.log.warning("symbol/list MST 파싱 실패: %s", e)
        return jsonify({
            "ok": True,
            "futures_code":      runtime.settings.get("FUTURES_CODE"),
            "mini_futures_code": runtime.settings.get("MINI_FUTURES_CODE"),
            "trade_instrument":  runtime.settings.get("TRADE_INSTRUMENT", "normal"),
            "contracts":         contracts,
        })

    @app.route("/api/symbol/set", methods=["POST"])
    def api_symbol_set():
        """
        노멀 근월물 변경 (WS 재구독 포함).
        포지션 보유 중에는 변경 불가.
        body: {"futures_code": "A01609"}
        """
        if runtime.order_state is not None:
            pos = runtime.order_state.get_position()
            if not pos.is_flat():
                return jsonify({
                    "ok": False,
                    "error": "포지션 보유 중 종목 변경 불가 — 청산 후 변경하세요.",
                }), 409

        body = request.get_json(silent=True) or {}
        new_code = str(body.get("futures_code", "")).strip()
        if not new_code:
            return jsonify({"ok": False, "error": "futures_code 필요"}), 400

        old_code      = runtime.settings["FUTURES_CODE"]
        old_mini_code = runtime.settings.get("MINI_FUTURES_CODE", "")

        # ── 노멀 코드 갱신 ────────────────────────────────────────────────
        runtime.settings["FUTURES_CODE"] = new_code
        runtime.settings["WS_TR_KEY"]    = new_code

        # ── 미니 코드 동기화: A01XXX → A05XXX (같은 월물) ────────────────
        # 단축코드 구조: A[01|05][연도1자리][월2자리]
        # 노멀·미니 suffix(3자리)가 동일 → prefix만 교체
        try:
            from mst_parser import load_contracts
            suffix        = new_code[3:]          # e.g. "606"
            mini_candidate = "A05" + suffix        # e.g. "A05606"
            # MST에 실제 존재하는지 확인
            all_mini = {c.shrn_iscd for c in load_contracts(("B",), "KOSPI200")}
            if mini_candidate in all_mini:
                runtime.settings["MINI_FUTURES_CODE"] = mini_candidate
                runtime.log.info(
                    "미니 코드 동기화 | %s → %s", old_mini_code, mini_candidate
                )
            else:
                runtime.log.warning(
                    "미니 동기화 후보 %s 가 MST에 없음 — 기존 %s 유지",
                    mini_candidate, old_mini_code,
                )
        except Exception as e:
            runtime.log.warning("미니 코드 동기화 실패 — 기존 유지: %s", e)

        new_mini_code = runtime.settings.get("MINI_FUTURES_CODE", "")

        # ── WS 재연결 요청 (다음 틱 처리 후 자동 재구독) ─────────────────
        if runtime.rt_client is not None:
            runtime.rt_client.change_symbol(new_code)

        runtime.log.info(
            "종목 변경 | 노멀 %s → %s | 미니 %s → %s (WS 재연결 예약)",
            old_code, new_code, old_mini_code, new_mini_code,
        )
        return jsonify({
            "ok":           True,
            "old_code":     old_code,
            "new_code":     new_code,
            "mini_code":    new_mini_code,
        })

    @app.route("/api/instrument", methods=["GET"])
    def api_instrument_get():
        """현재 매매 종목(노멀/미니) 설정 조회."""
        return jsonify({
            "ok":                True,
            "trade_instrument":  runtime.settings.get("TRADE_INSTRUMENT", "normal"),
            "futures_code":      runtime.settings.get("FUTURES_CODE"),
            "mini_futures_code": runtime.settings.get("MINI_FUTURES_CODE"),
        })

    @app.route("/api/instrument", methods=["POST"])
    def api_instrument_set():
        """
        매매 종목 전환: normal(표준선물) ↔ mini(미니선물).
        포지션 보유 중에는 변경 불가.
        body: {"instrument": "mini"}  or  {"instrument": "normal"}
        """
        if runtime.order_state is not None:
            pos = runtime.order_state.get_position()
            if not pos.is_flat():
                return jsonify({
                    "ok": False,
                    "error": "포지션 보유 중 매매 종목 변경 불가 — 청산 후 변경하세요.",
                }), 409

        body = request.get_json(silent=True) or {}
        instrument = str(body.get("instrument", "")).strip().lower()
        if instrument not in ("normal", "mini"):
            return jsonify({"ok": False, "error": "instrument는 normal|mini 만 허용"}), 400

        old = runtime.settings.get("TRADE_INSTRUMENT", "normal")
        runtime.settings["TRADE_INSTRUMENT"] = instrument
        runtime.log.info("매매 종목 전환 | %s → %s", old, instrument)
        return jsonify({
            "ok":                True,
            "old_instrument":    old,
            "trade_instrument":  instrument,
            "order_symbol":      (
                runtime.settings.get("MINI_FUTURES_CODE")
                if instrument == "mini"
                else runtime.settings.get("FUTURES_CODE")
            ),
        })

    # ------------------------------------------------------------------
    # 외국인 수급 신호 모드 (장중 전환 가능)
    # ------------------------------------------------------------------
    _VALID_FOREIGN_MODES = {"composite", "delta_only"}

    @app.route("/api/foreign-signal-mode", methods=["GET"])
    def api_foreign_signal_mode_get():
        return jsonify({
            "ok": True,
            "mode": runtime.state.get("foreign_signal_mode", "composite"),
        })

    @app.route("/api/foreign-signal-mode", methods=["POST"])
    def api_foreign_signal_mode_set():
        payload = request.get_json(silent=True) or {}
        mode = str(payload.get("mode", "")).strip().lower()
        if mode not in _VALID_FOREIGN_MODES:
            return jsonify({"ok": False, "error": f"유효하지 않은 모드: {mode}. 허용값: {_VALID_FOREIGN_MODES}"}), 400
        with runtime.state_obj.engine_lock:
            runtime.state["foreign_signal_mode"] = mode
        runtime.log.info("foreign_signal_mode 변경 → %s", mode)
        return jsonify({"ok": True, "mode": mode})

    # ------------------------------------------------------------------
    # 엔진 안전 제어
    # ------------------------------------------------------------------
    @app.route("/api/engine/resume", methods=["POST"])
    def api_engine_resume():
        """하드스톱 수동 해제. 서킷 브레이커도 같이 리셋."""
        if runtime.error_tracker is None:
            return jsonify({"ok": False, "error": "error_tracker 미초기화"}), 503
        was_halted = runtime.error_tracker.is_hard_stopped()
        runtime.error_tracker.reset_hard_stop()
        runtime.log.warning("엔진 resume 요청 | was_halted=%s", was_halted)
        return jsonify({
            "ok": True,
            "was_halted": was_halted,
            "engine_halted": runtime.state.get("engine_halted", False),
            "circuit_open": runtime.state.get("circuit_open", False),
        })

    # ------------------------------------------------------------------
    # 장전 수동 옵션 API
    # ------------------------------------------------------------------
    _VALID_PM_MODES = {"MANIA_2", "MANIA_1", "NORMAL", "FEAR_1", "FEAR_2"}
    _PM_LABELS = {
        "MANIA_2": "광기 x2",
        "MANIA_1": "광기 x1",
        "NORMAL":  "옵션없음",
        "FEAR_1":  "공포 x1",
        "FEAR_2":  "공포 x2",
    }

    @app.route("/api/premarket/mode", methods=["GET"])
    def api_premarket_mode_get():
        mode = str(runtime.state.get("premarket_manual_mode", "NORMAL"))
        return jsonify({
            "ok": True,
            "mode": mode,
            "label": _PM_LABELS.get(mode, mode),
        })

    @app.route("/api/premarket/mode", methods=["POST"])
    def api_premarket_mode_set():
        payload = request.get_json(silent=True) or {}
        mode = str(payload.get("mode", "")).strip().upper()
        if mode not in _VALID_PM_MODES:
            return jsonify({"ok": False, "error": f"유효하지 않은 mode. 허용: {sorted(_VALID_PM_MODES)}"}), 400

        old_mode = str(runtime.state.get("premarket_manual_mode", "NORMAL"))
        import time as _time
        updated_at = _time.strftime("%Y-%m-%d %H:%M:%S")

        with runtime.state_obj.engine_lock:
            runtime.state["premarket_manual_mode"] = mode
            runtime.state["premarket_last_updated"] = updated_at

        # ConfigManager 에도 영속 저장 (서버 재시작 후에도 유지)
        runtime.cfg_mgr.update({"premarket_manual_mode": mode})

        runtime.log.warning(
            "[PREMARKET] manual mode changed | %s -> %s | at=%s",
            old_mode, mode, updated_at,
        )
        return jsonify({
            "ok": True,
            "mode": mode,
            "label": _PM_LABELS.get(mode, mode),
            "previous": old_mode,
            "updated_at": updated_at,
        })

    # ------------------------------------------------------------------
    # 장중 LLM 영향도 API
    # ------------------------------------------------------------------
    _VALID_LLM_INFLUENCES = {"LOW", "MID", "HIGH"}
    _LLM_INFLUENCE_LABELS = {
        "LOW":  "낮음 (×0.5)",
        "MID":  "보통 (×1.0)",
        "HIGH": "높음 (×1.5)",
    }

    @app.route("/api/llm/influence", methods=["GET"])
    def api_llm_influence_get():
        level = str(runtime.state.get("llm_intraday_influence", "MID"))
        return jsonify({
            "ok": True,
            "level": level,
            "label": _LLM_INFLUENCE_LABELS.get(level, level),
        })

    @app.route("/api/llm/influence", methods=["POST"])
    def api_llm_influence_set():
        payload = request.get_json(silent=True) or {}
        level = str(payload.get("level", "")).strip().upper()
        if level not in _VALID_LLM_INFLUENCES:
            return jsonify({"ok": False, "error": f"유효하지 않은 level. 허용: LOW / MID / HIGH"}), 400

        old_level = str(runtime.state.get("llm_intraday_influence", "MID"))

        with runtime.state_obj.engine_lock:
            runtime.state["llm_intraday_influence"] = level

        runtime.cfg_mgr.update({"llm_intraday_influence": level})

        runtime.log.info(
            "[LLM_INFLUENCE] level changed | %s -> %s",
            old_level, level,
        )
        return jsonify({
            "ok": True,
            "level": level,
            "label": _LLM_INFLUENCE_LABELS.get(level, level),
            "previous": old_level,
        })

    @app.route("/api/llm/safety", methods=["GET"])
    def api_llm_safety():
        """LLM 안전 제한 현황 (일일 호출 · 토큰 · 간격)."""
        chain = runtime.llm_chain
        if chain is None:
            return jsonify({"ok": False, "error": "LLM 비활성"}), 200
        stats = chain.get_safety_stats()
        et = runtime.error_tracker
        return jsonify({
            "ok": True,
            "llm": stats,
            "engine": {
                "halted": et.is_hard_stopped() if et else False,
                "circuit_open": runtime.state.get("circuit_open", False),
                "engine_halted": runtime.state.get("engine_halted", False),
            },
        })

    # ------------------------------------------------------------------
    # 시뮬레이션 모드 API (Easy / Expert)
    # ------------------------------------------------------------------
    @app.route("/api/sim/mode", methods=["GET"])
    def api_sim_mode_get():
        mode = str(runtime.state.get("simulation_mode", "expert"))
        level = runtime.state.get("selected_level")
        result = {"ok": True, "simulation_mode": mode, "selected_level": level}
        if runtime.sim_profile_resolver and mode == "easy" and isinstance(level, int):
            lc = runtime.sim_profile_resolver.get_level_config(level)
            if lc:
                result["level_label"] = str(lc.get("label", f"LEVEL_{level:02d}"))
                result["level_style"] = str(lc.get("style", ""))
                result["level_desc"]  = str(lc.get("desc",  ""))
        return jsonify(result)

    @app.route("/api/sim/mode", methods=["POST"])
    def api_sim_mode_set():
        payload = request.get_json(silent=True) or {}
        mode = str(payload.get("mode", "")).strip().lower()
        if mode not in ("easy", "expert"):
            return jsonify({"ok": False, "error": "mode 허용값: easy | expert"}), 400
        old_mode = str(runtime.state.get("simulation_mode", "expert"))
        with runtime.state_obj.engine_lock:
            runtime.state["simulation_mode"] = mode
            if mode == "expert":
                runtime.state["selected_level"] = None
            apply_runtime_config(runtime)
        runtime.log.info("[SIM_MODE] %s → %s", old_mode, mode)
        return jsonify({"ok": True, "simulation_mode": mode, "previous": old_mode})

    @app.route("/api/sim/level", methods=["GET"])
    def api_sim_level_get():
        level = runtime.state.get("selected_level")
        avail = []
        if runtime.sim_profile_resolver:
            avail = runtime.sim_profile_resolver.available_levels()
        return jsonify({"ok": True, "selected_level": level, "available_levels": avail})

    @app.route("/api/sim/level", methods=["POST"])
    def api_sim_level_set():
        payload = request.get_json(silent=True) or {}
        try:
            level = int(payload.get("level", 0))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "level은 정수여야 합니다"}), 400
        if not (1 <= level <= 20):
            return jsonify({"ok": False, "error": "level 허용 범위: 1 ~ 20"}), 400
        old_level = runtime.state.get("selected_level")
        with runtime.state_obj.engine_lock:
            runtime.state["selected_level"] = level
            runtime.state["simulation_mode"] = "easy"
            apply_runtime_config(runtime)
        runtime.log.info("[SIM_LEVEL] %s → %d (easy mode 자동 전환)", old_level, level)
        result: Dict[str, Any] = {"ok": True, "selected_level": level, "previous": old_level}
        if runtime.sim_profile_resolver:
            lc = runtime.sim_profile_resolver.get_level_config(level)
            if lc:
                result["level_label"] = str(lc.get("label", f"LEVEL_{level:02d}"))
                result["level_style"] = str(lc.get("style", ""))
                result["level_desc"]  = str(lc.get("desc",  ""))
        return jsonify(result)

    # ------------------------------------------------------------------
    # 실행 모드 API (OFF / VIRTUAL / PAPER / LIVE)
    # ------------------------------------------------------------------
    @app.route("/api/execution/mode", methods=["GET"])
    def api_execution_mode_get():
        return jsonify({
            "ok": True,
            "execution_mode": runtime.state.get("execution_mode", "OFF"),
            "execution_enabled": runtime.settings.get("EXECUTION_ENABLED", False),
            "order_env": runtime.settings.get("ORDER_ENV", "virtual"),
        })

    @app.route("/api/execution/mode", methods=["POST"])
    def api_execution_mode_set():
        payload = request.get_json(silent=True) or {}
        mode = str(payload.get("mode", "")).strip().upper()
        if mode not in ("OFF", "VIRTUAL", "PAPER", "LIVE"):
            return jsonify({"ok": False, "error": "mode 허용값: OFF | VIRTUAL | PAPER | LIVE"}), 400

        # PAPER 전환 게이트: order_state 초기화 여부 확인
        if mode == "PAPER" and runtime.order_state is None:
            return jsonify({
                "ok": False,
                "error": "order_state 미초기화 — HTS_ID 설정 확인 후 재시작",
            }), 503

        # LIVE 전환 게이트 1: 비밀번호 검증
        if mode == "LIVE":
            stored_pw = runtime.settings.get("LIVE_PASSWORD", "")
            if not stored_pw:
                return jsonify({
                    "ok": False,
                    "error": "LIVE_PASSWORD 미설정 — .env.secrets에 MMEAN_LIVE_PASSWORD 등록 후 재시작",
                }), 403
            input_pw = str(payload.get("password", ""))
            if not hmac.compare_digest(stored_pw.encode(), input_pw.encode()):
                runtime.log.warning("실전 모드 전환 비밀번호 불일치 시도")
                return jsonify({"ok": False, "error": "비밀번호가 틀렸습니다"}), 401

        # LIVE 전환 게이트 2: EXECUTION_ENABLED=true 필수
        if mode == "LIVE" and not runtime.settings.get("EXECUTION_ENABLED", False):
            return jsonify({
                "ok": False,
                "error": "LIVE 모드는 EXECUTION_ENABLED=true 필요 (.env.public 확인)",
            }), 403

        with runtime.state_obj.engine_lock:
            old_mode = runtime.state.get("execution_mode", "OFF")
            runtime.state["execution_mode"] = mode
            if mode == "PAPER":
                runtime.settings["ORDER_ENV"] = "virtual"   # 모의계좌 강제
            elif mode == "LIVE":
                runtime.settings["ORDER_ENV"] = os.getenv("KIS_ORDER_ENV", "virtual").lower()

        runtime.log.warning("실행 모드 전환 | %s → %s", old_mode, mode)
        return jsonify({
            "ok": True,
            "execution_mode": mode,
            "previous": old_mode,
            "order_env": runtime.settings.get("ORDER_ENV", "virtual"),
        })

    @app.route("/prompts")
    def prompt_board():
        return render_template("prompt_board.html")

    @app.route("/api/prompts", methods=["GET"])
    def api_prompts_all():
        try:
            all_info = runtime.prompt_mgr.get_all_info()
            combined = {
                "strategy": runtime.prompt_mgr.get_combined_meta("strategy"),
                "opportunity": runtime.prompt_mgr.get_combined_meta("opportunity"),
            }
            reload_mode = os.getenv("PROMPT_RELOAD_MODE", "immediate")
            return jsonify({
                "ok": True,
                "prompts": all_info,
                "combined": combined,
                "reload_mode": reload_mode,
            })
        except Exception as e:
            runtime.log.error("api_prompts_all 오류: %s", e)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/prompts/<name>", methods=["GET"])
    def api_prompt_get(name: str):
        if name not in runtime.prompt_valid_names:
            return jsonify({"error": f"유효하지 않은 name: {name}"}), 400
        try:
            info = runtime.prompt_mgr.get_all_info().get(name, {})
            return jsonify({"ok": True, **info})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/prompts/<name>", methods=["POST"])
    def api_prompt_save(name: str):
        if name not in runtime.prompt_valid_names:
            return jsonify({"error": f"유효하지 않은 name: {name}"}), 400
        body = request.get_json(silent=True) or {}
        content = str(body.get("content", "")).strip()
        note = str(body.get("note", "")).strip()
        if not content:
            return jsonify({"error": "content 없음"}), 400
        try:
            result = runtime.prompt_mgr.save_prompt(name, content, note=note, source="board")
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
