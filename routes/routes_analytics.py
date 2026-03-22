# MMEAN/routes_analytics.py
from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime

import os
import requests
from flask import jsonify, render_template, request, Response, stream_with_context


def register_analytics_routes(app, db_path: str, log) -> None:
    def _analytics_conn():
        c = sqlite3.connect(db_path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c

    # ── 데이터 소스 뷰 ─────────────────────────────────────────────
    # source="sim"  → sim_trades 컬럼을 trades 스키마로 정규화한 임시 뷰 _src
    # source="live" → trades 테이블을 그대로 _src 로 매핑
    # 기본값: "sim" (시뮬레이션 단계)
    _SIM_VIEW_SQL = """
        CREATE TEMPORARY VIEW IF NOT EXISTS _src AS
        SELECT id,
               entry_time                              AS open_ts,
               exit_time                               AS close_ts,
               direction, entry_price, exit_price,
               ROUND(pnl / 25000.0, 2)                AS pnl_ticks,
               exit_reason, entry_bias, exit_bias,
               0.0  AS entry_long_score,
               0.0  AS entry_short_score,
               0.0  AS entry_confidence,
               0.0  AS entry_basis,
               0.0  AS entry_slope,
               0.0  AS entry_vol_burst,
               ''   AS entry_reason,
               ''   AS exit_reason_detail,
               ''   AS config_hash,
               0.0  AS exit_long_score,
               0.0  AS exit_short_score,
               0.0  AS exit_confidence,
               COALESCE(max_favorable_pt,      0)      AS max_favorable_pt,
               COALESCE(max_adverse_excursion, 0)      AS max_adverse_excursion,
               COALESCE(hold_ticks,            0)      AS hold_ticks,
               COALESCE(cum_equity,            0)      AS cum_equity,
               COALESCE(entry_llm_call_id,     0)      AS entry_llm_call_id,
               COALESCE(entry_llm_score,      -1)      AS entry_llm_score,
               COALESCE(entry_llm_direction, 'UNKNOWN') AS entry_llm_direction,
               0.0  AS entry_trend_weight_long,
               0.0  AS entry_trend_weight_short,
               0.0  AS entry_trend_long_score,
               0.0  AS entry_trend_short_score
        FROM sim_trades
    """
    _LIVE_VIEW_SQL = "CREATE TEMPORARY VIEW IF NOT EXISTS _src AS SELECT * FROM trades"

    def _setup_source(conn: sqlite3.Connection, source: str) -> None:
        """요청별 임시 뷰 _src 생성. 이후 모든 쿼리는 FROM _src 사용."""
        sql = _SIM_VIEW_SQL if source == "sim" else _LIVE_VIEW_SQL
        conn.execute(sql)

    @app.route("/analytics")
    def analytics_page():
        return render_template("analytics.html")
    
    @app.route("/analytics/ai-report")
    def analytics_ai_report_view():
        date = request.args.get("date", "")
        session = request.args.get("session", "all")
        return render_template("ai_report.html", date=date, session=session)  
    
    @app.route("/api/analytics/dates")
    def analytics_dates():
        try:
            source = request.args.get("source", "sim")
            with closing(_analytics_conn()) as conn:
                _setup_source(conn, source)
                rows = conn.execute("""
                    SELECT substr(open_ts,1,10) AS date,
                           COUNT(*) AS cnt,
                           SUM(CASE WHEN CAST(substr(open_ts,12,2) AS INT)>=18
                                     OR CAST(substr(open_ts,12,2) AS INT)<6
                                    THEN 1 ELSE 0 END) AS night_cnt
                    FROM _src
                    GROUP BY substr(open_ts,1,10)
                    ORDER BY date DESC
                    LIMIT 60
                """).fetchall()
            return jsonify([dict(r) for r in rows])
        except Exception as e:
            log.error("analytics_dates 오류: %s", e)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/analytics/trades")
    def analytics_trades():
        try:
            source  = request.args.get("source", "sim")
            date    = request.args.get("date", "")
            session = request.args.get("session", "all")
            limit   = int(request.args.get("limit", 500))
            wheres, params = [], []

            if date:
                wheres.append("substr(open_ts,1,10)=?")
                params.append(date)

            if session == "day":
                wheres.append("CAST(substr(open_ts,12,2) AS INT)>=6")
                wheres.append("CAST(substr(open_ts,12,2) AS INT)<18")
            elif session == "night":
                wheres.append("(CAST(substr(open_ts,12,2) AS INT)>=18 OR CAST(substr(open_ts,12,2) AS INT)<6)")

            w = ("WHERE " + " AND ".join(wheres)) if wheres else ""

            with closing(_analytics_conn()) as conn:
                _setup_source(conn, source)
                rows = conn.execute(f"""
                    SELECT id, open_ts, close_ts, direction, entry_price, exit_price,
                           pnl_ticks, exit_reason, entry_bias, entry_long_score, entry_short_score,
                           entry_confidence, entry_basis, entry_slope, entry_vol_burst,
                           entry_reason, exit_bias, exit_long_score, exit_short_score,
                           exit_confidence, exit_reason_detail, config_hash,
                           max_favorable_pt, max_adverse_excursion,
                           hold_ticks, cum_equity,
                           entry_llm_call_id, entry_llm_score, entry_llm_direction,
                           entry_trend_weight_long, entry_trend_weight_short,
                           entry_trend_long_score,  entry_trend_short_score
                    FROM _src {w}
                    ORDER BY open_ts DESC
                    LIMIT ?
                """, params + [limit]).fetchall()

            return jsonify([dict(r) for r in rows])
        except Exception as e:
            log.error("analytics_trades 오류: %s", e)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/analytics/summary")
    def analytics_summary():
        try:
            source  = request.args.get("source", "sim")
            date    = request.args.get("date", "")
            session = request.args.get("session", "all")
            wheres, params = [], []

            if date:
                wheres.append("substr(open_ts,1,10)=?")
                params.append(date)

            if session == "day":
                wheres.append("CAST(substr(open_ts,12,2) AS INT)>=6")
                wheres.append("CAST(substr(open_ts,12,2) AS INT)<18")
            elif session == "night":
                wheres.append("(CAST(substr(open_ts,12,2) AS INT)>=18 OR CAST(substr(open_ts,12,2) AS INT)<6)")

            w = ("WHERE " + " AND ".join(wheres)) if wheres else ""

            with closing(_analytics_conn()) as conn:
                _setup_source(conn, source)

                r = conn.execute(f"""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN pnl_ticks>0 THEN 1 ELSE 0 END) AS wins,
                           SUM(CASE WHEN pnl_ticks<=0 THEN 1 ELSE 0 END) AS losses,
                           SUM(COALESCE(pnl_ticks,0)) AS total_ticks,
                           SUM(COALESCE(pnl_ticks,0))*25000 AS total_pnl,
                           AVG(COALESCE(pnl_ticks,0)) AS avg_ticks,
                           MAX(COALESCE(pnl_ticks,0)) AS max_win_ticks,
                           MIN(COALESCE(pnl_ticks,0)) AS max_loss_ticks,
                           SUM(CASE WHEN exit_reason='TAKE_PROFIT'    THEN 1 ELSE 0 END) AS tp_cnt,
                           SUM(CASE WHEN exit_reason='STOP_LOSS'      THEN 1 ELSE 0 END) AS sl_cnt,
                           SUM(CASE WHEN exit_reason='TRAILING_STOP'  THEN 1 ELSE 0 END) AS ts_cnt,
                           SUM(CASE WHEN exit_reason='BIAS_REVERSAL'  THEN 1 ELSE 0 END) AS br_cnt
                    FROM _src {w}
                """, params).fetchone()

                hourly = conn.execute(f"""
                    SELECT CAST(substr(open_ts,12,2) AS INT) AS hour,
                           COUNT(*) AS cnt,
                           SUM(COALESCE(pnl_ticks,0))*25000 AS pnl
                    FROM _src {w}
                    GROUP BY hour
                    ORDER BY hour
                """, params).fetchall()

                by_dir = conn.execute(f"""
                    SELECT direction, COUNT(*) AS cnt,
                           SUM(COALESCE(pnl_ticks,0))*25000 AS pnl,
                           AVG(COALESCE(pnl_ticks,0)) AS avg_ticks
                    FROM _src {w}
                    GROUP BY direction
                """, params).fetchall()

                by_exit = conn.execute(f"""
                    SELECT exit_reason, COUNT(*) AS cnt,
                           SUM(COALESCE(pnl_ticks,0))*25000 AS pnl
                    FROM _src {w}
                    GROUP BY exit_reason
                """, params).fetchall()

                eq_rows = conn.execute(f"""
                    SELECT open_ts, COALESCE(pnl_ticks,0)*25000 AS pnl_won
                    FROM _src {w}
                    ORDER BY open_ts
                """, params).fetchall()

            cum = 0
            equity = []
            for row in eq_rows:
                cum += (row["pnl_won"] or 0)
                equity.append({"ts": row["open_ts"], "cum": cum})

            summary = dict(r) if r and r["total"] else {
                "total": 0,
                "wins": 0,
                "losses": 0,
                "total_ticks": 0,
                "total_pnl": 0,
                "avg_ticks": 0,
                "max_win_ticks": 0,
                "max_loss_ticks": 0,
                "tp_cnt": 0,
                "sl_cnt": 0,
                "ts_cnt": 0,
                "br_cnt": 0,
            }

            return jsonify({
                "summary": summary,
                "hourly": [dict(h) for h in hourly],
                "by_dir": [dict(d) for d in by_dir],
                "by_exit": [dict(e) for e in by_exit],
                "equity": equity,
            })
        except Exception as e:
            log.error("analytics_summary 오류: %s", e)
            return jsonify({
                "error": str(e),
                "summary": {
                    "total": 0,
                    "wins": 0,
                    "losses": 0,
                    "total_ticks": 0,
                    "total_pnl": 0,
                    "avg_ticks": 0,
                    "max_win_ticks": 0,
                    "max_loss_ticks": 0,
                    "tp_cnt": 0,
                    "sl_cnt": 0,
                    "ts_cnt": 0,
                    "br_cnt": 0,
                },
                "hourly": [],
                "by_dir": [],
                "by_exit": [],
                "equity": [],
            }), 500

    @app.route("/api/analytics/ticks")
    def analytics_ticks():
        try:
            date = request.args.get("date", "")
            session = request.args.get("session", "all")
            limit = int(request.args.get("limit", 2000))
            wheres, params = [], []

            if date:
                wheres.append("substr(ts,1,10)=?")
                params.append(date)

            if session == "day":
                wheres.append("CAST(substr(ts,12,2) AS INT)>=6")
                wheres.append("CAST(substr(ts,12,2) AS INT)<18")
            elif session == "night":
                wheres.append("(CAST(substr(ts,12,2) AS INT)>=18 OR CAST(substr(ts,12,2) AS INT)<6)")

            w = ("WHERE " + " AND ".join(wheres)) if wheres else ""

            with closing(_analytics_conn()) as conn:
                rows = conn.execute(f"""
                    SELECT ts, bias, entry_signal, confidence, futures_price, basis,
                           basis_ema, basis_slope, volume_burst, foreign_buy, long_score, short_score
                    FROM regime_ticks {w}
                    ORDER BY ts DESC
                    LIMIT ?
                """, params + [limit]).fetchall()

            return jsonify([dict(r) for r in reversed(list(rows))])
        except Exception as e:
            log.error("analytics_ticks 오류: %s", e)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/analytics/regimes")
    def analytics_regimes():
        try:
            date = request.args.get("date", "")
            limit = int(request.args.get("limit", 200))
            w = "WHERE substr(start_ts,1,10)=?" if date else ""
            p = [date] if date else []

            with closing(_analytics_conn()) as conn:
                rows = conn.execute(
                    f"SELECT * FROM regime_events {w} ORDER BY start_ts DESC LIMIT ?",
                    p + [limit],
                ).fetchall()

            return jsonify([dict(r) for r in rows])
        except Exception as e:
            log.error("analytics_regimes 오류: %s", e)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/analytics/config_history")
    def analytics_config_history():
        try:
            with closing(_analytics_conn()) as conn:
                rows = conn.execute(
                    "SELECT ts,hash,action,changed FROM config_snapshots ORDER BY ts DESC LIMIT 50"
                ).fetchall()

            return jsonify([dict(r) for r in rows])
        except Exception as e:
            log.error("analytics_config_history 오류: %s", e)
            return jsonify({"error": str(e)}), 500

    @app.route("/api/analytics/report", methods=["POST"])
    def analytics_report():
        """
        Gemini Flash 기반 AI 리포트.
        프론트 prompt -> 서버 -> Gemini API(비스트리밍) -> SSE 청크 응답
        """
        import json as _json

        try:
            body = request.get_json(silent=True) or {}
            prompt = str(body.get("prompt", "")).strip()
            if not prompt:
                return jsonify({"error": "prompt 없음"}), 400

            gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
            if not gemini_key:
                return jsonify({"error": "GEMINI_API_KEY 환경변수 없음"}), 500

            model = os.getenv("REPORT_GEMINI_MODEL", "gemini-3-flash-preview")
            api_url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model}:generateContent?key={gemini_key}"
            )

            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": 1200,
                    "temperature": 0.4,
                },
            }

            def generate():
                try:
                    resp = requests.post(api_url, json=payload, timeout=30)

                    if resp.status_code != 200:
                        yield f"data: {_json.dumps({'error': resp.text[:300]}, ensure_ascii=False)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

                    data = resp.json()
                    candidates = data.get("candidates") or []
                    if not candidates:
                        yield f"data: {_json.dumps({'error': 'Gemini 응답 없음'}, ensure_ascii=False)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

                    parts = candidates[0].get("content", {}).get("parts") or []
                    text = "".join(p.get("text", "") for p in parts).strip()

                    if not text:
                        yield f"data: {_json.dumps({'error': '빈 응답'}, ensure_ascii=False)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

                    chunk_size = 500
                    for i in range(0, len(text), chunk_size):
                        chunk = text[i:i + chunk_size]
                        yield f"data: {_json.dumps({'text': chunk}, ensure_ascii=False)}\n\n"

                except Exception as gen_err:
                    yield f"data: {_json.dumps({'error': str(gen_err)}, ensure_ascii=False)}\n\n"
                finally:
                    yield "data: [DONE]\n\n"

            return Response(
                stream_with_context(generate()),
                mimetype="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )
 

        except Exception as e:
            log.error("analytics_report 오류: %s", e)
            return jsonify({"error": str(e)}), 500        