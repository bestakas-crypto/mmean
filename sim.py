
# sim.py
from __future__ import annotations

import path_setup  # noqa: F401 — 서브디렉토리 sys.path 등록

import argparse
import os
import shlex
import sqlite3
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

DAY_SCRIPT        = BASE_DIR / "sim_opt" / "day_sim.py"
NIGHT_SCRIPT      = BASE_DIR / "sim_opt" / "night_sim.py"
AUTO_NIGHT_SCRIPT = BASE_DIR / "scripts" / "night_auto_run.py"
AUTO_DAY_SCRIPT   = BASE_DIR / "scripts" / "day_auto_run.py"

DEFAULT_SOURCE_DB  = BASE_DIR / "storage" / "mmean.db"
DEFAULT_SIM_DB     = BASE_DIR / "storage" / "sim.db"
DEFAULT_DAY_JSON   = BASE_DIR / "workspace" / "day_levels.json"
DEFAULT_NIGHT_JSON = BASE_DIR / "workspace" / "night_levels.json"

# ── 자동 루프 기본 설정 (세션 중 변경 가능) ───────────────────────────
_DEFAULT_AUTO_CFG = {
    "loops":          0,      # 0=무한
    "random":       300,
    "bayes":        200,
    "startup":       40,
    "train":         15,
    "valid":          5,
    "cluster":        5,
    "top":           10,
    "seed":          42,
    "sleep":          0,      # 루프 간 대기 초
    "skip_random":   False,
    "skip_bayes":    False,
    "skip_validate": False,
    "skip_adopt":    False,
    "skip_stress":   False,
    "skip_cluster":  False,
    # 날짜 직접 지정 (None = n일 기반 자동 탐지)
    "train_dates":   None,    # List[str] or None
    "valid_dates":   None,    # List[str] or None
}

# 야간 / 정규장 설정은 각각 독립 복사본
_night_auto_cfg: dict = {**_DEFAULT_AUTO_CFG, "prefix": "auto"}
_day_auto_cfg:   dict = {**_DEFAULT_AUTO_CFG, "prefix": "day_auto"}


# ─────────────────────────────────────────────────────────────────────
# 기본 유틸
# ─────────────────────────────────────────────────────────────────────

def clear_screen() -> None:
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


def today_str() -> str:
    return date.today().isoformat()


def yesterday_str() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def last_night_date() -> str:
    """
    현재 시각 기준 '마지막 야간장' 시작 날짜 반환.

    야간장: D 18:00 ~ D+1 06:00  (레이블 = D)

        현재 시각 >= 18:00  →  오늘 야간장이 막 시작됨      → 오늘(D)
        06:00 <= 시각 < 18:00 →  어제 야간장이 완료됨        → 어제(D-1)
        시각 < 06:00          →  어제 야간장이 진행 중       → 어제(D-1)
    """
    h = datetime.now().hour
    if h >= 18:
        return date.today().isoformat()
    return (date.today() - timedelta(days=1)).isoformat()


def last_day_date() -> str:
    """
    현재 시각 기준 '마지막 정규장' 날짜 반환.

    정규장: 08:45 ~ 15:35  (당일)

        시각 >= 15:35  →  오늘 정규장이 완료됨              → 오늘
        시각 < 15:35   →  오늘 정규장 진행 중 or 미시작     → 어제
    """
    now = datetime.now()
    if now.hour > 15 or (now.hour == 15 and now.minute >= 35):
        return date.today().isoformat()
    return (date.today() - timedelta(days=1)).isoformat()


def recent_days(n: int = 3, end_offset_days: int = 1) -> list[str]:
    end   = date.today() - timedelta(days=end_offset_days)
    start = end - timedelta(days=n - 1)
    return [(start + timedelta(days=i)).isoformat() for i in range(n)]


def ask(prompt: str, default: str | None = None) -> str:
    if default is not None:
        raw = input(f"{prompt} [{default}]: ").strip()
        return raw or default
    return input(f"{prompt}: ").strip()


def ask_yes_no(prompt: str, default: bool = False) -> bool:
    tag = "Y/n" if default else "y/N"
    raw = input(f"{prompt} ({tag}): ").strip().lower()
    if not raw:
        return default
    return raw in {"y", "yes", "1", "true"}


def ask_dates(default_dates: list[str]) -> list[str]:
    default_text = " ".join(default_dates)
    raw = input(f"  날짜들 (공백구분, 엔터=최근 3일) [{default_text}]: ").strip()
    if not raw:
        return default_dates
    return sorted(set(raw.split()))


def fetch_trading_days_in_range(start: str, end: str, mode: str = "day") -> list[str]:
    """mmean.db에서 start~end 기간의 실제 거래일 목록을 조회."""
    db = str(DEFAULT_SOURCE_DB)
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
        if mode == "night":
            rows = conn.execute("""
                SELECT DISTINCT date(ts)
                FROM regime_ticks
                WHERE date(ts) BETWEEN ? AND ?
                  AND time(ts) >= '18:00:00'
                ORDER BY date(ts)
            """, (start, end)).fetchall()
        else:
            rows = conn.execute("""
                SELECT DISTINCT date(ts)
                FROM regime_ticks
                WHERE date(ts) BETWEEN ? AND ?
                  AND time(ts) >= '08:45:00' AND time(ts) < '15:35:00'
                ORDER BY date(ts)
            """, (start, end)).fetchall()
        conn.close()
        return [r[0] for r in rows if r[0]]
    except Exception as e:
        print(f"  [오류] DB 조회 실패: {e}")
        return []


def fetch_date_pattern(date: str, mode: str = "day") -> str | None:
    """mmean.db session_patterns에서 해당 날짜의 pattern_type 조회."""
    db = str(DEFAULT_SOURCE_DB)
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
        row = conn.execute("""
            SELECT pattern_type FROM session_patterns
            WHERE date = ? AND session_mode = ?
        """, (date, mode)).fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def fetch_same_pattern_dates(pattern: str, dates: list[str], mode: str = "day") -> list[str]:
    """주어진 날짜 목록 중 같은 pattern_type인 날짜만 반환."""
    db = str(DEFAULT_SOURCE_DB)
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
        placeholders = ",".join("?" * len(dates))
        rows = conn.execute(f"""
            SELECT date FROM session_patterns
            WHERE date IN ({placeholders})
              AND session_mode = ?
              AND pattern_type = ?
            ORDER BY date
        """, (*dates, mode, pattern)).fetchall()
        conn.close()
        return [r[0] for r in rows]
    except Exception:
        return []


def fetch_all_trained_dates(train_date: str, mode: str = "day") -> set[str]:
    """sim.db에서 해당 훈련 날짜 study의 모든 source_dates를 조회."""
    sim_db = str(DEFAULT_SIM_DB)
    result: set[str] = set()
    try:
        conn = sqlite3.connect(f"file:{sim_db}?mode=ro", uri=True, timeout=10)
        # study_name 패턴: day_YYYY-MM-DD 또는 night_YYYY-MM-DD
        prefix = "night_" if mode == "night" else "day_"
        study  = f"{prefix}{train_date}"
        rows   = conn.execute("""
            SELECT DISTINCT source_dates
            FROM sim_runs
            WHERE study_name = ? AND status = 'done'
        """, (study,)).fetchall()
        conn.close()
        import json
        for r in rows:
            try:
                dates = json.loads(r[0])
                if isinstance(dates, list):
                    result.update(dates)
            except Exception:
                pass
    except Exception:
        pass
    return result


def fetch_null_flow_dates(dates: list[str], mode: str = "day") -> set[str]:
    """
    flow_score가 전부 NULL인 날짜 반환 — ForeignFlowEngine 미실행 = 시뮬 불가.
    틱은 있지만 flow_score가 하나도 없으면 지표 미수집으로 판단.
    """
    db = str(DEFAULT_SOURCE_DB)
    result: set[str] = set()
    if not dates:
        return result
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
        if mode == "night":
            time_filter = "time(ts) >= '18:00:00'"
        else:
            time_filter = "time(ts) >= '08:45:00' AND time(ts) < '15:35:00'"
        for d in dates:
            row = conn.execute(f"""
                SELECT COUNT(*), COUNT(flow_score)
                FROM regime_ticks
                WHERE date(ts) = ?
                  AND {time_filter}
            """, (d,)).fetchone()
            total, non_null = row if row else (0, 0)
            if total > 0 and non_null == 0:
                result.add(d)
        conn.close()
    except Exception:
        pass
    return result


def ask_range_valid_dates(train_dates: list[str], mode: str = "day") -> list[str]:
    """
    시작~끝 날짜 범위를 입력받아 실제 거래일을 조회하고
    훈련 날짜(train_dates) + sim.db 기존 훈련일을 제외한 검증 날짜 목록을 반환.
    훈련 날짜의 레짐(pattern_type)이 session_patterns에 있으면 같은 레짐 날짜만 필터링.
    flow_score가 전부 NULL인 날짜는 자동 제외 (지표 미수집).
    """
    print("\n  ── 검증 날짜 범위 입력 ──────────────────────────────")

    # 훈련 날짜 레짐 조회
    train_d  = train_dates[0] if train_dates else None
    pattern  = fetch_date_pattern(train_d, mode) if train_d else None
    if pattern:
        print(f"  훈련 날짜 레짐: {train_d} → {pattern}")
        print(f"  ※ 범위 내에서 같은 레짐({pattern})의 날짜만 검증 후보로 사용합니다.")
    else:
        print(f"  훈련 날짜 레짐: {train_d} → 미분류 (day_pattern.py 실행 권장)")
        print(f"  ※ 레짐 필터 없이 전체 날짜를 검증 후보로 사용합니다.")

    # sim.db에서 기존 훈련일 자동 수집
    all_trained: set[str] = set(train_dates)
    for td in train_dates:
        all_trained.update(fetch_all_trained_dates(td, mode))
    print(f"  훈련 날짜 (자동 제외): {', '.join(sorted(all_trained))}")

    start = input("  범위 시작일 (YYYY-MM-DD): ").strip()
    end   = input("  범위 종료일 (YYYY-MM-DD): ").strip()

    if not start or not end:
        print("  날짜 입력이 없습니다.")
        return []

    all_days = fetch_trading_days_in_range(start, end, mode)
    if not all_days:
        print(f"  [{start} ~ {end}] 범위에 거래일 데이터가 없습니다.")
        return []

    # flow_score NULL 날짜 자동 제외 (ForeignFlowEngine 미실행 = 시뮬 불가)
    null_dates = fetch_null_flow_dates(all_days, mode)
    if null_dates:
        print(f"  ※ flow_score 미수집 날짜 자동 제외: {', '.join(sorted(null_dates))}")
        all_days = [d for d in all_days if d not in null_dates]
    if not all_days:
        print(f"  [{start} ~ {end}] flow_score 유효 거래일이 없습니다.")
        return []

    # 레짐 필터 적용
    if pattern:
        same_regime = set(fetch_same_pattern_dates(pattern, all_days, mode))
        filtered    = [d for d in all_days if d in same_regime]
        other       = [d for d in all_days if d not in same_regime]
        print(f"\n  범위 내 거래일 ({len(all_days)}일): {', '.join(all_days)}")
        print(f"  같은 레짐({pattern}) ({len(filtered)}일): {', '.join(filtered) if filtered else '없음'}")
        if other:
            # 다른 레짐 날짜의 패턴 표시
            db = str(DEFAULT_SOURCE_DB)
            try:
                conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
                ph   = ",".join("?" * len(other))
                pats = conn.execute(
                    f"SELECT date, pattern_type FROM session_patterns "
                    f"WHERE date IN ({ph}) AND session_mode=? ORDER BY date",
                    (*other, mode)
                ).fetchall()
                conn.close()
                pat_map = {r[0]: r[1] for r in pats}
            except Exception:
                pat_map = {}
            other_info = ", ".join(
                f"{d}({pat_map.get(d,'?')})" for d in other
            )
            print(f"  다른 레짐 제외 ({len(other)}일): {other_info}")
        candidate = filtered
    else:
        candidate = all_days
        print(f"\n  범위 내 거래일 ({len(all_days)}일): {', '.join(all_days)}")

    # 훈련일 제외
    valid_days = [d for d in candidate if d not in all_trained]
    print(f"  훈련일 제외 후 ({len(valid_days)}일): {', '.join(valid_days) if valid_days else '없음'}")

    if not valid_days:
        print("  검증할 날짜가 없습니다.")
        return []

    confirm = input("\n  위 날짜로 검증을 진행하시겠습니까? (Y/n): ").strip().lower()
    if confirm == "n":
        return []

    return valid_days


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MMEAN day/night simulation launcher")
    p.add_argument("--dry", action="store_true", help="실제 실행 없이 명령어만 출력")
    return p.parse_args()


def cmd_text(args: list[str]) -> str:
    return " ".join(shlex.quote(x) for x in args)


def run_command(args: list[str], dry_mode: bool = False) -> int:
    preview = cmd_text(args)
    print("\n" + "─" * 72)
    print(f"  실행 명령어: {preview}")
    print("─" * 72)
    if dry_mode:
        print("  [DRY MODE] 실제 실행은 하지 않았습니다.")
        print("─" * 72)
        return 0
    proc = subprocess.run(args)
    print("─" * 72)
    print(f"  종료 코드: {proc.returncode}")
    print("─" * 72)
    return proc.returncode


def pause(msg: str = "\n엔터를 누르면 런처로 돌아갑니다...") -> None:
    try:
        input(msg)
    except KeyboardInterrupt:
        print("\n메뉴로 돌아갑니다...")


def get_script_and_json(mode: str) -> tuple[Path, Path]:
    if mode == "day":
        return DAY_SCRIPT, DEFAULT_DAY_JSON
    return NIGHT_SCRIPT, DEFAULT_NIGHT_JSON


def build_base(mode: str) -> list[str]:
    script_path, json_path = get_script_and_json(mode)
    base = [
        sys.executable,
        str(script_path),
        "--source-db", str(DEFAULT_SOURCE_DB),
        "--sim-db",    str(DEFAULT_SIM_DB),
    ]
    if mode == "day":
        base += ["--day-json",   str(json_path)]
    else:
        base += ["--night-json", str(json_path)]
    return base


# ─────────────────────────────────────────────────────────────────────
# 새 터미널 창 런치
# ─────────────────────────────────────────────────────────────────────

def launch_in_new_window(cmd: list[str], title: str, dry_mode: bool) -> None:
    """새 터미널 창에서 cmd 실행 (창은 프로세스 종료 후에도 유지)."""
    if dry_mode:
        preview = subprocess.list2cmdline(cmd) if os.name == "nt" else " ".join(shlex.quote(c) for c in cmd)
        print(f"\n[DRY-RUN] 새 창에서 실행할 명령어:\n  {preview}")
        return

    if os.name == "nt":
        # Windows: subprocess.list2cmdline → 이중따옴표 기반 CMD 호환 이스케이프
        # (shlex.quote 는 홑따옴표를 생성해 CMD 에서 인식 불가)
        cmd_str = subprocess.list2cmdline(cmd)
        subprocess.Popen(
            f'start "{title}" cmd /k {cmd_str}',
            shell=True,
        )
        print(f'\n  새 창 실행 시작 — 창 제목: "{title}"')
    else:
        # Linux / macOS: xterm 시도 → 없으면 백그라운드
        cmd_str = " ".join(shlex.quote(c) for c in cmd)
        try:
            subprocess.Popen(
                ["xterm", "-title", title, "-e",
                 "bash", "-c", f"{cmd_str}; read -p 'Press Enter...'"]
            )
        except FileNotFoundError:
            subprocess.Popen(cmd)
            print(f"  백그라운드 실행 시작: {cmd_str}")


# ─────────────────────────────────────────────────────────────────────
# 자동 루프 설정 화면
# ─────────────────────────────────────────────────────────────────────

_AUTO_FIELDS = [
    # (번호, 레이블,               키,           타입)
    ( 1, "랜덤 trials        ", "random",        "int"),
    ( 2, "Bayes trials       ", "bayes",         "int"),
    ( 3, "Bayes startup      ", "startup",       "int"),
    ( 4, "train 일수 (자동)  ", "train",         "int"),
    ( 5, "valid 일수 (자동)  ", "valid",         "int"),
    ( 6, "반복 횟수  (0=무한)", "loops",         "int"),
    ( 7, "루프 간 대기 (초)  ", "sleep",         "int"),
    ( 8, "study prefix       ", "prefix",        "str"),
    ( 9, "cluster K          ", "cluster",       "int"),
    (10, "validation top N   ", "top",           "int"),
    (11, "seed               ", "seed",          "int"),
    (12, "skip: random       ", "skip_random",   "bool"),
    (13, "skip: bayes        ", "skip_bayes",    "bool"),
    (14, "skip: validate     ", "skip_validate", "bool"),
    (15, "skip: adopt        ", "skip_adopt",    "bool"),
    (16, "skip: stress       ", "skip_stress",   "bool"),
    (17, "skip: cluster      ", "skip_cluster",  "bool"),
    (18, "train 날짜 직접 지정", "train_dates",  "dates"),
    (19, "valid 날짜 직접 지정", "valid_dates",  "dates"),
]


def _val_str(cfg: dict, key: str, ftype: str) -> str:
    v = cfg.get(key)
    if ftype == "bool":
        return "ON " if v else "off"
    if ftype == "dates":
        if not v:
            return "[자동]"
        return f"{len(v)}일 지정  ({v[0]} ~ {v[-1]})"
    if key == "loops":
        return f"{v}  (0=무한)" if v == 0 else str(v)
    return str(v)


def _show_date_grid(label: str, dates: list, cols: int = 5, mode: str = "") -> None:
    print(f"\n  {label}:")
    if not dates:
        print("    (없음)")
        return
    if mode == "night":
        # 야간장: 각 날짜 옆에 세션 시간 표시
        for d in dates:
            try:
                next_d = (datetime.strptime(d, "%Y-%m-%d")
                          + timedelta(days=1)).strftime("%m-%d")
                print(f"    {d}  (18:00 ~ {next_d} 06:00)")
            except Exception:
                print(f"    {d}")
    else:
        for i, d in enumerate(dates):
            end = "\n" if (i + 1) % cols == 0 or i == len(dates) - 1 else "  "
            print(f"    {d}", end=end)


def _preview_dates(mode: str, cfg: dict) -> None:
    """DB 쿼리 또는 직접 지정 기반으로 train/valid 날짜를 시각화."""
    # ── 직접 지정된 경우 ────────────────────────────────────────
    if cfg.get("train_dates") or cfg.get("valid_dates"):
        train_d = sorted(cfg.get("train_dates") or [])
        valid_d = sorted(cfg.get("valid_dates") or [])
        sess    = "야간장" if mode == "night" else "정규장"
        print(f"\n  {'─'*58}")
        print(f"  날짜 미리보기  (직접 지정 | {sess})")
        if mode == "night":
            print(f"  ※ 야간 세션: 각 날짜 D 18:00 ~ D+1 06:00 구간이 적용됩니다")
        print(f"  {'─'*58}")
        _show_date_grid(f"TRAIN ({len(train_d)}일)", train_d, mode=mode)
        _show_date_grid(f"VALID ({len(valid_d)}일)", valid_d, mode=mode)
        print(f"\n  {'─'*58}")
        input("\n  엔터로 돌아가기...")
        return

    # ── 자동 탐지 ────────────────────────────────────────────────
    n_total = cfg["train"] + cfg["valid"]
    src     = str(DEFAULT_SOURCE_DB)
    try:
        conn = sqlite3.connect(f"file:{src}?mode=ro", uri=True, timeout=10)
        if mode == "night":
            rows = conn.execute("""
                SELECT DISTINCT date(ts) FROM regime_ticks
                WHERE  time(ts) >= '18:00:00'
                ORDER  BY date(ts) DESC LIMIT ?
            """, (n_total,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT DISTINCT date(ts) FROM regime_ticks
                WHERE  time(ts) >= '08:45:00' AND time(ts) < '15:35:00'
                ORDER  BY date(ts) DESC LIMIT ?
            """, (n_total,)).fetchall()
        conn.close()
        all_dates = sorted(r[0] for r in rows if r[0])
    except Exception as exc:
        print(f"\n  DB 조회 오류: {exc}")
        input("  엔터로 돌아가기...")
        return

    n_valid = cfg["valid"]
    if len(all_dates) > n_valid:
        train_d = all_dates[:-n_valid]
        valid_d = all_dates[-n_valid:]
    else:
        train_d, valid_d = all_dates, []

    sess = "야간장" if mode == "night" else "정규장"
    print(f"\n  {'─'*58}")
    print(f"  날짜 미리보기  ({sess} | 자동 탐지 | 최근 {n_total}일 기준)")
    if mode == "night":
        print(f"  ※ 야간 세션: 각 날짜 D 18:00 ~ D+1 06:00 구간이 적용됩니다")
    print(f"  DB: {src}")
    print(f"  {'─'*58}")
    _show_date_grid(f"TRAIN ({len(train_d)}일)", train_d, mode=mode)
    _show_date_grid(f"VALID ({len(valid_d)}일)", valid_d, mode=mode)
    found = len(all_dates)
    if found < n_total:
        print(f"\n  ⚠  요청 {n_total}일 중 {found}일만 존재 — 날짜 부족 가능")
    print(f"\n  {'─'*58}")
    input("\n  엔터로 돌아가기...")


def _print_auto_settings(mode: str, cfg: dict) -> None:
    mode_label = "야간장 night" if mode == "night" else "정규장 day"
    script     = AUTO_NIGHT_SCRIPT if mode == "night" else AUTO_DAY_SCRIPT
    exists     = "존재" if script.exists() else "없음(!)"

    # 날짜 상태 요약
    td = cfg.get("train_dates")
    vd = cfg.get("valid_dates")
    date_mode = (f"직접지정 train={len(td)}일 / valid={len(vd or [])}일"
                 if td else f"자동 탐지 train={cfg['train']}일 / valid={cfg['valid']}일")

    print("╔══════════════════════════════════════════════════════════╗")
    print(f"║  MMEAN 자동 루프 설정  |  {mode_label:<30}║")
    print(f"║  스크립트: {str(script.name):<46}║")
    print(f"║  파일:     {exists:<46}║")
    print(f"║  날짜:     {date_mode:<46}║")
    print("╠══════════════════════════════════════════════════════════╣")
    print("║  [ # ]  항목                    값                      ║")
    print("║  ──────────────────────────────────────────────────────  ║")
    for no, label, key, ftype in _AUTO_FIELDS:
        v = _val_str(cfg, key, ftype)
        # 4·5번은 직접 지정 시 회색 표시
        dim = ""
        if no in (4, 5) and (td or vd):
            dim = " (무시됨)"
        print(f"║  [{no:>2}]  {label}  {v}{dim:<20}║")
    print("╠══════════════════════════════════════════════════════════╣")
    print("║  번호→값 변경  [p]미리보기  [s]새창시작  [0]취소        ║")
    print("╚══════════════════════════════════════════════════════════╝")


def _edit_auto_setting(cfg: dict, no: int, mode: str = "night") -> None:
    """번호에 해당하는 설정값 대화식 변경 (변경 후 피드백 출력)."""
    hit = next(((n, l, k, t) for n, l, k, t in _AUTO_FIELDS if n == no), None)
    if hit is None:
        print("  없는 번호입니다.")
        time.sleep(0.8)
        return
    _, label, key, ftype = hit
    cur = cfg.get(key)
    try:
        if ftype == "bool":
            cfg[key] = not cur
            print(f"  ✓ {label.strip()}: {'ON' if cfg[key] else 'off'}")
        elif ftype == "int":
            raw = input(f"  {label.strip()} 새 값 [{cur}]: ").strip()
            if raw:
                new_val  = int(raw)
                cfg[key] = new_val
                print(f"  ✓ {label.strip()}: {cur} → {new_val}")
            else:
                print(f"  (변경 없음, 현재값: {cur})")
        elif ftype == "str":
            raw = input(f"  {label.strip()} 새 값 [{cur}]: ").strip()
            if raw:
                cfg[key] = raw
                print(f"  ✓ {label.strip()}: '{cur}' → '{raw}'")
            else:
                print(f"  (변경 없음, 현재값: {cur})")
        elif ftype == "dates":
            cur_str = " ".join(cur) if cur else "자동"
            if mode == "night":
                print(f"  야간 세션 날짜 입력 — D 18:00 ~ D+1 06:00 자동 적용")
            else:
                print(f"  날짜를 공백으로 구분해 입력 (엔터=자동으로 초기화)")
            print(f"  예) 2026-03-10 2026-03-11 2026-03-12")
            raw = input(f"  [{cur_str}]: ").strip()
            if raw:
                new_dates = sorted(set(raw.split()))
                cfg[key]  = new_dates
                if mode == "night":
                    print(f"  ✓ {label.strip()}: {len(new_dates)}일 지정")
                    for d in new_dates:
                        try:
                            next_d = (datetime.strptime(d, "%Y-%m-%d")
                                      + timedelta(days=1)).strftime("%m-%d")
                            print(f"      {d}  →  18:00 ~ {next_d} 06:00")
                        except Exception:
                            print(f"      {d}")
                else:
                    print(f"  ✓ {label.strip()}: {len(new_dates)}일 지정"
                          f"  ({new_dates[0]} ~ {new_dates[-1]})")
            else:
                cfg[key] = None
                print(f"  ✓ {label.strip()}: 자동 탐지로 초기화됨")
    except ValueError:
        print("  잘못된 값 — 변경하지 않습니다.")
    time.sleep(0.7)   # 피드백 메시지가 보이도록 잠깐 대기


def _build_auto_cmd(mode: str, cfg: dict) -> list[str]:
    """auto_cfg → 실행 명령어 리스트 생성."""
    script = AUTO_NIGHT_SCRIPT if mode == "night" else AUTO_DAY_SCRIPT
    json_path = DEFAULT_NIGHT_JSON if mode == "night" else DEFAULT_DAY_JSON

    cmd = [sys.executable, str(script)]
    cmd += ["--source-db", str(DEFAULT_SOURCE_DB)]
    cmd += ["--sim-db",    str(DEFAULT_SIM_DB)]
    if mode == "night":
        cmd += ["--night-json", str(DEFAULT_NIGHT_JSON)]
    else:
        cmd += ["--day-json",   str(DEFAULT_DAY_JSON)]

    cmd += ["--loops",   str(cfg["loops"])]
    cmd += ["--random",  str(cfg["random"])]
    cmd += ["--bayes",   str(cfg["bayes"])]
    cmd += ["--startup", str(cfg["startup"])]
    cmd += ["--cluster", str(cfg["cluster"])]
    cmd += ["--top",     str(cfg["top"])]
    cmd += ["--prefix",  str(cfg["prefix"])]
    cmd += ["--seed",    str(cfg["seed"])]
    if cfg["sleep"] > 0:
        cmd += ["--sleep", str(cfg["sleep"])]

    # 날짜 직접 지정 vs 자동 탐지
    if cfg.get("train_dates"):
        cmd += ["--train-dates"] + list(cfg["train_dates"])
    else:
        cmd += ["--train", str(cfg["train"])]

    if cfg.get("valid_dates"):
        cmd += ["--valid-dates"] + list(cfg["valid_dates"])
    else:
        cmd += ["--valid", str(cfg["valid"])]

    skip_map = [
        ("skip_random",   "--skip-random"),
        ("skip_bayes",    "--skip-bayes"),
        ("skip_validate", "--skip-validate"),
        ("skip_adopt",    "--skip-adopt"),
        ("skip_stress",   "--skip-stress"),
        ("skip_cluster",  "--skip-cluster"),
    ]
    for key, flag in skip_map:
        if cfg[key]:
            cmd.append(flag)

    return cmd


def run_auto_mode(mode: str, dry_mode: bool) -> None:
    """자동 루프 설정 화면 → 새 창에서 실행."""
    cfg = _night_auto_cfg if mode == "night" else _day_auto_cfg

    while True:
        try:
            clear_screen()
            _print_auto_settings(mode, cfg)
            raw = input("  선택: ").strip().lower()

            if raw == "0":
                return

            if raw == "p":
                clear_screen()
                _preview_dates(mode, cfg)
                continue

            if raw == "s":
                script = AUTO_NIGHT_SCRIPT if mode == "night" else AUTO_DAY_SCRIPT
                if not script.exists():
                    print(f"\n  오류: {script} 파일이 없습니다.")
                    pause()
                    return

                cmd   = _build_auto_cmd(mode, cfg)
                title = f"MMEAN Auto-Run [{mode}] prefix={cfg['prefix']}"
                print(f"\n  실행 명령어:\n  {cmd_text(cmd)}\n")
                if ask_yes_no("  위 명령어로 새 창 실행?", True):
                    launch_in_new_window(cmd, title, dry_mode)
                    pause("\n  자동 루프가 새 창에서 시작됐습니다. 엔터로 돌아가기...")
                return

            # 번호 입력 → 설정 변경
            try:
                no = int(raw)
                _edit_auto_setting(cfg, no, mode)
            except ValueError:
                print("  번호 / 'p' / 's' / '0' 을 입력하세요.")
                time.sleep(0.8)

        except KeyboardInterrupt:
            print("\n취소")
            return


# ─────────────────────────────────────────────────────────────────────
# 메인 메뉴 헤더
# ─────────────────────────────────────────────────────────────────────

def print_header(today: str, dry_mode: bool, mode: str,
                 def_date: str = "") -> None:
    dry_label  = "ON" if dry_mode else "OFF"
    mode_label = "정규장(day)" if mode == "day" else "야간장(night)"
    sess_label = (f"마지막 야간장  {def_date}" if mode == "night"
                  else f"마지막 정규장  {def_date}")
    print("╔══════════════════════════════════════════════════════╗")
    print("║        MMEAN 시뮬레이션 런처                        ║")
    print(f"║  오늘: {today:<42}║")
    print(f"║  모드: {mode_label:<42}║")
    print(f"║  기본: {sess_label:<42}║")
    print(f"║  DRY 모드: {dry_label:<38}║")
    print("╠══════════════════════════════════════════════════════╣")
    print("║  ──────────── 수동 실행 ───────────────────────────  ║")
    print("║  [1] 전체 프로파일 실행       (--all-profiles)       ║")
    print("║  [2] 단일 프로파일 실행       (--profile N)          ║")
    print("║  [3] 결과 보고서 출력         (--report-only)        ║")
    print("║  ─────────────────────────────────────────────────   ║")
    print("║  [4] 랜덤 탐색                (--random N)           ║")
    print("║  [5] Bayesian 탐색            (--bayes N)            ║")
    print("║  ─────────────────────────────────────────────────   ║")
    print("║  [6] Validation 실행          (날짜 기반 [권장])      ║")
    print("║  [6r] Validation 실행         (범위 기반 [자동])     ║")
    print("║  [6s] Validation 실행         (study 이름 기반)      ║")
    print("║  [6a] Validation + 자동채택   (날짜 기반 + adopt)    ║")
    print("║  [6ar] Validation + 자동채택  (범위 기반 + adopt)    ║")
    print("║  [7] 슬리피지 스트레스 테스트 (--stress)             ║")
    print("║  [7s] shadow 현황 출력        (validated/shadow/adop)║")
    print("║  [7a] shadow→adopted 수동승격 (adoption id 입력)     ║")
    print("║  [8] 채택 현황 출력           (--adoptions)          ║")
    print("║  ─────────────────────────────────────────────────   ║")
    print("║  [9] 상위 설정 군집화         (--cluster K)          ║")
    print("║  ──────────── 자동 루프 ───────────────────────────  ║")
    print("║  [a] 자동 루프 설정 / 시작    (새 창 실행)           ║")
    print("║  ─────────────────────────────────────────────────   ║")
    print("║  [d] DRY 모드 토글                                   ║")
    print("║  [0] 종료                                            ║")
    print("╚══════════════════════════════════════════════════════╝")


def choose_mode() -> str:
    while True:
        raw = input("  모드 선택 (day/night, 엔터=night): ").strip().lower()
        if raw == "":
            return "night"
        if raw in {"day", "night"}:
            return raw
        print("  day 또는 night 만 입력하세요.")


# ─────────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    args     = parse_args()
    dry_mode = args.dry
    mode     = "day"

    today = today_str()

    while True:
        try:
            # ── 루프마다 재계산 (시각·모드 변동 반영) ─────────────────
            if mode == "night":
                def_date       = last_night_date()
                def_date_label = f"마지막 야간장 ({def_date})"
            else:
                def_date       = last_day_date()
                def_date_label = f"마지막 정규장 ({def_date})"

            default_multi_dates = recent_days(3, end_offset_days=1)

            clear_screen()
            print_header(today, dry_mode, mode, def_date)
            choice = input("  선택: ").strip().lower()

            if choice == "0":
                print("종료합니다.")
                return

            if choice == "d":
                dry_mode = not dry_mode
                print(f"\nDRY 모드가 {'ON' if dry_mode else 'OFF'} 로 변경되었습니다.")
                pause()
                continue

            # ── 자동 루프 ─────────────────────────────────────────
            if choice == "a":
                run_auto_mode(mode, dry_mode)
                continue

            # ── 수동 실행 (기존) ──────────────────────────────────
            cmd = build_base(mode)

            if choice == "1":
                d     = ask(f"  날짜 (YYYY-MM-DD, 엔터={def_date_label})", def_date)
                force = ask_yes_no("  이미 완료된 run도 재실행?", False)
                cmd  += ["--date", d, "--all-profiles"]
                if force:
                    cmd.append("--force")

            elif choice == "2":
                d       = ask(f"  날짜 (YYYY-MM-DD, 엔터={def_date_label})", def_date)
                profile = ask("  프로파일 번호", "1")
                force   = ask_yes_no("  이미 완료된 run도 재실행?", False)
                cmd    += ["--date", d, "--profile", profile]
                if force:
                    cmd.append("--force")

            elif choice == "3":
                print("  보고서 종류:")
                print("    [1] 단일 세션 성적표")
                print("    [2] 멀티 세션 성적표")
                print("    [3] 랜덤/BO 탐색 결과 (study)")
                print("    [4] Validation 결과 (study)")
                print("    [5] 채택 현황 (adoptions)")
                rtype = input("  선택 [1]: ").strip() or "1"
                if rtype == "1":
                    d    = ask(f"  날짜 (YYYY-MM-DD, 엔터={def_date_label})", def_date)
                    cmd += ["--date", d, "--report-only"]
                elif rtype == "2":
                    dates = ask_dates(default_multi_dates)
                    cmd  += ["--dates", *dates, "--report-only"]
                elif rtype == "3":
                    study = ask("  study 이름")
                    d     = ask(f"  날짜 (파서용, 엔터={def_date})", def_date)
                    top   = ask("  상위 N", "10")
                    cmd  += ["--date", d, "--report-only",
                             "--study", study, "--top", top]
                elif rtype == "4":
                    study = ask("  validation study 이름")
                    dates = ask_dates(default_multi_dates)
                    top   = ask("  top N", "10")
                    cmd  += ["--dates", *dates, "--validate-study", study,
                             "--validate-top", top, "--report-only"]
                elif rtype == "5":
                    study = ask("  study 이름")
                    d     = ask(f"  날짜 (파서용, 엔터={def_date})", def_date)
                    cmd  += ["--date", d, "--study", study,
                             "--adoptions", "--report-only"]
                else:
                    print("  잘못된 선택입니다.")
                    pause()
                    continue

            elif choice == "4":
                dates = ask_dates(default_multi_dates)
                n     = ask("  trial 수", "300")
                seed  = ask("  seed", "42")
                study = ask("  study 이름 (엔터=자동)", "")
                force = ask_yes_no("  같은 study 기존 결과 삭제 후 재실행?", False)
                top   = ask("  상위 N 출력", "15")
                cmd  += ["--dates", *dates, "--random", n,
                         "--seed", seed, "--top", top]
                if study:
                    cmd += ["--study", study]
                if force:
                    cmd.append("--force")

            elif choice == "5":
                dates         = ask_dates(default_multi_dates)
                n             = ask("  BO trial 수", "200")
                startup       = ask("  startup trial 수", "40")
                seed          = ask("  seed", "42")
                default_study = "day_bo_v1" if mode == "day" else "night_bo_v1"
                study         = ask("  study 이름", default_study)
                force         = ask_yes_no("  같은 study 기존 결과 삭제 후 재실행?", False)
                top           = ask("  상위 N 출력", "15")
                cmd          += ["--dates", *dates, "--bayes", n,
                                 "--startup", startup, "--seed", seed,
                                 "--study", study, "--top", top]
                if force:
                    cmd.append("--force")

            elif choice == "6":
                # 날짜 기반 validation [권장 — cross-study 통합]
                train_d = ask(f"  탐색 기준 날짜 (train, 엔터={def_date})", def_date)
                dates   = ask_dates(default_multi_dates)
                top     = ask("  검증 top N", "10")
                force   = ask_yes_no("  기존 validation run도 재실행?", False)
                cmd    += ["--validate-date", train_d,
                           "--dates", *dates, "--validate-top", top]
                if force:
                    cmd.append("--force")

            elif choice == "6r":
                # 범위 기반 validation [자동 거래일 조회, 훈련일 제외]
                train_d = ask(f"  훈련 날짜 (train, 엔터={def_date})", def_date)
                dates   = ask_range_valid_dates([train_d], mode)
                if not dates:
                    pause("  검증 날짜가 없습니다. 엔터로 돌아가기...")
                    continue
                top   = ask("  검증 top N", "10")
                force = ask_yes_no("  기존 validation run도 재실행?", False)
                cmd  += ["--validate-date", train_d,
                         "--dates", *dates, "--validate-top", top]
                if force:
                    cmd.append("--force")

            elif choice == "6ar":
                # 범위 기반 validation + 자동채택
                train_d = ask(f"  훈련 날짜 (train, 엔터={def_date})", def_date)
                dates   = ask_range_valid_dates([train_d], mode)
                if not dates:
                    pause("  검증 날짜가 없습니다. 엔터로 돌아가기...")
                    continue
                top   = ask("  검증 top N", "10")
                force = ask_yes_no("  기존 validation run도 재실행?", False)
                cmd  += ["--validate-date", train_d,
                         "--dates", *dates, "--validate-top", top, "--adopt"]
                if force:
                    cmd.append("--force")

            elif choice == "6s":
                # study 이름 기반 validation [구방식]
                dates = ask_dates(default_multi_dates)
                study = ask("  validation 대상 study 이름")
                top   = ask("  검증 top N", "10")
                force = ask_yes_no("  기존 validation run도 재실행?", False)
                cmd  += ["--dates", *dates,
                         "--validate-study", study, "--validate-top", top]
                if force:
                    cmd.append("--force")

            elif choice == "6a":
                # 날짜 기반 validation + 자동채택
                train_d = ask(f"  탐색 기준 날짜 (train, 엔터={def_date})", def_date)
                dates   = ask_dates(default_multi_dates)
                top     = ask("  검증 top N", "10")
                force   = ask_yes_no("  기존 validation run도 재실행?", False)
                cmd    += ["--validate-date", train_d,
                           "--dates", *dates, "--validate-top", top, "--adopt"]
                if force:
                    cmd.append("--force")

            elif choice == "7":
                dates = ask_dates(default_multi_dates)
                study = ask("  stress 대상 study 이름")
                force = ask_yes_no("  기존 stress run도 재실행?", False)
                cmd  += ["--dates", *dates,
                         "--validate-study", study, "--stress"]
                if force:
                    cmd.append("--force")

            elif choice == "7s":
                # shadow 현황 출력
                from sim_opt.shadow_evaluator import ShadowEvaluator
                ev = ShadowEvaluator(
                    sim_db_path    = str(DEFAULT_SIM_DB),
                    source_db_path = str(DEFAULT_SOURCE_DB),
                )
                ev.print_shadow_status()
                pause()
                continue

            elif choice == "7a":
                # validated → paper_shadow 또는 paper_shadow → adopted 수동 승격
                from sim_opt.shadow_evaluator import ShadowEvaluator
                ev = ShadowEvaluator(
                    sim_db_path    = str(DEFAULT_SIM_DB),
                    source_db_path = str(DEFAULT_SOURCE_DB),
                )
                ev.print_shadow_status()

                adoption_id_raw = ask("  승격할 adoption ID").strip()
                if not adoption_id_raw.isdigit():
                    print("  ID가 잘못됐습니다.")
                    pause()
                    continue

                adoption_id = int(adoption_id_raw)
                promo_check = ev.check_promotion(adoption_id)
                print(f"\n  승격 기준 점검: {promo_check['reason']}")
                print(f"  eligible: {'YES' if promo_check['eligible'] else 'NO'}")

                # 단계별 처리: validated → paper_shadow 먼저
                import sqlite3 as _sqlite3
                try:
                    _sc = _sqlite3.connect(str(DEFAULT_SIM_DB), timeout=10)
                    _sc.row_factory = _sqlite3.Row
                    _cur_status = _sc.execute(
                        "SELECT status FROM sim_adoptions WHERE id=?",
                        (adoption_id,)
                    ).fetchone()
                    _sc.close()
                    cur_status = _cur_status["status"] if _cur_status else "unknown"
                except Exception:
                    cur_status = "unknown"

                if cur_status == "validated":
                    print(f"\n  현재 status=validated → paper_shadow로 이동합니까?")
                    if ask_yes_no("  paper_shadow 시작", True):
                        ok = ev.promote_to_shadow(adoption_id)
                        print(f"  결과: {'paper_shadow 시작' if ok else '실패'}")
                    pause()
                    continue

                elif cur_status == "paper_shadow":
                    force_promo = False
                    if not promo_check["eligible"]:
                        force_promo = ask_yes_no(
                            "  기준 미충족 — 강제 adopted 승격?", False
                        )
                    if promo_check["eligible"] or force_promo:
                        ok = ev.promote(adoption_id, force=force_promo)
                        print(f"  결과: {'adopted 승격 완료' if ok else '실패'}")
                    pause()
                    continue

                else:
                    print(f"  status={cur_status} — 승격 대상 아닙니다.")
                    pause()
                    continue

            elif choice == "8":
                study = ask("  채택 현황을 볼 study 이름")
                d     = ask(f"  날짜 (필수 parser 용, 엔터={def_date})", def_date)
                cmd  += ["--date", d, "--adoptions", "--study", study]

            elif choice == "9":
                study = ask("  cluster 대상 study 이름")
                k     = ask("  cluster 개수 K", "5")
                top   = ask("  top N 대상 수", "50")
                d     = ask(f"  날짜 (필수 parser 용, 엔터={def_date})", def_date)
                cmd  += ["--date", d, "--cluster", k,
                         "--study", study, "--top", top]

            else:
                print("지원하지 않는 메뉴입니다.")
                pause()
                continue

            run_command(cmd, dry_mode=dry_mode)
            pause()

        except KeyboardInterrupt:
            print("\n\nCtrl+C 감지 — 종료하지 않고 메뉴로 돌아갑니다.")
            pause("엔터를 누르면 계속합니다...")
        except Exception as exc:
            print(f"\n오류: {exc}")
            pause("엔터를 누르면 메뉴로 돌아갑니다...")


if __name__ == "__main__":
    main()
