"""
session_detect.py — 시간 기반 자동 세션 감지

장 시간표:
    데이장  : 08:44 ~ 15:45
    장외    : 그 외

※ 야간장 모드는 신뢰성 문제로 비활성화됨 (추후 재구성 예정)

사용법:
    from session_detect import detect_session
    session = detect_session()  # "day" | None
"""

from datetime import datetime
import logging

log = logging.getLogger("MMEAN.SessionDetect")

# ─── 장 시간 정의 (HHMM 정수 비교 — 초 단위 오차 없음) ──────
_DAY_START = 844   # 08:44
_DAY_END   = 1545  # 15:45


def _hhmm(dt: datetime) -> int:
    """datetime → HHMM 정수. 예) 08:44:30 → 844"""
    return dt.hour * 100 + dt.minute


def detect_session(now: datetime | None = None) -> str | None:
    """
    현재 시각 기준 세션 자동 감지.

    Returns:
        "day"  : 데이장 시간 (08:44 ~ 15:45)
        None   : 장외 시간
    """
    if now is None:
        now = datetime.now()

    hm = _hhmm(now)

    if _DAY_START <= hm <= _DAY_END:
        return "day"

    return None


def detect_session_strict(now: datetime | None = None) -> str:
    """
    장외 시간이면 RuntimeError 발생.
    프로그램 시작 시 강제 검증용.
    """
    session = detect_session(now)
    if session is None:
        if now is None:
            now = datetime.now()
        raise RuntimeError(
            f"장외 시간입니다 ({now.strftime('%H:%M')}). "
            f"데이장 08:44~15:45 에 실행하세요."
        )
    return session


def get_session(env_override: str | None = None,
                now: datetime | None = None,
                strict: bool = False) -> str:
    """
    최종 세션 결정 함수. app_bootstrap.py 에서 호출.

    우선순위:
        1. env_override 가 "day" 로 명시된 경우 → 그대로 사용
        2. env_override 가 없거나 "auto" → 시간 자동 감지

    Args:
        env_override : os.getenv("MMEAN_SESSION") 값 (없으면 None)
        now          : 테스트용 datetime 주입 (None = 현재 시각)
        strict       : True 면 장외 시간 시 RuntimeError

    Returns:
        "day"
    """
    # 명시 override (night는 무시하고 day로 처리)
    if env_override:
        ev = env_override.lower()
        if ev == "day":
            log.info("세션 모드 [수동설정]: DAY")
            return "day"
        if ev == "night":
            log.warning("야간 모드는 비활성화됨 — 'day' 로 대체 적용")
            return "day"

    # 자동 감지
    if strict:
        session = detect_session_strict(now)
    else:
        session = detect_session(now)
        if session is None:
            if now is None:
                now = datetime.now()
            log.warning(
                "장외 시간 (%s) — 세션 감지 불가. 기본값 'day' 적용.",
                now.strftime("%H:%M")
            )
            session = "day"

    log.info("세션 모드 [자동감지]: %s  (%s)",
             session.upper(),
             (now or datetime.now()).strftime("%H:%M"))
    return session


# ─── CLI 테스트 ──────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from datetime import timedelta

    print("=" * 50)
    print("  세션 감지 테스트")
    print("=" * 50)

    test_cases = [
        datetime.now().replace(hour=8,  minute=44),   # 데이장 시작
        datetime.now().replace(hour=12, minute=0),    # 데이장 중간
        datetime.now().replace(hour=15, minute=45),   # 데이장 종료
        datetime.now().replace(hour=16, minute=0),    # 장외
        datetime.now().replace(hour=18, minute=0),    # 야간장 시작
        datetime.now().replace(hour=23, minute=0),    # 야간장 중간
        datetime.now().replace(hour=0,  minute=30),   # 자정 이후
        datetime.now().replace(hour=5,  minute=59),   # 야간장 종료 직전
        datetime.now().replace(hour=6,  minute=0),    # 야간장 종료 → 장외
    ]

    for dt in test_cases:
        result = detect_session(dt)
        label = result.upper() if result else "장외"
        print(f"  {dt.strftime('%H:%M')}  →  {label}")

    print()
    print(f"  현재시각: {datetime.now().strftime('%H:%M')}  →  {detect_session() or '장외'}")
    print("=" * 50)
