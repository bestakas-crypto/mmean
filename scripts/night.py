
from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

BASE_DIR  = Path(__file__).resolve().parent
_PROJ_DIR = BASE_DIR.parent   # scripts/ → 프로젝트 루트
SCRIPT_PATH = BASE_DIR / 'night_sim.py'
DEFAULT_SOURCE_DB = _PROJ_DIR / 'storage' / 'mmean.db'
DEFAULT_SIM_DB    = _PROJ_DIR / 'storage' / 'sim.db'
DEFAULT_NIGHT_JSON = BASE_DIR.parent / 'workspace' / 'night_levels.json'


def clear_screen() -> None:
    try:
        os.system('cls' if os.name == 'nt' else 'clear')
    except Exception:
        pass


def today_str() -> str:
    return date.today().isoformat()


def yesterday_str() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def recent_days(n: int = 3, end_offset_days: int = 1) -> list[str]:
    """
    기본 멀티 날짜:
    - 엔터 시 최근 3일
    - 기본 종료 기준은 '어제'
    """
    end_date = date.today() - timedelta(days=end_offset_days)
    start_date = end_date - timedelta(days=n - 1)
    return [(start_date + timedelta(days=i)).isoformat() for i in range(n)]


def ask(prompt: str, default: str | None = None) -> str:
    if default is not None:
        raw = input(f"{prompt} [{default}]: ").strip()
        return raw or default
    return input(f"{prompt}: ").strip()


def ask_yes_no(prompt: str, default: bool = False) -> bool:
    tag = 'Y/n' if default else 'y/N'
    raw = input(f"{prompt} ({tag}): ").strip().lower()
    if not raw:
        return default
    return raw in {'y', 'yes', '1', 'true'}


def ask_dates(default_dates: list[str]) -> list[str]:
    default_text = ' '.join(default_dates)
    raw = input(f"  날짜들 (공백구분, 엔터=최근 3일) [{default_text}]: ").strip()
    if not raw:
        return default_dates
    return sorted(set(raw.split()))


def build_base() -> list[str]:
    return [
        sys.executable,
        str(SCRIPT_PATH),
        '--source-db', str(DEFAULT_SOURCE_DB),
        '--sim-db', str(DEFAULT_SIM_DB),
        '--night-json', str(DEFAULT_NIGHT_JSON),
    ]


def cmd_text(args: list[str]) -> str:
    return ' '.join(shlex.quote(x) for x in args)


def run_command(args: list[str], dry_mode: bool = False) -> int:
    preview = cmd_text(args)
    print('\n' + '─' * 72)
    print(f'  실행 명령어: {preview}')
    print('─' * 72)

    if dry_mode:
        print('  [DRY MODE] 실제 실행은 하지 않았습니다.')
        print('─' * 72)
        return 0

    proc = subprocess.run(args)
    print('─' * 72)
    print(f'  종료 코드: {proc.returncode}')
    print('─' * 72)
    return proc.returncode


def print_menu(today: str, dry_mode: bool) -> None:
    dry_label = 'ON' if dry_mode else 'OFF'
    print('╔══════════════════════════════════════════════════════╗')
    print('║        MMEAN 야간 시뮬레이션 런처                   ║')
    print(f'║  오늘: {today:<42}║')
    print(f'║  DRY 모드: {dry_label:<38}║')
    print('╠══════════════════════════════════════════════════════╣')
    print('║  [1] 전체 프로파일 실행       (--all-profiles)       ║')
    print('║  [2] 단일 프로파일 실행       (--profile N)          ║')
    print('║  [3] 결과 보고서 출력         (--report-only)        ║')
    print('║  ──────────────────────────────────────────────────  ║')
    print('║  [4] 랜덤 탐색                (--random N)           ║')
    print('║  [5] Bayesian 탐색            (--bayes N)            ║')
    print('║  ──────────────────────────────────────────────────  ║')
    print('║  [6] Validation 실행          (--validate-study)     ║')
    print('║  [7] 슬리피지 스트레스 테스트 (--stress)             ║')
    print('║  [8] 채택 현황 출력           (--adoptions)          ║')
    print('║  ──────────────────────────────────────────────────  ║')
    print('║  [9] 상위 설정 군집화         (--cluster K)          ║')
    print('║  [d] DRY 모드 토글                                   ║')
    print('║  [0] 종료                                            ║')
    print('╚══════════════════════════════════════════════════════╝')


def parse_launcher_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description='MMEAN 야간 시뮬레이션 인터랙티브 런처',
    )
    p.add_argument(
        '--dry',
        action='store_true',
        help='실제 실행 없이 명령어만 미리 확인',
    )
    return p.parse_args()


def pause(msg: str = '\n엔터를 누르면 런처로 돌아갑니다...') -> None:
    try:
        input(msg)
    except KeyboardInterrupt:
        print('\n메뉴로 돌아갑니다...')


def main() -> None:
    launcher_args = parse_launcher_args()
    dry_mode = launcher_args.dry

    today = today_str()
    yesterday = yesterday_str()
    default_multi_dates = recent_days(3, end_offset_days=1)

    while True:
        try:
            clear_screen()
            print_menu(today, dry_mode)
            choice = input('  선택: ').strip().lower()

            if choice == '0':
                print('종료합니다.')
                return

            if choice == 'd':
                dry_mode = not dry_mode
                print(f"\nDRY 모드가 {'ON' if dry_mode else 'OFF'} 로 변경되었습니다.")
                pause()
                continue

            args = build_base()

            if choice == '1':
                d = ask('  날짜 (YYYY-MM-DD, 엔터=어제)', yesterday)
                force = ask_yes_no('  이미 완료된 run도 재실행?', False)
                args += ['--date', d, '--all-profiles']
                if force:
                    args.append('--force')

            elif choice == '2':
                d = ask('  날짜 (YYYY-MM-DD, 엔터=어제)', yesterday)
                profile = ask('  프로파일 번호', '1')
                force = ask_yes_no('  이미 완료된 run도 재실행?', False)
                args += ['--date', d, '--profile', profile]
                if force:
                    args.append('--force')

            elif choice == '3':
                mode = ask('  보고서 유형 (session/multi/random/validation/adoptions)', 'session').lower()
                if mode == 'session':
                    d = ask('  날짜 (YYYY-MM-DD, 엔터=어제)', yesterday)
                    args += ['--date', d, '--report-only']
                elif mode == 'multi':
                    dates = ask_dates(default_multi_dates)
                    args += ['--dates', *dates, '--report-only']
                elif mode == 'random':
                    study = ask('  study 이름')
                    d = ask('  날짜 (필수 parser 용, 엔터=어제)', yesterday)
                    top = ask('  상위 N', '10')
                    args += ['--date', d, '--report-only', '--study', study, '--top', top]
                elif mode == 'validation':
                    study = ask('  validation study 이름')
                    dates = ask_dates(default_multi_dates)
                    top = ask('  top N', '10')
                    args += ['--dates', *dates, '--report-only', '--validate-study', study, '--validate-top', top]
                elif mode == 'adoptions':
                    study = ask('  study 이름')
                    d = ask('  날짜 (필수 parser 용, 엔터=어제)', yesterday)
                    args += ['--date', d, '--report-only', '--adoptions', '--study', study]
                else:
                    print('지원하지 않는 보고서 유형입니다.')
                    pause('엔터를 누르면 메뉴로 돌아갑니다...')
                    continue

            elif choice == '4':
                dates = ask_dates(default_multi_dates)
                n = ask('  trial 수', '300')
                seed = ask('  seed', '42')
                study = ask('  study 이름 (엔터=자동)', '')
                force = ask_yes_no('  같은 study 기존 결과 삭제 후 재실행?', False)
                top = ask('  상위 N 출력', '15')
                args += ['--dates', *dates, '--random', n, '--seed', seed, '--top', top]
                if study:
                    args += ['--study', study]
                if force:
                    args.append('--force')

            elif choice == '5':
                dates = ask_dates(default_multi_dates)
                n = ask('  BO trial 수', '200')
                startup = ask('  startup trial 수', '40')
                seed = ask('  seed', '42')
                study = ask('  study 이름', 'night_bo_v1')
                force = ask_yes_no('  같은 study 기존 결과 삭제 후 재실행?', False)
                top = ask('  상위 N 출력', '15')
                args += ['--dates', *dates, '--bayes', n, '--startup', startup, '--seed', seed, '--study', study, '--top', top]
                if force:
                    args.append('--force')

            elif choice == '6':
                dates = ask_dates(default_multi_dates)
                study = ask('  validation 대상 study 이름')
                top = ask('  검증 top N', '10')
                force = ask_yes_no('  기존 validation run도 재실행?', False)
                args += ['--dates', *dates, '--validate-study', study, '--validate-top', top]
                if force:
                    args.append('--force')

            elif choice == '7':
                dates = ask_dates(default_multi_dates)
                study = ask('  stress 대상 study 이름')
                force = ask_yes_no('  기존 stress run도 재실행?', False)
                args += ['--dates', *dates, '--validate-study', study, '--stress']
                if force:
                    args.append('--force')

            elif choice == '8':
                study = ask('  채택 현황을 볼 study 이름')
                d = ask('  날짜 (필수 parser 용, 엔터=어제)', yesterday)
                args += ['--date', d, '--adoptions', '--study', study]

            elif choice == '9':
                study = ask('  cluster 대상 study 이름')
                k = ask('  cluster 개수 K', '5')
                top = ask('  top N 대상 수', '50')
                d = ask('  날짜 (필수 parser 용, 엔터=어제)', yesterday)
                args += ['--date', d, '--cluster', k, '--study', study, '--top', top]

            else:
                print('지원하지 않는 메뉴입니다.')
                pause('엔터를 누르면 메뉴로 돌아갑니다...')
                continue

            run_command(args, dry_mode=dry_mode)
            pause()

        except KeyboardInterrupt:
            print('\n\nCtrl+C 감지 — 종료하지 않고 메뉴로 돌아갑니다.')
            pause('엔터를 누르면 계속합니다...')
        except Exception as exc:
            print(f'\n오류: {exc}')
            pause('엔터를 누르면 메뉴로 돌아갑니다...')


if __name__ == '__main__':
    main()
