# path_setup.py — MMEAN 서브디렉토리 sys.path 등록
# analyzer_app.py, sim.py 등 진입점에서 최상단에 import한다.
#
# 디렉토리 재구성 후 각 서브디렉토리의 모듈들이
# 서로를 flat import (from app_bootstrap import ...)로 참조하므로
# 모든 서브디렉토리를 sys.path에 등록해 기존 import가 그대로 작동하게 한다.

import sys
import os

def setup() -> None:
    root = os.path.dirname(os.path.abspath(__file__))
    # insert(0,...) 반복 특성상 리스트 마지막 항목이 sys.path[0](최우선)에 위치.
    # core/ 를 마지막에 삽입 → sys.path[0] 최우선.
    # 이렇게 해야 flat import 충돌(kis_tr_catalog 등)에서 core/ 모듈이 우선 로드됨.
    # order/ 등 이름 충돌 위험 있는 패키지는 앞쪽(낮은 우선순위)에 배치.
    subdirs = [
        "order",
        "db",
        "sim_opt",
        "scripts",
        "routes",
        "rag",
        "logs",
        "llm",
        "engines",
        "config",
        "regime",
        "idx",    # ← 지수선물 계약 스펙 / MST 파서
        "core",   # ← 마지막 삽입 → sys.path[0] 최우선
    ]
    # root 먼저 등록
    if root not in sys.path:
        sys.path.insert(0, root)
    # 서브디렉토리 등록 (중복 방지)
    for sub in subdirs:
        path = os.path.join(root, sub)
        if os.path.isdir(path) and path not in sys.path:
            sys.path.insert(0, path)

setup()
