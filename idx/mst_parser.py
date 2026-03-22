# idx/mst_parser.py
"""
fo_idx_code_mts.mst 파일 파서 — KIS 지수선물 종목 코드 목록.

파일 형식: 파이프(|) 구분 텍스트, 9개 필드
  [0] info_type      1=지수선물  B=미니선물  (외 다수)
  [1] shrn_iscd      단축코드 (FUTURES_CODE, e.g. A01606)
  [2] stnd_iscd      표준코드
  [3] kor_name       한글종목명 (e.g. "F 202606")
  [4] atm_cls_code   ATM구분 (옵션용)
  [5] acpr           행사가 (옵션용)
  [6] mmsc_cls_code  월물구분 (0=연결 1=최근월 2=차근월 3=차차근월 ...)
  [7] unas_shrn_iscd 기초자산 단축코드 (SPOT_CODE, e.g. 2001)
  [8] unas_kor_name  기초자산명 (e.g. KOSPI200)

단축코드 구조:
  A[01|05] [연도 마지막 1자리] [월 2자리]
  A01606 = 표준선물 2026년 6월물
  A05605 = 미니선물 2026년 5월물
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional, Tuple

# MST 파일 경로 — 이 파일과 같은 디렉토리
_MST_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fo_idx_code_mts.mst")

_INFO_TYPE_LABEL: dict[str, str] = {
    "1": "지수선물",   "2": "지수SP",
    "3": "스타선물",   "4": "스타SP",
    "5": "지수콜",     "6": "지수풋",
    "7": "변동성선물", "8": "변동성SP",
    "9": "섹터선물",   "A": "섹터SP",
    "B": "미니선물",   "C": "미니SP",
    "D": "미니콜",     "E": "미니풋",
    "J": "코스닥150콜","K": "코스닥150풋",
    "L": "위클리콜",   "M": "위클리풋",
}

_MONTH_LABEL: dict[str, str] = {
    "0": "연결선물",
    "1": "최근월물",
    "2": "차근월물",
    "3": "차차근월물",
    "4": "4번째월물",
    "5": "5번째월물",
    "6": "6번째월물",
    "7": "7번째월물",
}


@dataclass
class FuturesContract:
    info_type:       str   # "1"=지수선물 "B"=미니선물
    shrn_iscd:       str   # 단축코드 (FUTURES_CODE)
    stnd_iscd:       str   # 표준코드
    kor_name:        str   # 한글종목명
    mmsc_cls_code:   str   # 월물구분코드
    unas_shrn_iscd:  str   # 기초자산 단축코드 (SPOT_CODE)
    unas_kor_name:   str   # 기초자산명

    @property
    def type_label(self) -> str:
        return _INFO_TYPE_LABEL.get(self.info_type, self.info_type)

    @property
    def month_label(self) -> str:
        return _MONTH_LABEL.get(self.mmsc_cls_code, f"월물{self.mmsc_cls_code}")

    @property
    def display_name(self) -> str:
        return f"{self.kor_name.strip()} ({self.month_label})"

    def to_dict(self) -> dict:
        return {
            "shrn_iscd":      self.shrn_iscd,
            "stnd_iscd":      self.stnd_iscd,
            "kor_name":       self.kor_name.strip(),
            "info_type":      self.info_type,
            "type_label":     self.type_label,
            "mmsc_cls_code":  self.mmsc_cls_code,
            "month_label":    self.month_label,
            "display_name":   self.display_name,
            "unas_shrn_iscd": self.unas_shrn_iscd,
            "unas_kor_name":  self.unas_kor_name,
        }


def load_contracts(
    info_types: Tuple[str, ...] = ("1", "B"),
    unas_filter: str = "KOSPI200",
    mst_path: str = _MST_PATH,
) -> List[FuturesContract]:
    """
    MST 파일에서 지수선물 종목 목록 반환.

    info_types: ("1",)="표준만" / ("B",)="미니만" / ("1","B")="둘 다"
    unas_filter: 기초자산 이름 필터 (빈 문자열 = 전체)
    """
    contracts: List[FuturesContract] = []
    try:
        with open(mst_path, encoding="cp949", errors="replace") as f:
            for line in f:
                parts = line.rstrip("\n").split("|")
                if len(parts) < 9:
                    continue
                it = parts[0]
                if it not in info_types:
                    continue
                unas_name = parts[8]
                if unas_filter and unas_filter not in unas_name:
                    continue
                contracts.append(FuturesContract(
                    info_type      = it,
                    shrn_iscd      = parts[1],
                    stnd_iscd      = parts[2],
                    kor_name       = parts[3],
                    mmsc_cls_code  = parts[6],
                    unas_shrn_iscd = parts[7],
                    unas_kor_name  = unas_name,
                ))
    except FileNotFoundError:
        pass
    return contracts


def find_nearest(
    info_type: str = "1",
    unas_filter: str = "KOSPI200",
) -> Optional[FuturesContract]:
    """
    최근월물(mmsc_cls_code='1') 반환. 없으면 첫 번째 항목.
    info_type: "1"=표준 / "B"=미니
    """
    contracts = load_contracts(info_types=(info_type,), unas_filter=unas_filter)
    for c in contracts:
        if c.mmsc_cls_code == "1":
            return c
    return contracts[0] if contracts else None


def detect_nearest_pair(
    unas_filter: str = "KOSPI200",
) -> Tuple[Optional[FuturesContract], Optional[FuturesContract]]:
    """
    (노멀 최근월물, 미니 최근월물) 쌍 반환.
    장 시작 시 자동 종목 감지에 사용.
    """
    return find_nearest("1", unas_filter), find_nearest("B", unas_filter)
