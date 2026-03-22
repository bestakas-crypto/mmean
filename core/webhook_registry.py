# MMEAN/core/webhook_registry.py
"""
웹훅 클라이언트 레지스트리.

.env.webhook 파일을 파싱해 WebhookClient 목록을 반환한다.
각 클라이언트는 숫자 ID(1, 2, 3, …)로 구분된다.

파일 형식 (.env.webhook):
    CLIENT_1_URL=https://client1.example.com/webhook
    CLIENT_1_SECRET=hmac-secret-optional
    CLIENT_1_TOKEN=token-for-inbound-auth
    CLIENT_1_NAME=MyBot          (선택, 로그 식별용)

    CLIENT_2_URL=https://client2.example.com/mmean
    ...

발신(MMEAN → 클라이언트):
    CLIENT_N_URL    — POST 이벤트(진입/청산/강제청산) 전송 목적지
                      비어 있으면 발신 건너뜀 (조회 전용 클라이언트 허용)
    CLIENT_N_SECRET — HMAC-SHA256 서명 시크릿 (없으면 서명 헤더 미포함)

수신(클라이언트 → MMEAN):
    CLIENT_N_TOKEN  — POST /api/webhook/state 호출 시 X-MMEAN-Token 헤더 값
                      등록된 토큰과 일치해야 현재 시장 상태 응답

등록 기준:
    URL 또는 TOKEN 중 하나라도 있으면 레지스트리에 올린다.
    URL 만 있으면 → 발신 전용 (조회 인증 불가)
    TOKEN 만 있으면 → 조회 전용 (발신 이벤트 수신 불가)
    둘 다 있으면  → 양방향
    둘 다 없으면 → 건너뜀
"""
from __future__ import annotations

import hmac as _hmac
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class WebhookClient:
    """등록된 웹훅 클라이언트 1개."""
    id: int
    name: str
    url: str = ""          # MMEAN → 클라이언트 이벤트 전송 URL (빈 문자 = 발신 불가)
    secret: str = ""       # 발신 HMAC 서명 시크릿 (빈 문자 = 서명 없음)
    token: str = ""        # 클라이언트 → MMEAN 조회 인증 토큰 (빈 문자 = 조회 불허)


def load_registry(base_dir: str) -> List[WebhookClient]:
    """
    base_dir/.env.webhook 을 읽어 WebhookClient 목록을 반환한다.

    - 파일이 없으면 빈 리스트 반환 (오류 아님).
    - URL / TOKEN 중 하나라도 있으면 등록 (조회 전용 클라이언트 허용).
    - URL도 TOKEN도 없는 항목은 건너뜀.
    - N 순으로 정렬해 반환.
    """
    env_path = os.path.join(base_dir, ".env.webhook")
    if not os.path.exists(env_path):
        return []

    raw: Dict[int, Dict[str, str]] = {}

    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key   = key.strip()
            value = value.strip()

            # CLIENT_<N>_<FIELD> 패턴 매칭
            m = re.match(r"^CLIENT_(\d+)_(URL|SECRET|TOKEN|NAME)$", key, re.IGNORECASE)
            if not m:
                continue

            n          = int(m.group(1))
            field_name = m.group(2).upper()
            if n not in raw:
                raw[n] = {}
            raw[n][field_name] = value

    clients: List[WebhookClient] = []
    for n in sorted(raw):
        entry  = raw[n]
        url    = entry.get("URL",    "").strip()
        token  = entry.get("TOKEN",  "").strip()

        # URL도 TOKEN도 없으면 의미 없는 항목 — 건너뜀
        if not url and not token:
            continue

        clients.append(WebhookClient(
            id=n,
            name=entry.get("NAME", f"client-{n}"),
            url=url,
            secret=entry.get("SECRET", "").strip(),
            token=token,
        ))

    return clients


def find_by_token(clients: List[WebhookClient], token: str) -> Optional[WebhookClient]:
    """
    토큰 문자열로 클라이언트를 찾아 반환한다.

    - 토큰이 비어 있거나 등록된 클라이언트가 없으면 None.
    - 보안: 빈 token 을 가진 클라이언트는 매칭하지 않는다
      (TOKEN 미설정 클라이언트는 조회 인증 불허).
    - 타이밍 공격 방지: hmac.compare_digest() 사용.
    """
    if not token:
        return None
    token_bytes = token.encode()
    for c in clients:
        if c.token and _hmac.compare_digest(c.token.encode(), token_bytes):
            return c
    return None
