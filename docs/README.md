# MMEAN — AI-Driven KOSPI 200 Auto-Trading System

LLM(Claude / GPT / Gemini) 기반 실시간 KOSPI 200 선물 자동매매 시스템.
한국투자증권 KIS Open API (WebSocket + REST) 연동. Flask 대시보드 포함.

---

## 개요

BiasRegimeEngine이 진입 방향을 단독 판단하고, LLM은 포지션 크기(±20%)만 조정한다.
entry_gate(ALLOW / HALF / REJECT)가 RAG 패턴 메모리 기반으로 위험 국면을 차단하며,
CB1~CB4 서킷브레이커가 이상 상태를 감지해 진입을 막는다.

---

## 아키텍처

```
[KIS WebSocket]
      │ 실시간 체결가 (0.5s 루프)
      ▼
core/engine_runtime.py
      │
      ├─ regime/BiasRegimeEngine     long/short_score → LONG_READY / SHORT_READY
      ├─ engines/ForeignFlowEngine   외국인 순매수 → flow_score (-2.0 ~ +2.0)
      ├─ engines/FlowRegimeEngine    flow_score → regime_weight (sigmoid 연속값)
      │
      ├─ [게이트 레이어]
      │   ① opening_char size_filter   장전 특성 기반 차단 / 축소
      │   ② CB1~CB4 서킷브레이커       WS단절 / 데이터부족 / 세션불일치 / 쿨다운
      │   ③ entry_gate                 ALLOW / HALF / REJECT  (RAG 패턴 기반)
      │
      ├─ engines/LLMGate → LLMCaller  |flow_score| < 0.8 구간만 LLM 호출
      ├─ engines/PositionEngine        order_size = base × regime_weight × (1 + wa)
      └─ sim_opt/SimEngine             진입 / 청산 / PnL 관리 (T0~T5)
```

**DB 분리 원칙**
- `storage/mmean.db` — 시장 원본 (읽기 전용)
- `storage/sim.db` — 시뮬레이션 / 최적화 결과 (쓰기)

---

## 디렉토리 구조

```
C:\mmean\
├─ analyzer_app.py        Flask 앱 진입점
├─ path_setup.py          서브디렉토리 sys.path 등록
├─ sim.py                 시뮬레이션 CLI 메뉴
├─ mmean.bat              엔진 시작 배치
├─ sim.bat / rag_prep.bat / rep.bat  운영 보조 배치
│
├─ core/                  실행 코어
│   app_bootstrap.py      전체 초기화 (PID락, .env, 엔진 조립, RAG 백그라운드)
│   engine_runtime.py     메인 루프 — entry_gate / CB1~CB4 / LLM / 주문사이즈
│   app_state.py          AppRuntime dataclass
│   kis_data_api.py       KIS 시세·잔고 REST API
│   kis_order_api.py      KIS 주문 REST API
│   session_detect.py     세션 감지 (day / night)
│
├─ regime/                레짐 분석
│   regime_engine.py      BiasRegimeEngine (주간)
│   regime_recorder.py    regime_ticks DB 저장
│
├─ engines/               FlowEngine 파이프라인
│   foreign_flow_engine.py  외국인 flow_score
│   llm_gate.py / llm_caller.py / llm_ema_filter.py / llm_ttl_manager.py
│   position_engine.py    최종 order_size 계산
│   sim_profile_resolver.py  Easy/Expert 모드 병합
│
├─ llm/                   LLM 레이어
│   llm_chain.py          Claude/GPT/Gemini 멀티공급자 + 폴백
│   llm_schemas.py        OpportunitySchema (Pydantic)
│   prompt_manager.py     프롬프트 버전 관리 (DB)
│
├─ config/                파라미터 관리
│   config_manager.py     ConfigManager (SQLite 영속)
│   param_store.py        ParamStore 싱글톤
│
├─ order/                 주문 실행 레이어
│   order_executor.py     주문 실행기
│   kis_fill_ws.py        체결 WebSocket
│
├─ sim_opt/               시뮬레이션 / 최적화
│   sim_engine.py         SimEngine v3 (트레일링스탑 + T0~T5)
│   day_sim.py            DaySimRunner (Optuna 베이지안)
│   replay_sim.py         과거 틱 재생 시뮬
│   shadow_evaluator.py   paper_shadow → adopted 승격 판정
│
├─ rag/                   RAG / 패턴 메모리
│   rag_prep.py           regime_ticks → pattern_memory / failure_patterns
│   rag_retriever.py      벡터 유사도 검색
│
├─ db/                    DB 스키마 / 관리
│   db_setup.py           mmean.db 초기화 + 마이그레이션
│   db_setup_sim.py       sim.db 초기화
│
├─ routes/                Flask REST API
│   routes_core.py        /api/status, /api/config, /api/mode
│   routes_analytics.py   /api/analytics/*, /api/sim/*
│
├─ templates/             Flask HTML
│   dashboard.html        실시간 대시보드 (entry_gate / RAG 헬스 / 파라미터 50개)
│
├─ docs/                  문서
│   POLICY.md             운영 정책서
│   read.md               전체 디렉토리 설명 (상세)
│
├─ storage/  (gitignore)
│   mmean.db / sim.db
│
└─ workspace/  (gitignore)
    levels_v2.json        Easy Mode 레벨 1~20 파라미터
```

---

## 시뮬레이션 2모드

| 모드 | 설명 | config 소스 |
|------|------|-------------|
| **Easy** | 레벨(1~20) 선택만으로 완성 | `workspace/levels_v2.json` |
| **Expert** | 개별 파라미터 직접 조정 | `ConfigManager` (DB 영속) |

- Easy 모드 활성 시 `/api/config` POST 차단 (409 반환)

---

## entry_gate

RAG 패턴 메모리(pattern_memory / failure_patterns) 기반으로 위험 국면을 감지한다.

| gate | 조건 | 동작 |
|------|------|------|
| **REJECT** | warning_level=danger / forbidden(MED·HIGH) | 진입 차단 |
| **HALF** | warning_level=warning / caution | 최대 size-down 강제 (`weight_adjust=-0.20`) |
| **ALLOW** | 이상 없음 | 신호 그대로 |

---

## 파라미터 승격 파이프라인

```
[candidate] → slippage stress test → [validated]
           → 수동 승격 → [paper_shadow]
           → N≥20 AND PnL>0 AND WR≥45% → [adopted]
```

---

## 환경 설정

```bash
# .env.secrets 에 설정 필요 (절대 커밋 금지)
KIS_APP_KEY=...
KIS_APP_SECRET=...
KIS_ACCOUNT=...
ANTHROPIC_API_KEY=...    # Claude
OPENAI_API_KEY=...       # GPT (선택)
GOOGLE_API_KEY=...       # Gemini (선택)
```

```bash
# 실행
C:\mmean\mmean.bat          # Windows (conda mmean 환경)
python analyzer_app.py      # 직접 실행
```

---

## LLM 역할 원칙

| 항목 | LLM 권한 |
|------|----------|
| 진입 방향 결정 | ❌ BiasRegimeEngine 전담 |
| 진입 차단 | ❌ CB1~CB4 + entry_gate 전담 |
| 청산 | ❌ SimEngine 내부 규칙 전담 |
| **포지션 크기** | ✅ weight_adjust ±20% 조정만 허용 |

추세가 강할 때(`|flow_score| ≥ 0.8`)는 LLM 호출 자체를 스킵한다.
