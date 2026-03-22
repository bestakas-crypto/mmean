# MMEAN 운영 정책서 (POLICY.md)

작성일: 2026-03-22
대상: KOSPI 200 선물 정규장 / 야간장 자동매매 시스템
기준: 현재 구현 완료된 코드 기준 (aspirational 항목은 별도 표기)

---

## 목차

1. [용어 일원화](#1-용어-일원화)
2. [시스템 구조 개요](#2-시스템-구조-개요)
3. [진입 정책](#3-진입-정책)
4. [경고 신호 체계 — Caution / Ambiguous](#4-경고-신호-체계--caution--ambiguous)
5. [이상징후 차단기 CB1~CB4](#5-이상징후-차단기-cb1cb4)
6. [청산 정책](#6-청산-정책)
7. [포지션 크기 정책](#7-포지션-크기-정책)
8. [LLM 역할 범위](#8-llm-역할-범위)
9. [파라미터 승격 파이프라인](#9-파라미터-승격-파이프라인)
10. [RAG / 실패 회피 메모리](#10-rag--실패-회피-메모리)
11. [v1 한계 및 향후 과제](#11-v1-한계-및-향후-과제)

---

## 1. 용어 일원화

혼용 금지. 아래 정의 외의 표현은 코드 / 문서 어디에도 사용하지 않는다.

| 용어 | 정의 | 사용 위치 |
|---|---|---|
| **LONG** | 상승 베팅 포지션 (매수 진입, 상승으로 이익) | direction=+1, entry_signal, bias |
| **SHORT** | 하락 베팅 포지션 (매도 진입, 하락으로 이익) | direction=-1, entry_signal, bias |
| **BUY** | 매수 주문 방향 (KIS API order_type) | 주문 레이어만 |
| **SELL** | 매도 주문 방향 (KIS API order_type) | 주문 레이어만 |
| **BULL** | 상승 시장 레짐 | 레짐라벨, 시장 상태 기술 |
| **BEAR** | 하락 시장 레짐 | 레짐라벨, 시장 상태 기술 |
| **LONG_READY** | 상승 진입 조건 충족 신호 | entry_signal |
| **SHORT_READY** | 하락 진입 조건 충족 신호 | entry_signal |
| **LONG_BIAS** | flow_score 기준 상승 편향 상태 | bias 변수 |
| **SHORT_BIAS** | flow_score 기준 하락 편향 상태 | bias 변수 |
| **FORCE_EXIT_EOD** | 장 마감 전 자동 강제 청산 (14:45) | SimEngine / LiveExecutor |

> **FORCE_EXIT와 FORCE_EXIT_EOD는 목적이 다르다.**
> FORCE_EXIT_EOD는 14:45 시각 트리거 정기 청산으로 현재 활성 운영 중이다.
> FORCE_EXIT (LLM 또는 외부 명령에 의한 즉시 청산)는 **현재 v1 운영 정책에서 비활성**이다.
> v1에서 모든 청산은 SimEngine / LiveExecutor 내부 트리거(T0~T5, EOD)만 사용한다.
> FORCE_EXIT를 활성화하려면 데이터 검증과 별도 정책 결정이 선행되어야 한다.

---

## 2. 시스템 구조 개요

```
[시장 데이터]
     │
     ▼
BiasRegimeEngine  →  long_score / short_score  →  entry_signal
ForeignFlowEngine →  flow_score (-2.0 ~ +2.0)
RegimeEngine      →  레짐라벨 + long_weight / short_weight
     │
     ▼
[게이트 레이어] — 순서대로 적용, 하나라도 BLOCK이면 WAIT
  ① opening_char size_filter (skip → 차단, reduce → 0.5×)
  ② 이상징후 차단기 CB1~CB4
  ③ Warning Signal Entry Gate (Caution / Ambiguous)
     │
     ▼
LLMGate  →  |flow_score| ≥ 0.8이면 LLM 스킵 (추세 강함, 판단 불필요)
LLMCaller →  weight_adjust (-0.20 ~ +0.10) 반환
PositionEngine → order_size = base × regime_weight × (1 + weight_adjust)
     │
     ▼
SimEngine / LiveExecutor → 실제 진입/청산
     │
     ▼
JudgmentLog → judgment_events 기록 → shadow 평가 / RAG 메모리 소재
```

**DB 분리 원칙:**
- `mmean.db` — 시장 원본 (읽기전용, 절대 쓰지 않는다)
- `sim.db` — 시뮬레이션 결과 / 채택 파라미터 (쓰기)

---

## 3. 진입 정책

### 3-1. 진입 신호 생성

BiasRegimeEngine이 단독으로 판단한다. LLM은 진입 여부를 결정하지 않는다.

| 신호 | 조건 |
|---|---|
| LONG_READY | long_score ≥ enter_score AND (long_score - short_score) ≥ enter_gap |
| SHORT_READY | short_score ≥ enter_score AND (short_score - long_score) ≥ enter_gap |

입력 소스: 외국인순매수, OI증감, 베이시스, EMA 정배열, 거래량 급증, 체결강도

### 3-2. 진입 게이트 순서

아래 순서대로 적용. 앞 단계에서 차단되면 이후 단계는 실행하지 않는다.

```
1. opening_char size_filter == "skip"  → WAIT (차단)
2. CB1~CB4 이상징후 차단기              → WAIT (차단)
3. entry_gate == "REJECT"              → WAIT (차단)
4. 위 3개 모두 통과                    → entry_signal 유지
```

### 3-3. LLM 호출 조건

- 호출: |flow_score| < 0.8 (전환 구간, LLM 판단 필요)
- 스킵: |flow_score| ≥ 0.8 (추세 강함, weight_adjust = 0 자동)
- 실패 처리: timeout / JSON오류 / confidence < 0.55 → weight_adjust = 0

---

## 4. 경고 신호 체계 — Caution / Ambiguous

### 4-1. 개념 정의

두 신호는 독립적이다. 동시에 발화할 수 있으며, Ambiguous가 Caution보다 우선한다.

| 신호 | 정의 | 측정 기반 |
|---|---|---|
| **Caution** | 방향성은 유효하나 미시 체결 환경이 불리한 상태 | `pattern_memory.warning_level` (basis_zone \| volume_zone \| time_bucket) |
| **Ambiguous** | 방향성 자체의 신뢰가 붕괴된 상태 | v1 proxy: `failure_patterns.forbidden` (MED/HIGH confidence) |

> **Ambiguous v1 한계**: 현재 `forbidden` proxy는 "과거 체크포인트 조합의 반복 실패"를 뜻한다. 원래 정의인 "현재 시점 지표들의 구조 충돌"과 시제가 다르다. 운영 데이터 축적 후 실시간 지표 불일치 감지 방식으로 고도화 예정 (v2 과제).

### 4-2. entry_gate 매핑

엔진이 `warning_level`을 `entry_gate`로 직접 변환한다. LLM에 raw 등급을 전달하지 않는다.

```python
# core/engine_runtime.py — entry_gate 블록

if warning_level == "danger":       entry_gate = "REJECT"
elif warning_level in ("warning",
                       "caution"):  entry_gate = "HALF"
else:                               entry_gate = "ALLOW"

# Ambiguous proxy: forbidden(MED/HIGH) → REJECT 격상
if fac_type == "forbidden" and fac_conf in {"MED","HIGH"}:
    entry_gate = "REJECT"
```

### 4-3. entry_gate 행동 정의

| entry_gate | 진입 | weight_adjust 처리 |
|---|---|---|
| **ALLOW** | 신호 그대로 | LLM 출력 그대로 사용 |
| **HALF** ⚠ | 신호 그대로 | `_wa_scaled = -_WA_MAX_DOWN` — **최대 SIZE_DOWN 강제. 50% 수량 보장 아님** |
| **REJECT** | WAIT (차단) | `_wa_scaled = 0.0` (LLM 판단 무효화) |

> **⚠ HALF 명칭 주의 — v1 구현 내내 반드시 숙지**
>
> HALF는 정책 토큰명일 뿐이다. 직관적으로 "절반 수량"처럼 들리지만 그렇지 않다.
> 현재 v1 구현에서 HALF의 실제 동작: `_wa_scaled = -_WA_MAX_DOWN = -0.20`
> → `position_engine.compute(weight_adjust_filtered=-0.20)` 으로 전달
> → 최종 수량 = `base_size × regime_weight × 0.80` 계열 (regime_weight에 따라 달라짐)
> **50% 수량 보장이 아니다. 실제 의미는 "최대 size-down 강제"다.**
> 향후 엄밀한 50% 보장이 필요하면 PositionEngine에 별도 size_multiplier 추가가 필요하다 (v2 후보).

### 4-4. entry_gate 우선순위

`entry_gate`는 LLM `weight_adjust`보다 우선한다. LLM이 SIZE_UP(양수)을 출력했더라도 `entry_gate == "HALF"`이면 `_WA_MAX_DOWN`으로 덮어쓴다.

---

## 5. 이상징후 차단기 CB1~CB4

진입 시도 전 이상 상태를 감지하여 entry_signal을 WAIT으로 강제한다.
환경변수로 임계값 조정 가능.

| CB | 조건 | 차단 이유 코드 | 환경변수 |
|---|---|---|---|
| CB1 | `ws_connected == False` | `CB1_WS_DISCONNECTED` | — |
| CB2 | flow_engine 존재 AND `len(fnet) < MIN_FLOW_GATE` | `CB2_FLOW_INSUFFICIENT` | `MIN_FLOW_GATE` (기본 5) |
| CB3 | 선언된 session_type vs 실제 HH:MM 불일치 | `CB3_SESSION_MISMATCH` | — |
| CB4 | 같은 방향 신호 재발 < cooldown 경과 | `CB4_COOLDOWN` | `ENTRY_SIGNAL_COOLDOWN_SEC` (기본 30s) |

CB4 타이머는 신호가 차단 없이 통과할 때만 갱신된다. 차단된 신호에서는 갱신하지 않는다.

---

## 6. 청산 정책

### 6-1. SimEngine 청산 트리거

| 트리거 | 설명 | 트레일링 비활성 | 트레일링 활성 |
|---|---|---|---|
| T0 TAKE_PROFIT | tp_ticks 도달 | 고정 TP | tp_ticks > 0 시만 |
| T1 STOP_LOSS | sl_ticks 도달 | 초기 SL 고정 | 트레일링 SL로 교체 |
| T2 FORCE_EXIT_EOD | 14:45 강제 마감 | 항상 동작 | 항상 동작 |
| T3 BIAS_REVERSAL | bias 반전 | 즉시 청산 | 즉시 청산 |
| T4 SIGNAL_CONFLICT | 반대 신호 발생 | 즉시 청산 | 무시 (추세 중 노이즈) |
| T5 NEUTRAL_TIMEOUT | NEUTRAL 지속 | neutral_exit_ticks | neutral_exit_ticks × 3 |

### 6-2. 기본 청산 파라미터

| 파라미터 | 기본값 | 범위 |
|---|---|---|
| sim_sl_ticks | 10틱 | 5~25 |
| sim_tp_ticks | 0 (비활성) | 8~40 |
| sim_trailing_ticks | 12틱 | 1~8 |
| sim_trailing_activate | 8틱 | — |
| sim_neutral_exit_ticks | 3틱 | — |
| sim_profit_protect | 6틱 | — |
| sim_slippage_ticks | 1틱 (진입/청산 각각) | — |

### 6-3. LLM의 청산 권한

**LLM은 청산을 강제하지 않는다.** 모든 청산 트리거(T0~T5)는 SimEngine / LiveExecutor 내부 규칙이 전담한다. LLM은 진입 전 weight_adjust만 출력한다.

---

## 7. 포지션 크기 정책

### 7-1. 계산 공식

```
order_size = base_size
           × _oc_size_adj           (opening_char size_filter: 1.0 / 0.5 / 0.0)
           × regime_weight          (FlowEngine 연속값, 0.05 ~ 1.0)
           × (1 + weight_adjust)    (LLM 조정, entry_gate 우선순위 적용 후)
```

### 7-2. LLM weight_adjust 한도 (비대칭 클램프)

```python
_WA_MAX_UP   = 0.10   # SIZE_UP 최대 (위험 방향 — 절반으로 제한)
_WA_MAX_DOWN = 0.20   # SIZE_DOWN 최대
```

SIZE_UP과 SIZE_DOWN의 허용폭이 다르다. LLM은 포지션을 늘리는 방향보다 줄이는 방향에 더 큰 권한을 가진다.

### 7-3. 장전 보정 (PremarketManualMode)

| 모드 | 설명 |
|---|---|
| MANIA_2 / MANIA_1 | 시장 과열 시 수동 조절 |
| NORMAL | 기본값 |
| FEAR_1 / FEAR_2 | 시장 공포 시 수동 조절 |

---

## 8. LLM 역할 범위

### 8-1. LLM이 할 수 있는 것

- `weight_adjust` 출력 (`-_WA_MAX_DOWN` ~ `+_WA_MAX_UP`)
- `direction_bias` 출력 (참고용, 진입 방향 결정에 미사용)
- TP/SL 제안 (4~30틱 범위, 엔진이 최종 적용 여부 판단)
- RAG context 기반 시장 상태 근거 코드 출력

### 8-2. LLM이 할 수 없는 것

| 금지 항목 | 이유 |
|---|---|
| 진입 방향 결정 | BiasRegimeEngine 전담 |
| 진입 여부 차단 | CB 레이어 및 entry_gate 전담 |
| 청산 강제 (FORCE_EXIT) | SimEngine / LiveExecutor 전담, 데이터 검증 없음 |
| 손익 기반 판단 | FORBIDDEN_REASON_CODES로 즉시 기각 |
| 보유 중 개입 | 진입 전 1회만 호출, 보유 중 호출 금지 |
| 수치 계산 (틱가격, PnL) | 엔진 전담, LLM 부동소수점 신뢰 불가 |

### 8-3. LLM 실패 처리

아래 상황에서 LLM 판단은 없는 것으로 처리(`weight_adjust = 0`)한다.

- timeout
- JSON 파싱 오류
- 스키마 검증 실패
- confidence < 0.55
- rate_limit / provider 오류

---

## 9. 파라미터 승격 파이프라인

```
[candidate]
    │  run_slippage_stress() → stress_passed=1
    ▼
[validated]
    │  수동 승격 (sim.py [7a])
    ▼
[paper_shadow]
    │  ShadowEvaluator.promote() 조건 충족
    │  N≥20 AND PnL>0 AND WR≥45% AND shadow_obj/valid_obj ≤ 1.5
    ▼
[adopted]
```

shadow 평가는 `sim.bat` 실행 시 자동으로 `shadow_evaluator.py --session` 호출.
`sim.py [7s]` — shadow 현황 출력
`sim.py [7a]` — 수동 승격 (validated → paper_shadow, paper_shadow → adopted)

---

## 10. RAG / 실패 회피 메모리

### 10-1. 구성

| 컴포넌트 | 소재 | 역할 |
|---|---|---|
| `pattern_memory` | basis_zone \| volume_zone \| time_bucket 클러스터 | 미시 체결 환경 경고 (Caution 신호 소재) |
| `failure_patterns` | opening_char \| prov_regime \| afternoon_regime 조합 | 체크포인트 기반 반복 실패 기억 (Ambiguous proxy v1 소재) |
| `sim_rag_builder` | sim_adoptions 채택 파라미터 | 성공/실패 사례 문서화 → 벡터 검색 |

### 10-2. failure_patterns 분류 기준

| pattern_type | 조건 | LLM 주입 |
|---|---|---|
| `forbidden` | WR < 30% | ⛔ 주입 (MED/HIGH confidence만) |
| `failure` | 30% ≤ WR < 45% AND avg_pnl < 0 | ⚠ 주입 (MED/HIGH confidence만) |
| `success` | WR ≥ 50% AND avg_pnl > 0 | **미주입** — 아래 이유 참조 |

confidence 기준: LOW(n<10) — 주입 안 함 (과보수화 방지)

> **success 패턴을 주입하지 않는 이유**
>
> 현재 v1의 RAG 목적은 **진입 확장이 아니라 실패 회피**다.
> LLM에 성공 사례를 주입하면 "이런 조건에서 과거에 잘 됐다"는 확증 편향이 생겨
> 리스크가 높은 국면에서도 진입을 정당화하는 방향으로 LLM이 편향될 수 있다.
> success 패턴은 LLM 주입 대상이 아니라 **오프라인 분석 / 파라미터 튜닝 소재**로만 유지한다.

### 10-3. 과보수화 방지 원칙

- LOW confidence 패턴 미주입 (n < 10)
- n < 5 건너뜀
- override 허용 주석 포함 ("다른 강력한 진입 근거가 있으면 override 가능")
- success 패턴 미주입 — 실패 회피 우선 철학 유지

---

## 11. v1 한계 및 향후 과제

### 11-1. 현재 확정된 한계

| 항목 | 현재 v1 상태 | 향후 방향 |
|---|---|---|
| Ambiguous proxy | `forbidden` 기반 (과거 성과) | 실시간 지표 구조 충돌 감지로 교체 (데이터 필요) |
| HALF 수량 정확도 | `_wa_scaled = -0.20` → 엄밀히 50% 아님 | PositionEngine에 별도 size_multiplier 추가 (선택적) |
| failure_patterns 임계값 | `_FORBIDDEN_WR=0.30`, `_MIN_SAMPLES=5` 하드코딩 | 운영 데이터 90일 이상 후 검토 |
| sim 언어와 장중 판단 언어 | 조인 키 불일치 | 데이터 충분히 쌓인 후 정렬 |

### 11-2. 데이터 의존 과제 (지금 건드리지 않는다)

아래 항목은 **운영 데이터가 쌓인 뒤 별도 리서치 과제**로 처리한다. 지금 구현하면 데이터 없는 설계가 된다.

- Ambiguous proxy v2 (실시간 구조 충돌 감지)
- `_FORBIDDEN_WR` / `_MIN_SAMPLES` 임계값 튜닝
- confidence 등급 컷오프 보정
- sim 언어 / 장중 판단 언어 통일

### 11-3. Phase 2 설계 방향 (aspirational)

Phase 2는 데이터 충분히 축적된 이후 별도 설계한다. 현재 문서에 구체 사양을 넣지 않는다.
방향만 메모: 레짐 2-layer 정밀화, 일관성 점수 체계, LLM 역할 확장 여부 재검토.

---

## 변경 이력

| 날짜 | 내용 |
|---|---|
| 2026-03-22 | 최초 작성. Warning Signal Entry Gate 구현 완료 기준. |
| 2026-03-22 | FORCE_EXIT 비활성 명시 강화 / HALF 실제 의미 반복 강화 / success 미주입 이유 보강. |
