C:\mmean\
│
├─ 진입점
│   analyzer_app.py            Flask 앱 진입점 — 런타임 조립 & 라우트 등록
│   path_setup.py              서브디렉토리 sys.path 자동 등록
│   sim.py                     시뮬레이션 CLI 메뉴 (day/night/ab/replay/shadow)
│
├─ 배치 파일
│   mmean.bat                  엔진 시작 (conda mmean 활성화 → analyzer_app.py)
│   sim.bat                    시뮬레이션 메뉴 (sim.py 래퍼)
│   rag_prep.bat               RAG 사전작업 수동 실행 (step 1/2/4/0)
│   rag_build.bat              RAG 벡터 빌드
│   rag_menu.bat               RAG 메뉴 통합
│   rep.bat                    리포트 출력 (sim_report.py --watch)
│   db_archive.bat             DB 아카이브 (C드라이브 → F드라이브)
│   day_pattern.bat            일별 패턴 분석
│   purge_sim_trades.bat       sim_trades 오래된 데이터 정리
│   install.bat                의존성 설치 (requirements.txt)
│   gitput.bat                 git 커밋+push 자동화
│
├─ core/                       실행 코어
│   app_bootstrap.py           전체 초기화 — PID락, .env 로드, 엔진 조립, RAG 백그라운드
│   app_state.py               AppRuntime dataclass (엔진 공유 컨테이너)
│   engine_runtime.py          메인 루프 0.5s — entry_gate / CB1~CB4 / LLM / 주문사이즈
│   kis_data_api.py            KIS 시세·잔고·OI REST API 래퍼
│   kis_order_api.py           KIS 주문 REST API 래퍼
│   kis_tr_catalog.py          KIS TR 코드 카탈로그 (FHKIF03020100 등)
│   session_detect.py          세션 감지 (day / night — 현재 night 비활성)
│
├─ regime/                     레짐 분석 엔진
│   regime_engine.py           BiasRegimeEngine — long/short_score → entry_signal
│   night_regime_engine.py     NightRegimeEngine — 야간 레짐 (현재 비활성)
│   regime_recorder.py         regime_ticks → mmean.db INSERT
│
├─ engines/                    FlowEngine 파이프라인
│   foreign_flow_engine.py     외국인 순매수 누적 → flow_score (-2.0 ~ +2.0)
│   regime_engine.py           FlowRegimeEngine — flow_score → regime_weight (sigmoid)
│   llm_gate.py                |flow_score| ≥ 0.8 → LLM 스킵 판단
│   llm_input_builder.py       LLM 프롬프트용 feature dict 생성
│   llm_caller.py              LLM 호출 오케스트레이터 (진입 전 1회)
│   llm_ema_filter.py          weight_adjust EMA 평활화
│   llm_ttl_manager.py         weight_adjust TTL 감쇠
│   position_engine.py         order_size = base × regime_weight × (1 + weight_adjust)
│   sim_profile_resolver.py    Easy/Expert 모드 config 병합 (levels_v2.json)
│   adoption_view_adapter.py   sim_adoptions 조회 어댑터
│   checkpoint_analyst.py      체크포인트 조합 분석 (failure_patterns 소재)
│   market_analyst.py          시장 피처 분석 보조
│
├─ llm/                        LLM 레이어
│   llm_chain.py               LLMChain — Claude/GPT/Gemini 멀티공급자 + 폴백 체인
│   llm_controller.py          [비활성] 장전 전략 스케줄러
│   llm_filter.py              실시간 진입 신호 LLM 필터 (TTL 캐싱)
│   llm_schemas.py             OpportunitySchema (Pydantic)
│   prompt_manager.py          프롬프트 버전 관리 (DB 영속)
│   prompts/defaults/          기본 프롬프트 텍스트 (DB 없을 때 fallback)
│
├─ config/                     파라미터 관리
│   config_manager.py          ConfigManager + DEFAULT_CONFIG (SQLite 영속, hash 기반)
│   param_store.py             ParamStore 싱글톤 (매 틱 resolved_config 동기화)
│
├─ order/                      주문 실행 레이어 (실거래)
│   kis_auth_order.py          KIS 인증 + 공통 헤더 관리
│   kis_fill_ws.py             KIS 체결 WebSocket 수신
│   kis_order_api.py           KIS 주문 REST API
│   kis_tr_catalog.py          TR 코드 카탈로그
│   order_executor.py          주문 실행기 (진입/청산 지시)
│   order_state.py             주문 상태 추적
│
├─ sim_opt/                    시뮬레이션 / 최적화
│   sim_engine.py              SimEngine v3 — 트레일링스탑 + T0~T5 청산
│   day_sim.py                 DaySimRunner — Optuna 랜덤→베이지안→검증
│   night_sim.py               NightSimRunner — 야간 전용
│   replay_sim.py              과거 틱 재생 시뮬 (레벨별 성적표)
│   experiment_runner.py       파라미터 그리드 일괄 탐색
│   shadow_evaluator.py        Shadow 평가 — paper_shadow → adopted 승격 판정
│   sim_report.py              성과 리포트 (WR / PnL / 드로우다운)
│
├─ rag/                        RAG / 패턴 메모리 시스템
│   rag_prep.py                step1: regime_ticks → market_features
│                              step2: market_features → pattern_memory (Caution 소재)
│                              step4: judgment_events → failure_patterns (Ambiguous 소재)
│   pattern_schema.py          21개 REGIME_TAGS, PatternDocMeta 스키마
│   rag_retriever.py           벡터 유사도 기반 패턴 문서 검색
│   sim_rag_builder.py         sim.db + mmean.db → pattern_docs
│   sim_vector_store.py        벡터 임베딩 저장 / 검색
│
├─ db/                         DB 관리
│   db_setup.py                mmean.db 스키마 초기화 + 마이그레이션
│   db_setup_sim.py            sim.db 스키마 초기화
│   db_archive.py              C드라이브 → F드라이브 아카이브 + VACUUM
│   export_dev_db.py           개발용 경량 DB 추출 (최근 10,000건)
│   check_db.py                DB 상태 점검 유틸
│   purge_sim_trades.py        sim_trades 오래된 레코드 정리
│
├─ routes/                     Flask REST API
│   routes_core.py             /api/status, /api/config, /api/mode, /api/position/*
│   routes_analytics.py        /api/analytics/*, /api/sim/*, /api/report/*
│
├─ scripts/                    단독 실행 스크립트
│   day_auto_run.py            주간 자동 최적화 스케줄러 (무한 루프)
│   night_auto_run.py          야간 자동 최적화 스케줄러
│   night.py                   야간 시뮬 실행 래퍼
│   git_push.py                자동 git 커밋+push
│
├─ templates/                  Flask Jinja2 HTML
│   dashboard.html             메인 실시간 대시보드 (entry_gate / RAG 헬스 / 파라미터 50개)
│   analytics.html             거래 분석 리포트 페이지
│   ai_report.html             AI 전략 판단 이력 뷰
│   prompt_board.html          LLM 프롬프트 편집 보드
│
├─ docs/                       문서
│   POLICY.md                  운영 정책서 (진입/청산/포지션/LLM 역할 정의)
│   after.txt                  후속 작업 목록 (B/A/C 카테고리)
│   makelog.txt                작업 이력
│   read.md                    이 파일 — 디렉토리 구조 설명
│
├─ storage/  (gitignore)       DB 파일
│   mmean.db                   실거래 메인 DB (regime_ticks, judgment_events 등)
│   sim.db                     최적화 전용 DB (sim_runs, sim_trades, sim_adoptions)
│
└─ workspace/  (gitignore)     레벨 JSON (코드에서 상대경로로 참조)
    levels_v2.json             Easy Mode 레벨 1~20 파라미터 정의
    day_levels.json            정규장 Optuna 레벨 범위 정의
    night_levels.json          야간장 Optuna 레벨 범위 정의
