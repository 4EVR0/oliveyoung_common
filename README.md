# Git Submodule 사용 가이드

공용 코드(예: `oliveyoung_common`)를 여러 레포에서 관리하는 방법.

---

## 개념

| 용어 | 설명 |
|------|------|
| **부모 레포** | submodule을 사용하는 레포 (예: `Iceberg_pipeline`) |
| **서브모듈 레포** | 공유되는 독립 레포 (예: `oliveyoung_common`) |
| **포인터** | 부모 레포가 서브모듈의 특정 커밋 SHA를 기록하는 방식 |

> 서브모듈은 **독립된 레포**다. 부모 레포는 "어떤 커밋을 쓸지"만 기록하고, 코드 자체는 서브모듈 레포가 관리한다.

---

## 처음 클론할 때

```bash
# 서브모듈까지 한 번에 클론
git clone --recurse-submodules https://github.com/4EVR0/Iceberg_pipeline.git

# 이미 클론했는데 서브모듈이 비어있을 때
git submodule update --init
```

---

## 일상적인 작업 흐름

### 1. oliveyoung_common 코드 수정

```bash
cd oliveyoung_common   # 서브모듈 디렉토리로 이동 (독립된 레포)
# 코드 수정
git add .
git commit -m "feat: 새 기능 추가"
git push
```

### 2. 부모 레포에 최신 서브모듈 반영

```bash
cd ..   # 부모 레포 루트로 이동

# 서브모듈 최신 커밋으로 포인터 업데이트
git submodule update --remote oliveyoung_common

# 포인터 변경을 커밋
git add oliveyoung_common
git commit -m "chore: oliveyoung_common 업데이트"
git push
```

### 3. 팀원이 최신 서브모듈 받기

```bash
git pull
git submodule update   # 포인터가 가리키는 커밋으로 맞추기
```

---

## 자주 쓰는 명령어

```bash
# 서브모듈 상태 확인 (앞에 - 붙으면 초기화 안 됨)
git submodule status

# 모든 서브모듈 최신으로 업데이트
git submodule update --remote

# 특정 서브모듈만 업데이트
git submodule update --remote oliveyoung_common

# 서브모듈 포함 전체 상태 한눈에 보기
git status
```

---

## 4EVR0 프로젝트 구조

```
oliveyoung_common (독립 레포)
  └── batch.py, s3_paths.py, logging.py ...

Iceberg_pipeline/
  └── oliveyoung_common  ← submodule (포인터만 기록)

Oliveyoung_Crawling/
  └── Olive_Crawling/oliveyoung_common  ← submodule

INCI_Pipeline/
  └── oliveyoung_common  ← submodule

GraphRAG_Pipeline/
  └── oliveyoung_common  ← submodule
```

### oliveyoung_common 수정 → 전 파이프라인 반영 순서

```bash
# 1. 서브모듈 레포 수정 & push
cd oliveyoung_common
git add . && git commit -m "fix: ..." && git push

# 2. 각 파이프라인에서 업데이트 (병렬로 해도 됨)
for PIPELINE in Iceberg_pipeline INCI_Pipeline GraphRAG_Pipeline; do
  cd /path/to/4EVR0/$PIPELINE
  git submodule update --remote oliveyoung_common
  git add oliveyoung_common
  git commit -m "chore: oliveyoung_common 업데이트"
  git push
done

# Oliveyoung_Crawling은 경로가 다름
cd /path/to/4EVR0/Oliveyoung_Crawling
git submodule update --remote Olive_Crawling/oliveyoung_common
git add Olive_Crawling/oliveyoung_common
git commit -m "chore: oliveyoung_common 업데이트"
git push
```

---

## 주의사항

### ❌ 흔한 실수

```bash
# 서브모듈 디렉토리 안에서 부모 레포 커밋하지 말기
cd oliveyoung_common
git commit ...   # 이건 oliveyoung_common 레포에 커밋되는 것

# 부모에 커밋하려면 반드시 부모 루트에서
cd ..
git commit ...
```

### ⚠️ git pull 후 서브모듈 업데이트 필수

```bash
git pull
git submodule update   # 빠뜨리면 서브모듈이 이전 커밋에 머무름
```

### `.gitmodules` 확인

```ini
[submodule "oliveyoung_common"]
    path = oliveyoung_common
    url = https://github.com/4EVR0/oliveyoung_common.git
```

`path`: 부모 레포 내 디렉토리 위치  
`url`: 서브모듈 원격 레포 주소

---

## detached HEAD가 뜰 때

서브모듈은 기본적으로 특정 커밋에 고정되어 **detached HEAD** 상태가 된다. 정상이다.
서브모듈 내에서 코드를 수정하려면 브랜치를 먼저 체크아웃해야 한다.

```bash
cd oliveyoung_common
git checkout main   # 브랜치로 이동 후 수정
```
