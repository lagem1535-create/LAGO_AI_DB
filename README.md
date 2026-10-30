# LAGO 뉴스 자동 수집 (GitHub Actions)

매일 **07:00, 16:00 (한국 시간)** 에 뉴스를 모아 허깅페이스 `lagem/lago-wiki-db/news/`에 저장해요.
PC가 꺼져 있어도 깃허브가 돌려요.

## 처음 한 번 설정

1. 깃허브에서 **New repository** → 이름 `LAGO_AI_DB` → **Private** → Create
2. 파일 올리기
   - **Add file → Upload files** → `collect_news.py`, `requirements.txt`, `README.md` 올리고 Commit
   - **Add file → Create new file** → 이름 칸에 `.github/workflows/news.yml` 을 그대로 입력
     (슬래시를 치면 폴더가 저절로 생김) → 이 폴더의 `news.yml` 내용을 전부 복사해서 붙여넣기 → Commit
   - `news.yml` 이 꼭 `.github/workflows/` 안에 있어야 자동 실행돼요
3. 저장소 **Settings → Secrets and variables → Actions → New repository secret**
   - Name: `HF_TOKEN` / Secret: 허깅페이스 Write 토큰
4. **Actions** 탭 → 워크플로를 켜라는 버튼이 나오면 켜기
5. 테스트: **Actions → LAGO 뉴스 수집 → Run workflow** (backfill_days 0) → 초록 체크면 성공

## 알아 둘 것
- 깃허브 예약 실행은 몇 분~수십 분 늦게 시작될 수 있음
- 비공개 저장소 무료 한도: 한 달 2,000분. 한 번에 약 5~10분 × 하루 2번 → 한 달 300~600분
- 저장소에 60일 동안 아무 변화가 없으면 깃허브가 예약 실행을 멈출 수 있음 → Actions 탭에서 다시 켜기
- 지난 30일 채우기는 **캐글 노트북(`news_수집.ipynb`, BACKFILL_DAYS=30)** 으로 하는 걸 추천 (시간이 오래 걸려서)
