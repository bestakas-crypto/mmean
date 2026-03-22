# -*- coding: utf-8 -*-
# scripts/git_push.py

import subprocess
import datetime
import os
import sys

# 한글 깨짐 방지를 위해 출력 인코딩을 강제로 설정해!
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding='utf-8')

def run_command(command):
    """터미널 명령어를 UTF-8로 실행하는 똑똑한 도우미"""
    try:
        # env={'PYTHONIOENCODING': 'utf-8'} 를 추가해서 대화 통일!
        result = subprocess.run(
            command, shell=True, check=True, text=True, 
            capture_output=True, encoding='utf-8'
        )
        if result.stdout: print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        # 에러 메시지도 최대한 읽을 수 있게 출력
        print(f"명령어 실행 중 알림: {e.stderr}")
        return False

def auto_github_push():
    print("🚀 깃허브 자동 동기화 및 업로드를 시작합니다...")

    # 1. 깃허브 서버에 있는 최신 내용 먼저 가져오기 (가장 중요!)
    print("📥 서버의 최신 변경사항을 가져오는 중 (git pull)...")
    run_command("git pull origin main")

    # 2. 임시파일 장바구니에서 빼기
    run_command("git rm -r --cached .claude/worktrees/ >nul 2>&1")

    # 3. 변경된 파일 담기
    print("📦 변경된 파일들을 담는 중...")
    run_command("git add .")

    # 4. 포장지 작성
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit_message = f"자동 업데이트: {now}"
    print(f"📝 메시지 작성: {commit_message}")
    
    # 변경사항이 있을 때만 commit
    if not run_command(f'git commit -m "{commit_message}"'):
        print("💡 변경된 내용이 없거나 이미 최신 상태입니다.")
        # 변경사항이 없어도 push는 시도해볼 수 있어 (pull 받은 내용이 있을 수 있으니)
    
    # 5. 서버로 발송
    print("🚚 깃허브로 최종 배송 중...")
    if run_command("git push origin main"):
        print("✅ 모든 작업 완료! 깃허브를 확인해보세요.")
    else:
        print("❌ 배송 실패. 수동으로 'git pull'을 먼저 해야 할 수도 있어요.")

if __name__ == "__main__":
    auto_github_push()