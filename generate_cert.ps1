# PowerShell 스크립트: 테스트용 자체 서명 SSL 인증서 생성
# Docker의 openssl을 사용하여 인증서를 생성합니다.
# 주의: 이 인증서는 브라우저에서 경고를 발생시킵니다 (테스트 전용)

$ErrorActionPreference = "Stop"

$sslDir = ".\ssl"
if (-not (Test-Path $sslDir)) {
    New-Item -ItemType Directory -Force -Path $sslDir | Out-Null
    Write-Host "Created directory: $sslDir" -ForegroundColor Green
}

Write-Host "자체 서명 인증서(Self-Signed Certificate)를 생성합니다..." -ForegroundColor Cyan

# Docker를 이용해 openssl 실행
docker run --rm -v "${PWD}/ssl:/export" alpine/openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /export/key.pem -out /export/cert.pem -subj "/C=KR/ST=Seoul/L=Seoul/O=Development/OU=IT/CN=localhost"

if ($LASTEXITCODE -eq 0) {
    Write-Host "인증서 생성이 완료되었습니다." -ForegroundColor Green
    Write-Host " - Private Key: .\ssl\key.pem" -ForegroundColor Yellow
    Write-Host " - Certificate: .\ssl\cert.pem" -ForegroundColor Yellow
    Write-Host "`n명령어 실행: docker-compose up -d" -ForegroundColor Cyan
} else {
    Write-Host "인증서 생성 중 오류가 발생했습니다. Docker가 실행 중인지 확인하세요." -ForegroundColor Red
}
