"""
알림 서비스
이메일 알림 전송을 담당
"""
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional


class NotificationService:
    """이메일 알림 서비스"""

    def __init__(self):
        """환경 변수에서 이메일 설정 로드"""
        self.email = os.getenv("EMAIL")
        self.password = os.getenv("PASSWORD")
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.naver.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "465"))

        self._validate_config()

    def _validate_config(self) -> None:
        """설정 유효성 검증"""
        if not self.email:
            raise ValueError("EMAIL environment variable is not set")
        if not self.password:
            raise ValueError("PASSWORD environment variable is not set")

    def send_alert(
            self,
            subject: str,
            body: str,
            to_email: Optional[str] = None
    ) -> bool:
        if not to_email:
            to_email = self.email

        try:
            # 이메일 메시지 생성
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.email
            msg['To'] = to_email

            # 본문 추가 (plain text)
            text_part = MIMEText(body, 'plain', 'utf-8')
            msg.attach(text_part)

            # HTML 버전도 추가 (보기 좋게)
            html_body = self._format_html(body)
            html_part = MIMEText(html_body, 'html', 'utf-8')
            msg.attach(html_part)

            # SMTP 연결 및 전송
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.email, self.password)
                server.send_message(msg)

            print(f"✓ 알림 전송 성공: {to_email}")
            return True

        except smtplib.SMTPAuthenticationError as e:
            print(f"✗ 인증 실패: {e}")
            return False

        except smtplib.SMTPException as e:
            print(f"✗ SMTP 오류: {e}")
            return False

        except Exception as e:
            print(f"✗ 알림 전송 실패: {e}")
            return False

    def _format_html(self, text: str) -> str:
        # 줄바꿈을 <br>로 변환
        html_text = text.replace('\n', '<br>')

        # 기본 HTML 템플릿
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{
                    font-family: 'Malgun Gothic', sans-serif;
                    background-color: #f5f5f5;
                    padding: 20px;
                }}
                .container {{
                    background-color: white;
                    padding: 30px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    max-width: 600px;
                    margin: 0 auto;
                }}
                .header {{
                    color: #2c3e50;
                    border-bottom: 3px solid #3498db;
                    padding-bottom: 15px;
                    margin-bottom: 20px;
                }}
                .content {{
                    color: #34495e;
                    line-height: 1.8;
                    white-space: pre-wrap;
                }}
                .footer {{
                    margin-top: 30px;
                    padding-top: 15px;
                    border-top: 1px solid #ecf0f1;
                    color: #7f8c8d;
                    font-size: 12px;
                    text-align: center;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>크롤링 시스템 알림</h2>
                </div>
                <div class="content">
                    {html_text}
                </div>
                <div class="footer">
                    <p>이 메일은 자동으로 발송되었습니다.</p>
                </div>
            </div>
        </body>
        </html>
        """

        return html

    def send_error_alert(
            self,
            error_type: str,
            error_message: str,
            context: Optional[dict] = None
    ) -> bool:
        subject = f"크롤링 에러 발생: {error_type}"

        body = f"""
        에러가 발생했습니다.

        ===== 에러 정보 =====
        유형: {error_type}
        메시지: {error_message}
        """

        if context:
            body += "\n\n===== 추가 정보 =====\n"
            for key, value in context.items():
                body += f"{key}: {value}\n"

        return self.send_alert(subject, body)

    def send_completion_alert(
            self,
            task_name: str,
            stats: dict
    ) -> bool:
        subject = f"{task_name} 완료"

        body = f"""
        {task_name} 작업이 완료되었습니다.

        ===== 실행 결과 =====
        """

        for key, value in stats.items():
            body += f"{key}: {value}\n"

        return self.send_alert(subject, body)

    def test_connection(self) -> bool:
        try:
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.email, self.password)

            print("✓ SMTP 연결 테스트 성공")
            return True

        except Exception as e:
            print(f"✗ SMTP 연결 테스트 실패: {e}")
            return False

def send_naver_alert(
        from_email: str,
        to_email: str,
        password: str,
        message: str
) -> bool:
    try:
        # 환경변수 임시 설정
        original_email = os.getenv("EMAIL")
        original_password = os.getenv("PASSWORD")

        os.environ["EMAIL"] = from_email
        os.environ["PASSWORD"] = password

        # NotificationService 사용
        service = NotificationService()
        result = service.send_alert(
            subject="프로그램 알림",
            body=message,
            to_email=to_email
        )

        # 환경변수 복원
        if original_email:
            os.environ["EMAIL"] = original_email
        if original_password:
            os.environ["PASSWORD"] = original_password

        return result

    except Exception as e:
        print(f"send_naver_alert failed: {e}")
        return False
