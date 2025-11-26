import smtplib
from email.mime.text import MIMEText

# 구글 메일을 보내는 함수
# send_email: 송신 메일 주소, to_email : 수신 메일 주소, app_pw : 앱 비밀번호, body : 메일 본문
def send_google_alert(send_email:str, to_email: str, app_pw:str, body: str):
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = "데이터 크롤링 중 에러 발생"
    msg['From'] = send_email
    msg['To'] = to_email

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(send_email, app_pw)
        smtp.send_message(msg)

def send_naver_alert(send_email:str, to_email:str, app_pw:str, body: str):
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = "데이터 크롤링"
    msg['From'] = send_email
    msg['To'] = to_email

    with smtplib.SMTP("smtp.naver.com", 587) as smtp:
        smtp.starttls()
        smtp.login(send_email, app_pw)
        smtp.send_message(msg)