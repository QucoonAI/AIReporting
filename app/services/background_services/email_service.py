import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content
from app.config.settings import Settings, get_settings


settings = get_settings()


class EmailService:
    def __init__(self, settings: Settings = None):
        self.sg = sendgrid.SendGridAPIClient(api_key=settings.SENDGRID_AUTH_KEY)
        self.from_email = Email("boluwatife.israel@qucoon.com")
    
    def _send_email(self, to_email: str, subject: str, html_content: str, text_content: str = None):
        if text_content:
            text_content = Content("text/plain", text_content)
        
        mail = Mail(
            from_email=self.from_email,
            to_emails=To(to_email),
            subject=subject,
            plain_text_content=text_content,
            html_content=Content("text/html", html_content)
        )
        mail_json = mail.get()

        response = self.sg.client.mail.send.post(request_body=mail_json)
        print(response.status_code)
        print(response.headers)
    
    def send_verification_email(self, to_email: str, user_name: str, otp: str):
        """Send email verification OTP."""
        subject = "Verify Your Email Address"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Email Verification</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; }}
                .header {{ text-align: center; color: #333; margin-bottom: 30px; }}
                .otp-code {{ font-size: 32px; font-weight: bold; color: #007bff; text-align: center; 
                            letter-spacing: 5px; padding: 20px; border: 2px solid #007bff; 
                            border-radius: 8px; margin: 20px 0; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; 
                          font-size: 12px; color: #666; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Email Verification</h1>
                </div>
                
                <p>Hello {user_name},</p>
                
                <p>Welcome! Please verify your email address by entering the following OTP:</p>
                
                <div class="otp-code">{otp}</div>
                
                <p>This OTP will expire in 30 minutes for security reasons.</p>
                
                <p>If you didn't create an account, please ignore this email.</p>
                
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_content = f"""
        Hello {user_name},
        
        Welcome! Please verify your email address by entering the following OTP: {otp}
        
        This OTP will expire in 30 minutes for security reasons.
        
        If you didn't create an account, please ignore this email.
        """
        
        self._send_email(to_email, subject, html_content, text_content)
    
    def send_password_reset_email(self, to_email: str, user_name: str, otp: str):
        """Send password reset OTP."""
        subject = "Reset Your Password"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Password Reset</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; }}
                .header {{ text-align: center; color: #333; margin-bottom: 30px; }}
                .otp-code {{ font-size: 32px; font-weight: bold; color: #dc3545; text-align: center; 
                            letter-spacing: 5px; padding: 20px; border: 2px solid #dc3545; 
                            border-radius: 8px; margin: 20px 0; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; 
                          font-size: 12px; color: #666; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Password Reset</h1>
                </div>
                
                <p>Hello {user_name},</p>
                
                <p>You requested to reset your password. Use the following OTP to complete the process:</p>
                
                <div class="otp-code">{otp}</div>
                
                <p>This OTP will expire in 30 minutes for security reasons.</p>
                
                <p>If you didn't request a password reset, please ignore this email and your password will remain unchanged.</p>
                
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_content = f"""
        Hello {user_name},
        
        You requested to reset your password. Use the following OTP to complete the process: {otp}
        
        This OTP will expire in 30 minutes for security reasons.
        
        If you didn't request a password reset, please ignore this email and your password will remain unchanged.
        """
        
        self._send_email(to_email, subject, html_content, text_content)
    
    def send_password_change_email(self, to_email: str, user_name: str, otp: str):
        """Send password change confirmation OTP."""
        subject = "Confirm Password Change"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Password Change Confirmation</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f4; }}
                .container {{ max-width: 600px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; }}
                .header {{ text-align: center; color: #333; margin-bottom: 30px; }}
                .otp-code {{ font-size: 32px; font-weight: bold; color: #28a745; text-align: center; 
                            letter-spacing: 5px; padding: 20px; border: 2px solid #28a745; 
                            border-radius: 8px; margin: 20px 0; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; 
                          font-size: 12px; color: #666; text-align: center; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Password Change Confirmation</h1>
                </div>
                
                <p>Hello {user_name},</p>
                
                <p>You requested to change your password. Use the following OTP to confirm the change:</p>
                
                <div class="otp-code">{otp}</div>
                
                <p>This OTP will expire in 30 minutes for security reasons.</p>
                
                <p>If you didn't request this change, please contact support immediately.</p>
                
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_content = f"""
        Hello {user_name},
        
        You requested to change your password. Use the following OTP to confirm the change: {otp}
        
        This OTP will expire in 30 minutes for security reasons.
        
        If you didn't request this change, please contact support immediately.
        """
        
        self._send_email(to_email, subject, html_content, text_content)


def send_verification_email_task(
    email_service: EmailService,
    to_email: str,
    user_name: str,
    otp: str
):
    """Background task for sending verification email."""
    email_service.send_verification_email(to_email, user_name, otp)


def send_password_reset_email_task(
    email_service: EmailService,
    to_email: str,
    user_name: str,
    otp: str
):
    """Background task for sending password reset email."""
    email_service.send_password_reset_email(to_email, user_name, otp)


def send_password_change_email_task(
    email_service: EmailService,
    to_email: str,
    user_name: str,
    otp: str
):
    """Background task for sending password change email."""
    email_service.send_password_change_email(to_email, user_name, otp)


