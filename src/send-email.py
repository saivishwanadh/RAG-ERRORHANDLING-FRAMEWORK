import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# ========== CONFIG ==========
smtp_server = "smtp.office365.com"
smtp_port = 587
username = "fahad.alikhan@prowesssoft.com"
password = "shzjkthwnrngkgnb"
to_email = "saivishwanadh.veerlapati@prowesssoft.com"

# ========== Dynamic Values ==========
exception_id = "h88d1c54-67ef90gh-12ij34kl-56mn78op"
timestamp = "2026-04-16T05:04:16Z"
error_level = "Error"
msg_code = "DBCONNECTION-FAILED"
error_message = "Database connection failed"
error_dump = "User 'svc_integration' failed authentication against LDAP server ldap://10.10.10.10:389. Invalid credentials or account locked."
engine_name = "TIBCO BW 6.5.0"
# ========== Email HTML Template ==========
html_content = f"""
<html>
<head>
<style>
body {{
    font-family: Arial, sans-serif;
}}

.section-title {{
    background-color: #b0002a;
    color: white;
    padding: 8px;
    font-weight: bold;
}}

table {{
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 20px;
}}

td {{
    border: 1px solid #dddddd;
    padding: 8px;
}}

.label {{
    background-color: #f4f4f4;
    font-weight: bold;
    width: 25%;
}}

</style>
</head>
<body>

<h2>Error Notification</h2>

<div class="section-title">HEADER</div>
<table>
<tr><td class="label">Exception ID</td><td>{exception_id}</td>
<td class="label">Domain</td><td>GDC_ESB21_UAT</td></tr>

<tr><td class="label">Error Category</td><td>Technical</td>
<td class="label">Deployment</td><td>Aboutyou-Shared-EMEA</td></tr>

<tr><td class="label">Error Level</td><td>{error_level}</td>
<td class="label">Project Name</td><td>Aboutyou-Shared-EMEA_root</td></tr>

<tr><td class="label">Timestamp UTC</td><td>{timestamp}</td>
<td class="label">Engine Name</td><td>{engine_name}</td></tr>
</table>

<div class="section-title">DOCUMENT DETAILS</div>
<table>
<tr><td class="label">Document ID</td><td>NA</td></tr>
</table>

<div class="section-title">ERROR DETAILS</div>
<table>
<tr><td class="label">Message Code</td><td>{msg_code}</td></tr>
<tr><td class="label">Message</td><td>{error_message}</td></tr>
</table>

<div class="section-title">ERROR DUMP</div>
<table>
<tr><td>{error_dump}</td></tr>
</table>

</body>
</html>
"""


# ========== Create Email ==========
msg = MIMEMultipart("alternative")
msg["From"] = username
msg["To"] = to_email
msg["Subject"] = "UAT Error Notification"

msg.attach(MIMEText(html_content, "html"))

# ========== Send ==========
server = smtplib.SMTP(smtp_server, smtp_port)
server.starttls()
server.login(username, password)
server.sendmail(username, to_email, msg.as_string())
server.quit()

print("Email sent successfully!")
