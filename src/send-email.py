import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# ========== CONFIG ==========
smtp_server = "smtp.office365.com"
smtp_port = 587
username = "fahad.alikhan@prowesssoft.com"
password = "shzjkthwnrngkgnb"
to_email = "kishore.madirgav@prowesssoft.com"

# ========== Dynamic Values ==========
exception_id = "b8122c9a-4e1a90fd-10b88d22-cc7a210e"
timestamp = "2026-02-20T10:25:09Z"
error_level = "Fatal"
msg_code = "DB-CONN-REFUSED"
error_message = "Database connection refused"
error_dump = "TIBCO BW process failed to connect to Oracle database at 10.20.5.77:1521 from application host 10.10.0.5. Connection refused by listener. Service owner notified at db.admin@company.com. Impacted user session: anita.sharma@company.com."
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
