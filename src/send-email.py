import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ============================================================
# SMTP CONFIG
# ============================================================
smtp_server = "smtp.office365.com"
smtp_port   = 587
username    = "fahad.alikhan@prowesssoft.com"
password    = "shzjkthwnrngkgnb"
to_email    = "saivishwanadh.veerlapati@prowesssoft.com"


EXCEPTIONID     = "ef9c5a76-8ccd1234-77bb88aa-9900dd11"
ERRORCATEGORY   = "Business"
ERRORTYPE       = "Transformation"
ERRORCODE       = "XMLSchemaValidationFailed"
ERRORLEVEL      = "High"
TIMESTAMPUTC    = "2026-05-12T16:08:37Z"

DOMAIN          = "SAP_ESB_PRD"
DEPLOYMENT      = "Toyota-JP_STD"
PROJECTNAME     = "Toyota-JP_STD_root"
ENGINENAME      = "Toyota-JP_STD-LB-esbp14"

DOCUMENTID       = "ef9c5a75-8ccd1234-33ee44ff-77665544"
DOCUMENTNAME     = "TOY778DELIVERY-260512094256-12344321123.XML"
DOCUMENTCATEGORY = "Delivery"

MSGCODE      = "XSDValidationError"
PROCESSSTACK = "XMLValidation/Common/Process/ValidateSchema.process>SchemaValidation"
MSG          = (
    "Element 'DeliveryDate' contains invalid value format. "
    "Expected format=YYYY-MM-DD, "
    "Received format=12/05/2026, "
    "DocumentType=Delivery_I-301-09"
)
ERRORDUMP    = ""   # Leave blank or add a stack trace

# ============================================================
# HTML TEMPLATE  — matches exact TIBCO email structure
# ============================================================
html_content = f"""\
<html>
<head>
<style>
  body {{ font-family: Arial, sans-serif; font-size: 13px; }}
  h2   {{ color: #333; }}

  .section-title {{
      background-color: #8b0000;
      color: white;
      padding: 6px 10px;
      font-weight: bold;
      font-size: 13px;
      text-align: center;
  }}

  table {{
      width: 100%;
      border-collapse: collapse;
      margin-bottom: 0;
  }}

  td {{
      border: 1px solid #ccc;
      padding: 5px 8px;
      vertical-align: top;
  }}

  .label {{
      background-color: #f4d0d0;
      font-weight: bold;
      width: 18%;
      white-space: nowrap;
  }}

  a {{ color: #0563C1; }}
</style>
</head>
<body>

<!-- ═══════════════════ HEADER ═══════════════════ -->
<div class="section-title">HEADER</div>
<table>
  <tr>
    <td class="label">EXCEPTIONID</td>  <td>{EXCEPTIONID}</td>
    <td class="label">DOMAIN</td>       <td>{DOMAIN}</td>
  </tr>
  <tr>
    <td class="label">ERRORCATEGORY</td><td>{ERRORCATEGORY}</td>
    <td class="label">DEPLOYMENT</td>   <td>{DEPLOYMENT}</td>
  </tr>
  <tr>
    <td class="label">ERRORTYPE</td>    <td>{ERRORTYPE}</td>
    <td class="label">PROJECTNAME</td>  <td>{PROJECTNAME}</td>
  </tr>
  <tr>
    <td class="label">ERRORCODE</td>    <td>{ERRORCODE}</td>
    <td class="label">ENGINENAME</td>   <td>{ENGINENAME}</td>
  </tr>
  <tr>
    <td class="label">ERRORLEVEL</td>   <td>{ERRORLEVEL}</td>
    <td></td><td></td>
  </tr>
  <tr>
    <td class="label">TIMESTAMPUTC</td> <td>{TIMESTAMPUTC}</td>
    <td></td><td></td>
  </tr>
</table>

<!-- ═══════════════════ DOCUMENT DETAILS ═══════════════════ -->
<div class="section-title">DOCUMENT DETAILS</div>
<table>
  <tr>
    <td class="label">DOCUMENTID</td>
    <td><a href="#">{DOCUMENTID}</a></td>
  </tr>
  <tr>
    <td class="label">DOCUMENTNAME</td>
    <td>{DOCUMENTNAME}</td>
  </tr>
  <tr>
    <td class="label">DOCUMENTCATEGORY</td>
    <td>{DOCUMENTCATEGORY}</td>
  </tr>
</table>

<!-- ═══════════════════ ERROR DETAILS ═══════════════════ -->
<div class="section-title">ERROR DETAILS</div>
<table>
  <tr>
    <td class="label">MSGCODE</td>
    <td>{MSGCODE}</td>
  </tr>
  <tr>
    <td class="label">PROCESSSTACK</td>
    <td>{PROCESSSTACK}</td>
  </tr>
  <tr>
    <td class="label">MSG</td>
    <td>{MSG}</td>
  </tr>
</table>

<!-- ═══════════════════ ERROR DUMP ═══════════════════ -->
<div class="section-title">ERROR DUMP</div>
<table>
  <tr>
    <td class="label">ERRORDUMP</td>
    <td>{ERRORDUMP if ERRORDUMP else "&nbsp;"}</td>
  </tr>
</table>

</body>
</html>
"""

# ============================================================
# SEND
# ============================================================
msg = MIMEMultipart("alternative")
msg["From"]    = username
msg["To"]      = to_email
msg["Subject"] = f"TIBCO Error Notification - {ERRORCODE}"

msg.attach(MIMEText(html_content, "html"))

server = smtplib.SMTP(smtp_server, smtp_port)
server.starttls()
server.login(username, password)
server.sendmail(username, to_email, msg.as_string())
server.quit()

print(f"✅ Email sent successfully! [{ERRORCODE}] → {to_email}")
