
import os
import html
import smtplib
import re
from email.message import EmailMessage
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)


def format_solution_text(solution_text: str) -> str:
    """
    Convert plain text solution with numbered steps to formatted HTML.
    
    Args:
        solution_text: Raw solution text with \n line breaks
        
    Returns:
        HTML formatted solution text with proper paragraphs and lists
    """
    if not solution_text:
        return ""
    
    # Replace escaped newlines with actual newlines
    text = solution_text.replace("\\n", "\n")
    
    # Split into lines
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    
    html_parts = []
    in_list = False
    
    for line in lines:
        # Check if line starts with a number (e.g., "1.", "2.", "3.")
        if re.match(r'^\d+\.\s+', line):
            if not in_list:
                html_parts.append('<ol style="margin-left: 20px; margin-top: 10px; line-height: 1.6;">')
                in_list = True
            # Remove the number and add as list item
            content = re.sub(r'^\d+\.\s+', '', line)
            html_parts.append(f'<li style="margin-bottom: 8px;">{content}</li>')
        else:
            # Close list if we were in one
            if in_list:
                html_parts.append("</ol>")
                in_list = False
            # Add as paragraph
            html_parts.append(f'<p style="margin-bottom: 10px; line-height: 1.6;">{line}</p>')
    
    # Close list if still open
    if in_list:
        html_parts.append("</ol>")
    
    return "\n".join(html_parts)


def format_confirmed_solutions(solutions_text: str) -> str:
    """
    Format confirmed solutions from vector database.
    
    Args:
        solutions_text: Text containing multiple solutions
        
    Returns:
        HTML formatted solutions
    """
    if not solutions_text:
        return '<p style="color: #666;">No confirmed solutions available.</p>'
    
    # Try to parse as JSON first (in case it's stored as JSON string)
    try:
        import json
        solutions_dict = json.loads(solutions_text)
        if isinstance(solutions_dict, dict):
            # Format each solution from the dict
            html_parts = []
            for i in range(1, 4):  # solution1, solution2, solution3
                key = f"solution{i}"
                if key in solutions_dict:
                    instructions = solutions_dict[key].get("instructions", "")
                    if instructions:
                        html_parts.append(
                            f'<div style="margin-bottom: 15px; padding: 15px; '
                            f'background-color: #e8f5e9; border-left: 4px solid #4caf50; '
                            f'border-radius: 4px;">'
                        )
                        html_parts.append(
                            f'<h4 style="color: #2e7d32; margin-top: 0; margin-bottom: 10px;">'
                            f'Confirmed Solution {i}</h4>'
                        )
                        html_parts.append(format_solution_text(instructions))
                        html_parts.append('</div>')
            
            return "\n".join(html_parts) if html_parts else '<p style="color: #666;">No confirmed solutions available.</p>'
    except:
        pass
    
    # Fallback: Split by "Solution N:" pattern
    solution_blocks = re.split(r'Solution \d+:', solutions_text)
    solution_blocks = [block.strip() for block in solution_blocks if block.strip()]
    
    if not solution_blocks:
        return '<p style="color: #666;">No confirmed solutions available.</p>'
    
    html_parts = []
    
    for i, block in enumerate(solution_blocks, 1):
        html_parts.append(
            f'<div style="margin-bottom: 15px; padding: 15px; '
            f'background-color: #e8f5e9; border-left: 4px solid #4caf50; '
            f'border-radius: 4px;">'
        )
        html_parts.append(
            f'<h4 style="color: #2e7d32; margin-top: 0; margin-bottom: 10px;">'
            f'Confirmed Solution {i}</h4>'
        )
        html_parts.append(format_solution_text(block))
        html_parts.append('</div>')
    
    return "\n".join(html_parts)


class EmailService:
    def __init__(
        self,
        template_path: str,
        smtp_host: Optional[str] = None,
        smtp_port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        # SMTP configuration (fallback to env vars)
        self.smtp_host = os.getenv("SMTP_HOST")
        self.smtp_port = int(os.getenv("SMTP_PORT"))
        self.username = os.getenv("SMTP_USERNAME")
        self.password = os.getenv("SMTP_PASSWORD")

        if not self.username or not self.password:
            raise RuntimeError("SMTP credentials missing. Set SMTP_USERNAME and SMTP_PASSWORD")

        # Email template file path
        self.template_path = os.path.normpath(
            os.path.join(os.path.dirname(__file__), "..", "UI", template_path)
        )

    def _escape_html(self, value: Any, preserve_formatting: bool = False) -> str:
        """
        Safely escape HTML content.
        
        Args:
            value: Value to escape
            preserve_formatting: If True, preserves HTML tags (for formatted solutions)
        """
        if value is None:
            return ""
        
        text = str(value)
        
        # If preserve_formatting is True and text contains HTML tags, don't escape
        if preserve_formatting and re.search(r'<(p|ol|ul|li|div|br|h\d)>', text):
            return text
        
        return html.escape(text, quote=True)

    def populate_template_llm(self, payload: Dict[str, Any]) -> str:
        """
        Populate template with LLM-generated solutions.
        Solutions will be automatically formatted with proper HTML structure.
        """
        with open(self.template_path, "r", encoding="utf-8") as f:
            tpl = f.read()

        # Extract and format solution instructions
        solution1_text = payload.get("solution1", {}).get("instructions", "")
        solution2_text = payload.get("solution2", {}).get("instructions", "")
        solution3_text = payload.get("solution3", {}).get("instructions", "")
        root_cause = payload.get("rootCause", "")

        replacements = {
            "{{SERVICE_NAME}}": self._escape_html(payload.get("serviceName")),
            "{{APP_ENV}}": self._escape_html(payload.get("environment")),
            "{{TIMESTAMP}}": self._escape_html(payload.get("timestamp")),
            "{{ERROR_TYPE}}": self._escape_html(payload.get("errorType")),
            "{{ERROR_MESSAGE}}": self._escape_html(payload.get("errorMessage")),
            "{{ERROR_ID}}": self._escape_html(payload.get("errorId")),
            "{{SESSION_ID}}": self._escape_html(payload.get("sessionId")),
            "{{ROOT_CAUSE}}": format_solution_text(root_cause),
            "{{SOLUTION1_INSTRUCTIONS}}": format_solution_text(solution1_text),
            "{{SOLUTION2_INSTRUCTIONS}}": format_solution_text(solution2_text),
            "{{SOLUTION3_INSTRUCTIONS}}": format_solution_text(solution3_text),
        }

        for k, v in replacements.items():
            tpl = tpl.replace(k, v)

        return tpl

    def populate_template_db(self, payload: Dict[str, Any]) -> str:
        """
        Populate template with database-confirmed solutions.
        Both LLM solutions and confirmed solutions will be formatted with proper HTML.
        """
        with open(self.template_path, "r", encoding="utf-8") as f:
            tpl = f.read()

        # Extract and format solution instructions
        solution1_text = payload.get("solution1", {}).get("instructions", "")
        solution2_text = payload.get("solution2", {}).get("instructions", "")
        solution3_text = payload.get("solution3", {}).get("instructions", "")
        root_cause = payload.get("rootCause", "")
        confirmed_solutions = payload.get("confirmedSolutions", "")

        replacements = {
            "{{SERVICE_NAME}}": self._escape_html(payload.get("serviceName")),
            "{{APP_ENV}}": self._escape_html(payload.get("environment")),
            "{{TIMESTAMP}}": self._escape_html(payload.get("timestamp")),
            "{{ERROR_TYPE}}": self._escape_html(payload.get("errorType")),
            "{{ERROR_MESSAGE}}": self._escape_html(payload.get("errorMessage")),
            "{{ERROR_ID}}": self._escape_html(payload.get("errorId")),
            "{{SESSION_ID}}": self._escape_html(payload.get("sessionId")),
            "{{ROOT_CAUSE}}": format_solution_text(root_cause),
            "{{SOLUTION1_INSTRUCTIONS}}": format_solution_text(solution1_text),
            "{{SOLUTION2_INSTRUCTIONS}}": format_solution_text(solution2_text),
            "{{SOLUTION3_INSTRUCTIONS}}": format_solution_text(solution3_text),
            "{{CONFIRMED_INSTRUCTIONS}}": format_confirmed_solutions(confirmed_solutions),
        }

        for k, v in replacements.items():
            tpl = tpl.replace(k, v)

        return tpl

    def send_email(self, html_body: str, subject: str, to_addrs: str):
        """Send HTML Email via SMTP."""
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.username
        msg["To"] = to_addrs
        msg.set_content("This is an HTML email. Your client does not support HTML.")
        msg.add_alternative(html_body, subtype="html")

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=30) as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.login(self.username, self.password)
            s.send_message(msg)

        return True


# ============================================================================
# Test Function
# ============================================================================

def test_formatting():
    """Test the formatting functions."""
    sample_solution = """As a quick fix, configure the APIKit Router to accept POST requests for the /timeout endpoint.
1. In Anypoint Studio, open the Mule project and navigate to the apikit-config.xml file.
2. Locate the APIKit Router configuration for the /timeout endpoint.
3. Deploy the updated Mule application to the target environment."""
    
    print("Original text:")
    print(sample_solution)
    print("\n" + "="*80 + "\n")
    
    formatted = format_solution_text(sample_solution)
    print("Formatted HTML:")
    print(formatted)
    print("\n" + "="*80 + "\n")
    
    # Test confirmed solutions
    confirmed = """Solution 1:
Check network connectivity.
1. Verify firewall rules.
2. Test database connection.

Solution 2:
Restart the service.
1. Stop the application.
2. Clear cache.
3. Start the application."""
    
    formatted_confirmed = format_confirmed_solutions(confirmed)
    print("Formatted Confirmed Solutions:")
    print(formatted_confirmed)


'''if __name__ == "__main__":
    test_formatting()'''