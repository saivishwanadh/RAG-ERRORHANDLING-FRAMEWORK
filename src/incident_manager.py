"""
incident_manager.py
====================
Loosely-coupled Incident Management integration for the Opssolver Engine.

Architecture
------------
  IncidentProvider (ABC)          <- abstract interface
      └── ServiceNowProvider      <- concrete implementation for ServiceNow
  IncidentManager                 <- single entry-point the engine calls
      ├── loads the correct IncidentProvider via factory
      └── tracks incident state in local PostgreSQL  (itsm_incidents table)

To swap ServiceNow for another provider (e.g. Jira, PagerDuty):
  1. Create a new class that inherits IncidentProvider.
  2. Change ITSM_PROVIDER in .env to a matching key.
  3. Add the mapping in IncidentManager._load_provider().
  No changes needed anywhere else in the codebase.

Priority mapping
----------------
  count >= HIGH_PRIORITY_THRESHOLD  ->  Snow priority 1 (Critical)
  count <  HIGH_PRIORITY_THRESHOLD  ->  Snow priority 3 (Moderate)
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional, Tuple

import requests

from src.config import Config
from src.structuraldb import DB

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SNOW_PRIORITY_CRITICAL = 1   # corresponds to ServiceNow "1 - Critical"
SNOW_PRIORITY_MODERATE = 3   # corresponds to ServiceNow "3 - Moderate"

# ---------------------------------------------------------------------------
# Abstract Base Class — the contract every ITSM provider must follow
# ---------------------------------------------------------------------------


class IncidentProvider(ABC):
    """Abstract ITSM provider interface.

    Any concrete provider (ServiceNow, Jira, PagerDuty …) must implement
    these three methods.  The rest of the application only talks to this
    interface via IncidentManager.
    """

    @abstractmethod
    def create_incident(
        self,
        title: str,
        description: str,
        priority: int,
        work_notes: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Create a new incident.

        Returns
        -------
        (incident_id, incident_display)
            incident_id      – opaque provider-internal ID (e.g. ServiceNow sys_id)
            incident_display – human-readable reference (e.g. INC0012345)
        """

    @abstractmethod
    def update_incident(
        self,
        incident_id: str,
        notes: str,
        priority: Optional[int] = None,
    ) -> None:
        """Append notes to an existing incident and optionally change its priority."""

    @abstractmethod
    def resolve_incident(
        self,
        incident_id: str,
        resolution_notes: str,
    ) -> None:
        """Mark the incident as resolved (state=6) in the provider system."""

    @abstractmethod
    def is_incident_active(self, incident_id: str) -> bool:
        """Return True if the incident is still open / not resolved."""


# ---------------------------------------------------------------------------
# ServiceNow concrete implementation
# ---------------------------------------------------------------------------


class ServiceNowProvider(IncidentProvider):
    """Talks to the ServiceNow Table API (no custom fields required)."""

    TABLE_API = "/api/now/table/incident"

    def __init__(self):
        self._base_url = (Config.SERVICENOW_INSTANCE_URL or "").rstrip("/")
        self._session = requests.Session()
        self._session.auth = (
            Config.SERVICENOW_USERNAME,
            Config.SERVICENOW_PASSWORD,
        )
        self._session.headers.update(
            {"Content-Type": "application/json", "Accept": "application/json"}
        )

    # -- helpers ---------------------------------------------------------

    def _url(self, path: str = "") -> str:
        return f"{self._base_url}{self.TABLE_API}{path}"

    def _post(self, payload: dict) -> dict:
        resp = self._session.post(self._url(), json=payload, timeout=30)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"[IncidentManager] ServiceNow API Error: {resp.text}")
            raise e
        return resp.json().get("result", {})

    def _patch(self, sys_id: str, payload: dict) -> dict:
        resp = self._session.patch(self._url(f"/{sys_id}"), json=payload, timeout=30)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"[IncidentManager] ServiceNow API Error: {resp.text}")
            raise e
        return resp.json().get("result", {})

    def _get(self, sys_id: str) -> dict:
        resp = self._session.get(
            self._url(f"/{sys_id}"),
            params={"sysparm_fields": "sys_id,number,state,priority"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("result", {})

    # -- interface methods -----------------------------------------------

    def create_incident(
        self,
        title: str,
        description: str,
        priority: int,
        work_notes: Optional[str] = None,
    ) -> Tuple[str, str]:
        body = {
            "short_description": title,
            "description": description,
            "priority": str(priority),
            "category": "Software",
            "impact": "2",
            "urgency": "2" if priority != SNOW_PRIORITY_CRITICAL else "1",
        }
        if work_notes:
            body["work_notes"] = work_notes
        # Assign to the configured group if one is set
        group = getattr(Config, "SERVICENOW_ASSIGNMENT_GROUP", "")
        if group:
            body["assignment_group"] = group

        result = self._post(body)
        sys_id = result.get("sys_id", "")
        number = result.get("number", "")
        logger.info(
            f"[IncidentManager] ServiceNow incident created: {number} (sys_id={sys_id})"
            + (f", assigned to group='{group}'" if group else "")
        )
        return sys_id, number

    def update_incident(
        self,
        incident_id: str,
        notes: str,
        priority: Optional[int] = None,
    ) -> None:
        body: dict = {"work_notes": notes}
        if priority is not None:
            body["priority"] = str(priority)
            if priority == SNOW_PRIORITY_CRITICAL:
                body["urgency"] = "1"
                body["impact"] = "1"
                # Re-apply assignment group on escalation so it's never lost
                group = getattr(Config, "SERVICENOW_ASSIGNMENT_GROUP", "")
                if group:
                    body["assignment_group"] = group
        self._patch(incident_id, body)
        logger.info(
            f"[IncidentManager] ServiceNow incident updated: sys_id={incident_id}, "
            f"priority_change={priority}"
        )

    def resolve_incident(
        self,
        incident_id: str,
        resolution_notes: str,
    ) -> None:
        close_code = getattr(Config, "SERVICENOW_CLOSE_CODE", "Workaround provided") or "Workaround provided"
        body: dict = {
            "state": "6",  # 6 = Resolved
            "close_notes": resolution_notes,
            "close_code": close_code,
        }
        self._patch(incident_id, body)
        logger.info(
            f"[IncidentManager] ServiceNow incident resolved: sys_id={incident_id}"
        )

    def is_incident_active(self, incident_id: str) -> bool:
        """Returns True when state != 6 (Resolved) and != 7 (Closed)."""
        try:
            result = self._get(incident_id)
            state = int(result.get("state", 1))
            return state not in (6, 7)  # 6=Resolved, 7=Closed
        except Exception as exc:
            logger.warning(
                f"[IncidentManager] Could not fetch SNOW state for {incident_id}: {exc}. "
                f"Assuming active to avoid duplicate creation."
            )
            return True


# ---------------------------------------------------------------------------
# Database helpers  (itsm_incidents table)
# ---------------------------------------------------------------------------


# DB functions moved inside IncidentManager for connection sharing

# ---------------------------------------------------------------------------
# IncidentManager — the only class the rest of the engine should import
# ---------------------------------------------------------------------------


class IncidentManager:
    """
    Facade used by error-solution-create.py and the extractors.

    Usage
    -----
    manager = IncidentManager()

    # standard (new or duplicate) error processed by the core engine
    manager.handle_error(
        error_key   = "MyApp_DB_TIMEOUT",
        app_name    = "MyApp",
        error_code  = "DB_TIMEOUT",
        description = "Connection timed out after 30s",
        count       = 1,           # occurrence_count from payload
        llm_summary = "Root cause: overloaded DB. Steps: ...",
    )

    # high-priority escalation triggered by the extractors
    manager.handle_high_priority(
        error_key   = "MyApp_DB_TIMEOUT",
        app_name    = "MyApp",
        error_code  = "DB_TIMEOUT",
        description = "Connection timed out after 30s",
        count       = 10,
        llm_summary = "",          # optional — may not be available from extractor
    )
    """

    def __init__(self):
        self._provider: Optional[IncidentProvider] = None
        self._provider_name: str = (Config.ITSM_PROVIDER or "none").lower().strip()
        self._enabled: bool = self._provider_name not in ("", "none", "disabled")
        self._table_ready: bool = False
        self._db: Optional[DB] = None

        if self._enabled:
            self._db = DB()  # persistent connection
            self._provider = self._load_provider(self._provider_name)
            logger.info(f"[IncidentManager] Initialized with provider={self._provider_name}")
        else:
            logger.info("[IncidentManager] ITSM integration disabled (ITSM_PROVIDER=none).")

    def __del__(self):
        if hasattr(self, "_db") and self._db:
            self._db.close()

    def _ensure_table_exists(self) -> None:
        """Create the itsm_incidents table if it doesn't exist."""
        sql = """
            CREATE TABLE IF NOT EXISTS itsm_incidents (
                id SERIAL PRIMARY KEY,
                error_key VARCHAR(255) UNIQUE NOT NULL,
                provider VARCHAR(50) NOT NULL DEFAULT 'servicenow',
                incident_id VARCHAR(100) NOT NULL,
                incident_display VARCHAR(100) NOT NULL,
                priority INT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        if self._db:
            self._db.execute(sql)
            logger.info("[IncidentManager] itsm_incidents table ensured.")

    def _ensure_ready(self) -> bool:
        """Create the DB table on first use and return True if ITSM is enabled.

        This lazy approach avoids timing issues where the DB may not be ready
        when the module is first imported (e.g., Docker container startup).
        Returns False if ITSM is disabled, so callers can exit early.
        """
        if not self._enabled:
            return False
        if not self._table_ready:
            try:
                self._ensure_table_exists()
                self._table_ready = True
            except Exception as exc:
                logger.error(
                    f"[IncidentManager] DB table setup failed: {exc}. "
                    f"Will retry on next call."
                )
                return False  # Don't proceed — table might not exist
        return True

    # -- factory ---------------------------------------------------------

    @staticmethod
    def _load_provider(name: str) -> IncidentProvider:
        """Return the concrete provider matching `name`.

        To add a new provider later:
          1. Create `class MyProvider(IncidentProvider)` above.
          2. Add `"myprovider": MyProvider` to the registry below.
        """
        registry = {
            "servicenow": ServiceNowProvider,
        }
        cls = registry.get(name)
        if cls is None:
            raise ValueError(
                f"Unknown ITSM provider '{name}'. "
                f"Valid options: {list(registry.keys())} or 'none'."
            )
        return cls()

    # -- priority mapping ------------------------------------------------

    @staticmethod
    def _map_priority(count: int) -> int:
        return (
            SNOW_PRIORITY_CRITICAL
            if count >= Config.HIGH_PRIORITY_THRESHOLD
            else SNOW_PRIORITY_MODERATE
        )

    # -- public API ------------------------------------------------------

    def handle_error(
        self,
        error_key: str,
        app_name: str,
        error_code: str,
        description: str,
        count: int,
        llm_summary: str = "",
    ) -> Optional[dict]:
        """
        Called by the core engine (error-solution-create.py) after LLM analysis.

        Test case coverage
        ------------------
        case1: count < threshold  -> new ticket, moderate priority
        case2: count >= threshold -> new ticket, critical priority
        case3: existing moderate ticket + count crosses threshold -> update to critical
        case4/5: existing critical ticket  -> no update (suppress spam)
        """
        if not self._ensure_ready():
            return None

        desired_priority = self._map_priority(count)
        record = self._get_record(error_key)

        if record is None:
            # No incident yet — create one (case1, case2)
            return self._create_new(error_key, app_name, error_code, description, count,
                             llm_summary, desired_priority)

        # We have an existing record. Check if it is still active in the provider.
        if not self._provider.is_incident_active(record["incident_id"]):
            # Provider says ticket is closed — clean up and create fresh
            logger.info(
                f"[IncidentManager] Existing ticket {record['incident_display']} is closed. "
                f"Creating new one."
            )
            self._mark_closed(error_key)
            return self._create_new(error_key, app_name, error_code, description, count,
                             llm_summary, desired_priority)

        current_priority = record.get("priority", SNOW_PRIORITY_MODERATE)

        if current_priority == SNOW_PRIORITY_CRITICAL:
            # case4 / case5: Already high-priority, active ticket — do NOT spam
            logger.info(
                f"[IncidentManager] Ticket {record['incident_display']} already Critical "
                f"and active — no update sent (suppressed)."
            )
            return self._build_return_dict(record["incident_id"], record["incident_display"])

        if desired_priority == SNOW_PRIORITY_CRITICAL:
            # case3: Escalate from moderate to critical
            notes = (
                f"Opssolver Engine — Auto-escalation\n"
                f"Error '{error_code}' in '{app_name}' has now occurred {count}x, "
                f"crossing the threshold of {Config.HIGH_PRIORITY_THRESHOLD}.\n"
                f"Priority escalated to Critical.\n\n"
                f"Analysis:\n{llm_summary or 'N/A'}"
            )
            try:
                self._provider.update_incident(
                    record["incident_id"], notes, priority=SNOW_PRIORITY_CRITICAL
                )
                self._update_record_priority(error_key, SNOW_PRIORITY_CRITICAL)
                logger.warning(
                    f"[IncidentManager] Escalated ticket {record['incident_display']} to Critical."
                )
            except Exception as exc:
                logger.error(f"[IncidentManager] Failed to escalate incident: {exc}")
        else:
            # Moderate ticket still moderate — suppressed occurrence note based on user preference
            logger.info(
                f"[IncidentManager] Ticket {record['incident_display']} is moderate (count={count})."
            )
        
        return self._build_return_dict(record["incident_id"], record["incident_display"])

    def handle_high_priority(
        self,
        error_key: str,
        app_name: str,
        error_code: str,
        description: str,
        count: int,
        llm_summary: str = "",
    ) -> Optional[dict]:
        """
        Called by extractors when the error count crosses HIGH_PRIORITY_THRESHOLD
        within a single poll cycle (bypasses RabbitMQ — direct Critical ticket).

        Test case coverage
        ------------------
        case2: single-run batch crosses threshold -> create Critical ticket
        case5: subsequent run crosses threshold again -> no update if already Critical
        """
        if not self._ensure_ready():
            return None

        record = self._get_record(error_key)

        if record is None:
            # case2: Brand-new critical burst — create straight to Critical
            return self._create_new(
                error_key, app_name, error_code, description, count,
                llm_summary, SNOW_PRIORITY_CRITICAL
            )

        if not self._provider.is_incident_active(record["incident_id"]):
            self._mark_closed(error_key)
            return self._create_new(
                error_key, app_name, error_code, description, count,
                llm_summary, SNOW_PRIORITY_CRITICAL
            )

        # Ticket exists and is active
        if record.get("priority") == SNOW_PRIORITY_CRITICAL:
            # case5: Already Critical — suppress
            logger.info(
                f"[IncidentManager] High-priority ticket {record['incident_display']} "
                f"already Critical and active — no update sent (suppressed)."
            )
            return self._build_return_dict(record["incident_id"], record["incident_display"])

        # Existing Moderate ticket: escalate to Critical
        notes = (
            f"Opssolver Engine — HIGH-PRIORITY ESCALATION\n"
            f"Error '{error_code}' in '{app_name}' has surged to {count} occurrences "
            f"in the current poll cycle (threshold={Config.HIGH_PRIORITY_THRESHOLD}).\n"
            f"Escalating ticket to Critical immediately.\n\n"
            f"Description:\n{description[:500]}"
        )
        try:
            self._provider.update_incident(
                record["incident_id"], notes, priority=SNOW_PRIORITY_CRITICAL
            )
            self._update_record_priority(error_key, SNOW_PRIORITY_CRITICAL)
            logger.warning(
                f"[IncidentManager] Escalated {record['incident_display']} to Critical "
                f"via high-priority path."
            )
        except Exception as exc:
            logger.error(f"[IncidentManager] Failed to escalate via high-priority: {exc}")
            
        return self._build_return_dict(record["incident_id"], record["incident_display"])

    def resolve_ticket(self, error_key: str, resolution_notes: str) -> None:
        """
        Called when a verified solution is submitted via UI (ops_solution.py).
        1. Look up active ticket
        2. Resolve it in provider
        3. Delete local tracking so it triggers a fresh ticket next time
        """
        if not self._ensure_ready():
            return
            
        record = self._get_record(error_key)
        if not record:
            logger.info(f"[IncidentManager] Cannot resolve ticket for '{error_key}' — no active ticket tracked.")
            return
            
        incident_id = record["incident_id"]
        display_name = record["incident_display"]

        # Guard: don't try to resolve a ticket already closed in ServiceNow
        if not self._provider.is_incident_active(incident_id):
            logger.info(
                f"[IncidentManager] Ticket {display_name} is already closed in ServiceNow. "
                f"Cleaning up local record."
            )
            self._mark_closed(error_key)
            return

        try:
            self._provider.resolve_incident(incident_id, resolution_notes)
            self._mark_closed(error_key)
            logger.info(f"[IncidentManager] Successfully resolved ticket {display_name} for '{error_key}'.")
        except Exception as exc:
            logger.error(f"[IncidentManager] Failed to resolve ticket {display_name}: {exc}")

    # -- internal helpers ------------------------------------------------

    def _create_new(
        self,
        error_key: str,
        app_name: str,
        error_code: str,
        description: str,
        count: int,
        llm_summary: str,
        priority: int,
    ) -> Optional[dict]:
        title = f"{error_code} in {app_name}"
        body = (
            f"Application: {app_name}\n"
            f"Error Code:  {error_code}\n"
            f"Description:\n{description[:1000]}\n\n"
            f"Opssolver Analysis:\n{llm_summary or 'Pending analysis'}"
        )
        try:
            incident_id, incident_display = self._provider.create_incident(
                title, body, priority, work_notes=body
            )
            self._insert_record(error_key, self._provider_name, incident_id, incident_display, priority)
            logger.info(
                f"[IncidentManager] Created {incident_display} "
                f"(priority={priority}) for {error_key}"
            )
            return self._build_return_dict(incident_id, incident_display)
        except Exception as exc:
            logger.error(f"[IncidentManager] Failed to create incident: {exc}")
            return None

    def _build_return_dict(self, incident_id: str, incident_display: str) -> dict:
        url = getattr(Config, "SERVICENOW_INSTANCE_URL", "")
        if url:
            url = f"{url.rstrip('/')}/nav_to.do?uri=incident.do?sys_id={incident_id}"
        return {
            "incident_display": incident_display,
            "incident_url": url,
            "incident_id": incident_id
        }

    def _get_record(self, error_key: str) -> Optional[dict]:
        sql = "SELECT * FROM itsm_incidents WHERE error_key = %s LIMIT 1"
        if self._db:
            result = self._db.execute(sql, (error_key,), fetch=True)
            if result and len(result) > 0:
                return result[0]
        return None

    def _insert_record(
        self,
        error_key: str,
        provider_name: str,
        incident_id: str,
        incident_display: str,
        priority: int,
    ) -> None:
        sql = """
            INSERT INTO itsm_incidents (error_key, provider, incident_id, incident_display, priority)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (error_key) DO UPDATE SET
                incident_id = EXCLUDED.incident_id,
                incident_display = EXCLUDED.incident_display,
                priority = EXCLUDED.priority,
                updated_at = CURRENT_TIMESTAMP
        """
        if self._db:
            self._db.execute(sql, (error_key, provider_name, incident_id, incident_display, priority))

    def _update_record_priority(self, error_key: str, priority: int) -> None:
        sql = "UPDATE itsm_incidents SET priority = %s, updated_at = CURRENT_TIMESTAMP WHERE error_key = %s"
        if self._db:
            self._db.execute(sql, (priority, error_key))

    def _mark_closed(self, error_key: str) -> None:
        """Called when provider ticket is closed but error occurs again (triggering recreation)."""
        sql = "DELETE FROM itsm_incidents WHERE error_key = %s"
        if self._db:
            self._db.execute(sql, (error_key,))

