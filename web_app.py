from __future__ import annotations

import html
import os
import re
import sqlite3
import subprocess
import threading
import time
import unicodedata
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

WORKSPACE_DIR = Path(__file__).resolve().parent


def _load_dotenv(dotenv_path: Path) -> None:
  if not dotenv_path.exists() or not dotenv_path.is_file():
    return

  for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
      continue
    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip()
    if key and key not in os.environ:
      os.environ[key] = value


_load_dotenv(WORKSPACE_DIR / ".env")

app = FastAPI(title="OsiDOc Viewer", version="1.0.0")

_DOWNLOADS_ENV = os.getenv("OSI_DOWNLOAD_DIR", "downloads")
DOWNLOADS_DIR = Path(_DOWNLOADS_ENV)
if not DOWNLOADS_DIR.is_absolute():
    DOWNLOADS_DIR = (WORKSPACE_DIR / DOWNLOADS_DIR).resolve()

_DB_ENV = os.getenv("OSI_SQLITE_PATH", str(DOWNLOADS_DIR / "notificaciones.db"))
DB_PATH = Path(_DB_ENV)
if not DB_PATH.is_absolute():
    DB_PATH = (WORKSPACE_DIR / DB_PATH).resolve()

PAGE_SIZE_FIXED = 10
REMOTE_CHECK_STALE_MINUTES = int(os.getenv("OSI_REMOTE_CHECK_STALE_MINUTES", "10"))
COLUMNS_TO_DISPLAY = [
    "nro__notificacion",
    "asunto",
    "fecha_de_notificacion",
    "fecha_importacion",
]

UPDATE_STATE = {"running": False, "progress": 0, "error": None, "message": ""}


def _run_update():
    """Ejecuta osinergmin_auth.py en thread separado."""
    try:
        UPDATE_STATE["running"] = True
        UPDATE_STATE["error"] = None
        UPDATE_STATE["message"] = "Iniciando actualización incremental..."
        UPDATE_STATE["progress"] = 10

        script_path = WORKSPACE_DIR / "osinergmin_auth.py"
        result = subprocess.run(
            [
                str(WORKSPACE_DIR / ".venv" / "Scripts" / "python.exe"),
                str(script_path),
                "--incremental-only",
                "--skip-existing-notifications",
            ],
            cwd=str(WORKSPACE_DIR),
            capture_output=True,
            text=True,
            timeout=600,
        )

        combined_output = (result.stdout or "") + "\n" + (result.stderr or "")

        if result.returncode == 0:
            UPDATE_STATE["progress"] = 100
            if "No hay notificaciones nuevas o pendientes por descargar." in combined_output:
                UPDATE_STATE["message"] = "No hay nada nuevo para descargar."
            elif "No se descargaron documentos notificados." in combined_output:
                UPDATE_STATE["message"] = "No hubo documentos nuevos para descargar."
            else:
                UPDATE_STATE["message"] = "Actualización completada."
        else:
            UPDATE_STATE["error"] = f"Proceso finalizado con código {result.returncode}"
            UPDATE_STATE["message"] = "La actualización terminó con error."
    except Exception as e:
        UPDATE_STATE["error"] = str(e)
        UPDATE_STATE["progress"] = 0
        UPDATE_STATE["message"] = "Error durante la actualización."
    finally:
        UPDATE_STATE["running"] = False


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con


def _get_table_columns(con: sqlite3.Connection) -> list[str]:
    rows = con.execute("PRAGMA table_info(notificaciones)").fetchall()
    return [str(r["name"]) for r in rows]


def _find_notification_column(columns: list[str]) -> str | None:
    for col in columns:
        if "notif" in col.lower():
            return col
    return None


def _find_due_date_column(columns: list[str]) -> str | None:
  for col in columns:
    c = col.lower()
    if "venc" in c and "fecha" in c:
      return col
  for col in columns:
    if "venc" in col.lower():
      return col
  return None


def _normalize_text(value: str) -> str:
  normalized = unicodedata.normalize("NFD", value or "")
  without_accents = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
  return without_accents.lower().strip()


def _infer_document_type(text: str) -> str:
  t = _normalize_text(text)

  if "coactiva" in t or "cobranza" in t:
    return "Cobranza coactiva"
  if "resolucion" in t:
    return "Resolucion"
  if "requerimiento" in t:
    return "Requerimiento"
  if "oficio" in t:
    return "Oficio"
  if "informe" in t:
    return "Informe"
  return "No identificado"


def _notifications_with_files(base_dir: Path) -> set[str]:
    out: set[str] = set()
    pat = re.compile(r"\d{8,}-\d+")
    if not base_dir.exists():
        return out

    for folder in base_dir.rglob("*"):
        if not folder.is_dir() or not pat.fullmatch(folder.name):
            continue
        try:
            has_file = any(p.is_file() for p in folder.iterdir())
        except Exception:
            has_file = False
        if has_file:
            out.add(folder.name)
    return out


def _get_pending_notifications() -> list[str]:
    """Notificaciones en BD que aun no tienen archivos descargados."""
    if not DB_PATH.exists():
        return []

    with _connect() as con:
        columns = _get_table_columns(con)
        notif_col = _find_notification_column(columns)
        if not notif_col:
            return []

        q = f'SELECT DISTINCT "{notif_col}" FROM notificaciones WHERE "{notif_col}" IS NOT NULL AND TRIM("{notif_col}") <> ""'
        db_notifs = {str(r[0]).strip() for r in con.execute(q).fetchall() if str(r[0] or "").strip()}

    downloaded_notifs = _notifications_with_files(DOWNLOADS_DIR)
    return sorted(n for n in db_notifs if n not in downloaded_notifs)


def _minutes_since_last_sync() -> int | None:
    """Minutos desde la ultima modificacion de la BD local."""
    try:
        if not DB_PATH.exists():
            return None
        elapsed_seconds = max(0.0, time.time() - DB_PATH.stat().st_mtime)
        return int(elapsed_seconds // 60)
    except OSError:
        return None


def _remote_check_required() -> tuple[bool, int | None]:
    """Indica si conviene forzar una verificacion remota en SNE."""
    minutes = _minutes_since_last_sync()
    if minutes is None:
        return True, None
    return minutes >= REMOTE_CHECK_STALE_MINUTES, minutes


def _html_page(title: str, body: str) -> HTMLResponse:
    page = f"""<!doctype html>
<html lang=\"es\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #eff4fb;
      --card: #ffffff;
      --ink: #132236;
      --muted: #607188;
      --brand: #0057b8;
      --brand-2: #0284c7;
      --line: #d3dfec;
      --accent: #eef6ff;
      --success: #1f9d55;
      --shadow: 0 18px 42px rgba(11, 36, 66, 0.13);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Trebuchet MS", "Lucida Sans Unicode", "Lucida Grande", "Lucida Sans", Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 8% 5%, rgba(2, 132, 199, 0.14), transparent 40%),
        radial-gradient(circle at 95% 0%, rgba(0, 87, 184, 0.18), transparent 35%),
        linear-gradient(180deg, #f5f9ff 0%, var(--bg) 70%);
    }}
    .wrap {{ max-width: 1440px; margin: 0 auto; padding: 24px; }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }}
    .head {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 20px;
      background: linear-gradient(112deg, #003d88 0%, #0057b8 52%, #0284c7 100%);
      color: #fff;
      padding: 18px 22px;
      flex-wrap: wrap;
    }}
    .head > div:first-child {{ flex: 1; }}
    .head h1 {{ margin: 0; font-size: 24px; font-weight: 700; letter-spacing: 0.2px; }}
    .meta {{ font-size: 13px; opacity: 0.92; margin-top: 6px; }}
    .content {{ padding: 20px; }}
    .toolbar {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 14px; align-items: center; }}
    input[type=text] {{
      min-width: 260px;
      flex: 1;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 14px;
      transition: all 0.2s;
    }}
    input[type=text]:focus {{
      outline: none;
      border-color: var(--brand);
      box-shadow: 0 0 0 3px rgba(15, 90, 165, 0.1);
    }}
    button, .btn {{
      border: 0;
      border-radius: 10px;
      background: var(--brand);
      color: #fff;
      padding: 10px 15px;
      text-decoration: none;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: all 0.2s ease;
      white-space: nowrap;
    }}
    button:hover, .btn:hover {{
      background: #0d4a8f;
      transform: translateY(-1px);
      box-shadow: 0 4px 12px rgba(15, 90, 165, 0.25);
    }}
    button:active {{ transform: translateY(0); }}
    button:disabled {{
      opacity: 0.5;
      cursor: not-allowed;
      transform: none;
    }}
    .btn.secondary {{ background: #425f7f; }}
    .btn.secondary:hover {{ background: #2f4a67; }}
    .btn.refresh {{
      background: linear-gradient(110deg, #1f9d55, #3abf72);
      padding: 10px 18px;
      font-weight: 600;
    }}
    .btn.refresh:hover {{ background: linear-gradient(110deg, #188a4a, #2eab63); }}
    .spinner {{
      display: inline-block;
      width: 14px;
      height: 14px;
      border: 2px solid rgba(255,255,255,0.3);
      border-top-color: #fff;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }}
    @keyframes spin {{
      to {{ transform: rotate(360deg); }}
    }}
    .modal {{
      display: none;
      position: fixed;
      top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(0,0,0,0.5);
      z-index: 1000;
      align-items: center;
      justify-content: center;
      backdrop-filter: blur(4px);
    }}
    .modal.active {{ display: flex; }}
    .modal-content {{
      background: var(--card);
      border-radius: 16px;
      padding: 40px;
      text-align: center;
      box-shadow: 0 20px 60px rgba(0,0,0,0.3);
      max-width: 420px;
      animation: slideUp 0.3s ease;
    }}
    @keyframes slideUp {{
      from {{ opacity: 0; transform: translateY(20px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    .modal-content h2 {{ margin: 0 0 12px; color: var(--ink); font-size: 20px; }}
    .modal-spinner {{
      width: 52px; height: 52px;
      margin: 0 auto 24px;
      border: 3px solid var(--line);
      border-top-color: var(--brand);
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }}
    .modal-text {{ color: var(--muted); font-size: 14px; line-height: 1.5; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 11px 10px; text-align: left; vertical-align: top; }}
    th {{ background: var(--accent); position: sticky; top: 0; font-weight: 700; color: #0b3f76; z-index: 2; }}
    .table-wrap {{ max-height: 72vh; overflow: auto; border: 1px solid var(--line); border-radius: 12px; background: #fff; }}
    tr:hover {{ background: #fafbfc; }}
    .muted {{ color: var(--muted); font-size: 13px; }}
    .badge {{
      background: #eaf3ff;
      color: #0f5aa5;
      border: 1px solid #cde0f7;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 500;
      padding: 3px 9px;
      white-space: nowrap;
      display: inline-block;
    }}
    ul.file-list {{ margin: 12px 0 0; padding-left: 18px; }}
    .file-list li {{ margin: 8px 0; }}
    .file-list a {{ color: var(--brand); text-decoration: none; font-weight: 500; }}
    .file-list a:hover {{ text-decoration: underline; }}
    .info-box {{
      background: #edf5ff;
      border-left: 4px solid var(--brand);
      border-radius: 10px;
      padding: 12px 14px;
      margin-bottom: 12px;
      font-size: 13px;
      color: #0d4a8f;
    }}
    .kpi-row {{ display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 12px; }}
    .kpi {{
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
      padding: 10px 12px;
      min-width: 160px;
      box-shadow: 0 5px 16px rgba(11, 36, 66, 0.06);
    }}
    .kpi .k-label {{ font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.4px; }}
    .kpi .k-value {{ font-size: 18px; font-weight: 700; color: #0b3f76; margin-top: 2px; }}
    .pager {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }}
    .pager-group {{ display: inline-flex; gap: 8px; }}
    .page-chip {{
      border: 1px solid #b8d2ef;
      background: #eef6ff;
      color: #1b4f86;
      border-radius: 999px;
      padding: 7px 12px;
      font-size: 12px;
      font-weight: 700;
    }}
    .btn.disabled {{
      pointer-events: none;
      opacity: 0.45;
      box-shadow: none;
      transform: none;
    }}
    .accordion-row {{ display: none; background: #f8fbff; }}
    .accordion-row.open {{ display: table-row; }}
    .accordion-cell {{ padding: 14px 12px !important; }}
    .accordion-box {{
      border: 1px solid #d7e6f7;
      border-radius: 8px;
      background: #fff;
      padding: 10px 12px;
    }}
    .docs-empty {{ color: var(--muted); font-size: 13px; }}
    .docs-list {{ margin: 0; padding-left: 18px; }}
    .docs-list li {{ margin: 6px 0; }}
    .floating-alert {{
      position: fixed;
      right: 18px;
      bottom: 18px;
      z-index: 1200;
      min-width: 280px;
      max-width: 360px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #edf9f1;
      color: #1d7d46;
      box-shadow: 0 14px 30px rgba(10, 28, 46, 0.16);
      padding: 12px 14px;
      display: none;
    }}
    .floating-alert.show {{ display: block; }}
    .floating-alert.pending {{ background: #fff6e8; color: #9a5b00; border-color: #f2d39a; }}
    .floating-alert.check {{ background: #f6f2ff; color: #4d3b8f; border-color: #d8cbf7; }}
    .fa-title {{ font-weight: 700; margin-bottom: 6px; }}
    .fa-text {{ font-size: 13px; line-height: 1.4; }}
    .fa-actions {{ margin-top: 10px; text-align: right; }}
    .fa-actions .btn {{ padding: 8px 10px; font-size: 13px; }}
  </style>
</head>
<body>
  <div class=\"wrap\">{body}</div>
  <div id=\"updateModal\" class=\"modal\">
    <div class=\"modal-content\">
      <h2>Actualizando notificaciones...</h2>
      <div class=\"modal-spinner\"></div>
      <p class=\"modal-text\" id=\"updateProgress\">Conectando con el servidor...</p>
    </div>
  </div>
  <div id="floatingAlert" class="floating-alert" aria-live="polite">
    <div class="fa-title" id="floatingAlertTitle">Estado de descargas</div>
    <div class="fa-text" id="floatingAlertText">Verificando pendientes...</div>
    <div class="fa-actions">
      <button id="floatingUpdateBtn" type="button" class="btn refresh" onclick="abrirActualizacion(event)">Actualizar</button>
    </div>
  </div>
  <script>
    async function abrirActualizacion(evt) {{
      const modal = document.getElementById('updateModal');
      const btn = (evt && evt.target && evt.target.closest('button'))
        ? evt.target.closest('button')
        : document.querySelector('.btn.refresh');
      if (btn) btn.disabled = true;
      modal.classList.add('active');
      
      try {{
        const resp = await fetch('/api/actualizar', {{ method: 'POST' }});
        const data = await resp.json();
        
        if(data.success) {{
          const checkInterval = setInterval(async () => {{
            const status = await fetch('/api/estado').then(r => r.json());
            document.getElementById('updateProgress').textContent = `Descargando... ${{Math.min(status.progress || 0, 99)}}%`;
            
            if(!status.running) {{
              clearInterval(checkInterval);
              document.getElementById('updateProgress').textContent = status.message || '¡Completado! Recargando...';
              setTimeout(() => {{
                modal.classList.remove('active');
                if (btn) btn.disabled = false;
                location.reload();
              }}, 1500);
            }}
          }}, 800);
        }} else {{
          alert('Error: ' + (data.error || 'Desconocido'));
          modal.classList.remove('active');
          if (btn) btn.disabled = false;
        }}
      }} catch(err) {{
        alert('Error de conexión: ' + err);
        modal.classList.remove('active');
        if (btn) btn.disabled = false;
      }}
    }}

    async function refreshFloatingAlert() {{
      const box = document.getElementById('floatingAlert');
      const title = document.getElementById('floatingAlertTitle');
      const text = document.getElementById('floatingAlertText');
      const btn = document.getElementById('floatingUpdateBtn');
      if (!box || !title || !text || !btn) return;

      try {{
        const res = await fetch('/api/pending');
        const data = await res.json();
        const pending = Number(data.pending_count || 0);
        const needsCheck = Boolean(data.needs_remote_check);
        const minutesSinceSync = data.minutes_since_last_sync;
        box.classList.add('show');
        box.classList.remove('check');

        if (pending > 0) {{
          box.classList.add('pending');
          title.textContent = 'Hay novedades';
          text.textContent = `Tienes ${{pending}} notificación(es) pendiente(s) por descargar.`;
          btn.style.display = 'inline-flex';
        }} else if (needsCheck) {{
          box.classList.remove('pending');
          box.classList.add('check');
          title.textContent = 'Revisión recomendada';
          if (typeof minutesSinceSync === 'number') {{
            text.textContent = `La última sincronización fue hace ${{minutesSinceSync}} min. Presiona "Actualizar" para verificar si hay nuevas notificaciones en SNE.`;
          }} else {{
            text.textContent = 'No hay historial local de sincronización. Presiona "Actualizar" para verificar nuevas notificaciones en SNE.';
          }}
          btn.style.display = 'inline-flex';
        }} else {{
          box.classList.remove('pending');
          title.textContent = 'Todo al día';
          text.textContent = 'No hay pendientes por descargar en este momento.';
          btn.style.display = 'none';
        }}
      }} catch (e) {{
        box.classList.add('show');
        box.classList.remove('pending');
        box.classList.remove('check');
        title.textContent = 'Estado';
        text.textContent = 'No se pudo verificar pendientes ahora.';
        btn.style.display = 'none';
      }}
    }}

    refreshFloatingAlert();
    setInterval(refreshFloatingAlert, 20000);

    async function toggleDocs(btn, numero, rowId) {{
      const row = document.getElementById(`docs-row-${{rowId}}`);
      const body = document.getElementById(`docs-body-${{rowId}}`);
      if (!row || !body) return;

      if (row.classList.contains('open')) {{
        row.classList.remove('open');
        btn.textContent = 'Ver documentos';
        return;
      }}

      row.classList.add('open');
      btn.textContent = 'Ocultar documentos';

      if (body.dataset.loaded === '1') return;
      body.innerHTML = '<div class="docs-empty">Cargando documentos...</div>';

      try {{
        const resp = await fetch(`/api/notificaciones/${{encodeURIComponent(numero)}}/documentos`);
        const data = await resp.json();
        if (!data || !Array.isArray(data.files) || data.files.length === 0) {{
          body.innerHTML = '<div class="docs-empty">No hay documentos para esta notificación.</div>';
          body.dataset.loaded = '1';
          return;
        }}

        const items = data.files.map((f) => (
          `<li><a href="${{f.href}}" target="_blank"><strong>${{f.name}}</strong></a> ` +
          `<span class="muted">(${{f.date_folder}} | ${{f.size_kb}} KB)</span></li>`
        )).join('');
        body.innerHTML = `<ul class="docs-list">${{items}}</ul>`;
        body.dataset.loaded = '1';
      }} catch (err) {{
        body.innerHTML = '<div class="docs-empty">No se pudo cargar el detalle de documentos.</div>';
      }}
    }}
  </script>
</body>
</html>"""
    return HTMLResponse(content=page)


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "db": str(DB_PATH),
        "downloads": str(DOWNLOADS_DIR),
    }


@app.post("/api/actualizar")
def actualizar():
    """Inicia la actualización en background."""
    if UPDATE_STATE["running"]:
        return JSONResponse({"success": False, "error": "Ya se está ejecutando una actualización"}, status_code=400)

    UPDATE_STATE["progress"] = 5
    thread = threading.Thread(target=_run_update, daemon=True)
    thread.start()
    return JSONResponse({"success": True, "message": "Actualización iniciada"})


@app.get("/api/estado")
def estado():
    """Devuelve el estado actual de la actualización."""
    return JSONResponse({
        "running": UPDATE_STATE["running"],
        "progress": UPDATE_STATE["progress"],
        "error": UPDATE_STATE["error"],
        "message": UPDATE_STATE["message"],
    })


@app.get("/api/pending")
def pending_status():
    pending = _get_pending_notifications()
    needs_remote_check, minutes_since_last_sync = _remote_check_required()
    return JSONResponse({
        "pending_count": len(pending),
        "pending_notifications": pending,
        "needs_remote_check": needs_remote_check,
        "minutes_since_last_sync": minutes_since_last_sync,
        "stale_after_minutes": REMOTE_CHECK_STALE_MINUTES,
    })


@app.get("/", response_class=HTMLResponse)
def index(
  q: str = Query(default="", description="Texto para buscar"),
  page: int = Query(default=1, ge=1),
) -> HTMLResponse:
    if not DB_PATH.exists():
        return _html_page(
            "OsiDOc Viewer",
            """
<div class="card">
  <div class="head">
    <div><h1>OsiDOc Viewer</h1><div class="meta">Base de datos no encontrada</div></div>
    <button class="btn refresh" onclick="abrirActualizacion()"><span class="spinner"></span>Actualizar</button>
  </div>
  <div class="content">
    <div class="info-box">No existe la base de datos SQLite. Primero ejecuta <code>osinergmin_auth.py</code> para generar la data.</div>
  </div>
</div>
""",
        )

    with _connect() as con:
        columns = _get_table_columns(con)
        if not columns:
            return _html_page(
                "OsiDOc Viewer",
                """
<div class="card">
  <div class="head"><h1>OsiDOc Viewer</h1><span class="badge">Sin tabla</span></div>
  <div class="content">No se encontró la tabla notificaciones.</div>
</div>
""",
            )

        where_clause = ""
        params: list[str | int] = []
        if q.strip():
            where_parts = [f'CAST("{c}" AS TEXT) LIKE ?' for c in columns]
            where_clause = " WHERE " + " OR ".join(where_parts)
            params.extend([f"%{q.strip()}%"] * len(columns))

        total = int(con.execute(f"SELECT COUNT(*) FROM notificaciones{where_clause}", params).fetchone()[0])
        page_size = PAGE_SIZE_FIXED
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = min(page, total_pages)
        offset = (page - 1) * page_size
        query = f"SELECT rowid, * FROM notificaciones{where_clause} ORDER BY rowid DESC LIMIT ? OFFSET ?"
        rows = con.execute(query, params + [page_size, offset]).fetchall()

    notif_col = _find_notification_column(columns)
    due_col = _find_due_date_column(columns)
    asunto_col = next((c for c in columns if c.lower() == "asunto"), None)

    pending_notifs = _get_pending_notifications()
    pending_count = len(pending_notifs)
    needs_remote_check, minutes_since_last_sync = _remote_check_required()

    display_cols = [c for c in columns if c.lower() in [dc.lower() for dc in COLUMNS_TO_DISPLAY]]
    head = "".join(f"<th>{html.escape(c)}</th>" for c in display_cols)
    head += "<th>Fecha de vencimiento</th><th>Tipo de documento</th><th>Documentos</th>"
    body_rows: list[str] = []
    for row in rows:
        tds = []
        for c in display_cols:
            val = str(row[c] if row[c] is not None else "")
            tds.append(f"<td>{html.escape(val[:50])}</td>" if len(val) > 50 else f"<td>{html.escape(val)}</td>")

        due_value = ""
        if due_col and row[due_col] is not None:
          due_value = str(row[due_col]).strip()
        tds.append(f"<td>{html.escape(due_value) if due_value else '-'}</td>")

        source_text = ""
        if asunto_col and row[asunto_col] is not None:
          source_text = str(row[asunto_col])
        elif notif_col and row[notif_col] is not None:
          source_text = str(row[notif_col])
        doc_type = _infer_document_type(source_text)
        tds.append(f"<td>{html.escape(doc_type)}</td>")

        notif = str(row[notif_col]) if notif_col and row[notif_col] else ""
        row_id = int(row["rowid"])
        if notif:
            docs_link = (
                f"<button type='button' class='btn secondary' "
                f"onclick=\"toggleDocs(this, '{html.escape(notif)}', {row_id})\">Ver documentos</button>"
            )
        else:
            docs_link = "<span class='muted'>Sin Nro.</span>"
        tds.append(f"<td>{docs_link}</td>")

        body_rows.append("<tr>" + "".join(tds) + "</tr>")
        body_rows.append(
            "<tr id='docs-row-{}' class='accordion-row'><td colspan='{}' class='accordion-cell'>"
            "<div id='docs-body-{}' class='accordion-box' data-loaded='0'></div>"
            "</td></tr>".format(row_id, len(display_cols) + 1, row_id)
        )

    prev_page = max(1, page - 1)
    total_pages = max(1, (total + page_size - 1) // page_size)
    next_page = min(total_pages, page + 1)
    start_row = (page - 1) * page_size + 1 if total > 0 else 0
    end_row = min(page * page_size, total)

    body = f"""
<div class="card">
  <div class="head">
    <div>
      <h1>OsiDOc Viewer</h1>
      <div class="meta">📊 {total} registros en BD</div>
    </div>
    <button class="btn refresh" onclick="abrirActualizacion()"><span class="spinner"></span>Actualizar</button>
  </div>
  <div class="content">
    <form class="toolbar" method="get" action="/">
      <input type="text" name="q" value="{html.escape(q)}" placeholder="🔍 Buscar en cualquier columna..." />
      <button type="submit">Buscar</button>
      <a class="btn secondary" href="/">Limpiar</a>
    </form>
    <div class="kpi-row">
      <div class="kpi"><div class="k-label">Total Registros</div><div class="k-value">{total}</div></div>
      <div class="kpi"><div class="k-label">Página Actual</div><div class="k-value">{page}/{total_pages}</div></div>
      <div class="kpi"><div class="k-label">Bloque</div><div class="k-value">{start_row}-{end_row}</div></div>
      <div class="kpi"><div class="k-label">Tamaño Página</div><div class="k-value">10</div></div>
    </div>
    <div class="info-box">
      📄 Mostrando {start_row} a {end_row} de {total} registros (siempre 10 por página).
    </div>
    <div class="info-box" style="border-left-color: {'#e67e22' if (pending_count > 0 or needs_remote_check) else '#27ae60'}; background: {'#fff6e8' if (pending_count > 0 or needs_remote_check) else '#edf9f1'}; color: {'#9a5b00' if (pending_count > 0 or needs_remote_check) else '#1d7d46'};">
      {'⚠️ Hay ' + str(pending_count) + ' notificación(es) pendiente(s) por descargar. Presiona "Actualizar".' if pending_count > 0 else ('⚠️ La última sincronización local está desactualizada' + (' (hace ' + str(minutes_since_last_sync) + ' min)' if minutes_since_last_sync is not None else '') + '. Presiona "Actualizar" para verificar nuevas notificaciones en SNE.' if needs_remote_check else '✅ No hay pendientes por descargar en este momento.')}
    </div>
    <div class="pager">
      <div class="pager-group">
        <a class="btn secondary {'disabled' if page <= 1 else ''}" href="/?q={quote(q)}&page={prev_page}">← Anterior</a>
        <a class="btn secondary {'disabled' if page >= total_pages else ''}" href="/?q={quote(q)}&page={next_page}">Siguiente →</a>
      </div>
      <span class="page-chip">Página {page} de {total_pages}</span>
    </div>
    <div class="table-wrap">
      <table>
        <thead><tr>{head}</tr></thead>
        <tbody>{''.join(body_rows) if body_rows else '<tr><td colspan="999" style="text-align:center; color:#999;">Sin resultados</td></tr>'}</tbody>
      </table>
    </div>
  </div>
</div>
"""
    return _html_page("OsiDOc Viewer", body)


@app.get("/notificaciones/{numero}/documentos", response_class=HTMLResponse)
def documentos(numero: str) -> HTMLResponse:
    docs = sorted(DOWNLOADS_DIR.glob(f"*/{numero}/*"), key=lambda p: p.name.lower())

    if not docs:
        body = f"""
<div class="card">
  <div class="head"><h1>Documentos de {html.escape(numero)}</h1><span class="badge">0 archivos</span></div>
  <div class="content">
    <p>No se encontraron archivos para este número.</p>
    <a class="btn secondary" href="/">Volver al listado</a>
  </div>
</div>
"""
        return _html_page(f"Documentos {numero}", body)

    items: list[str] = []
    for path in docs:
        relative = path.relative_to(DOWNLOADS_DIR).as_posix()
        href = f"/files/{quote(relative)}"
        date_folder = path.parents[1].name if len(path.parents) > 1 else ""
        items.append(
            f"<li><a href='{href}' target='_blank'><strong>{html.escape(path.name)}</strong></a> "
            f"<span class='muted'>({html.escape(date_folder)})</span></li>"
        )

    body = f"""
<div class="card">
  <div class="head"><h1>📄 Documentos de {html.escape(numero)}</h1><span class="badge">{len(docs)} archivos</span></div>
  <div class="content">
    <a class="btn secondary" href="/">← Volver al listado</a>
    <ul class="file-list">{''.join(items)}</ul>
  </div>
</div>
"""
    return _html_page(f"Documentos {numero}", body)


@app.get("/api/notificaciones/{numero}/documentos")
def documentos_api(numero: str):
    docs = sorted(DOWNLOADS_DIR.glob(f"*/{numero}/*"), key=lambda p: p.name.lower())
    files: list[dict[str, str]] = []
    for path in docs:
        relative = path.relative_to(DOWNLOADS_DIR).as_posix()
        href = f"/files/{quote(relative)}"
        date_folder = path.parents[1].name if len(path.parents) > 1 else ""
        try:
            size_kb = f"{path.stat().st_size / 1024:.2f}"
        except OSError:
            size_kb = "0.00"
        files.append(
            {
                "name": path.name,
                "href": href,
                "date_folder": date_folder,
                "size_kb": size_kb,
            }
        )

    return JSONResponse({"numero": numero, "files": files})


@app.get("/files/{file_path:path}")
def serve_file(file_path: str):
    candidate = (DOWNLOADS_DIR / file_path).resolve()

    if DOWNLOADS_DIR not in candidate.parents and candidate != DOWNLOADS_DIR:
        raise HTTPException(status_code=400, detail="Ruta invalida")

    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    return FileResponse(
        str(candidate),
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename=\"{quote(candidate.name)}\""}
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("web_app:app", host="127.0.0.1", port=8000, reload=False)
