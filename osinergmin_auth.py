from __future__ import annotations

import argparse
import logging
import os
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
except Exception:
    webdriver = None
    TimeoutException = Exception
    ActionChains = None
    By = None
    Keys = None
    EC = None
    WebDriverWait = None


@dataclass
class AuthConfig:
    login_url: str = "https://osivirtual.osinergmin.gob.pe/autenticacion/acceso-sistema"
    base_url: str = "https://osivirtual.osinergmin.gob.pe"
    username_id: str = "documentoIdentidad"
    password_id: str = "contrasena"
    login_form_selector: str = "form"
    login_submit_selector: str = "button[type='submit']"
    post_login_selector: str = ""
    login_error_selector: str = ""
    success_url_keyword: str = "autenticacion/acceso-sistema"
    selenium_headless: bool = True
    timeout: int = 30
    captcha_timeout: int = 180
    open_sne_after_login: bool = True
    require_sne_click_navigation: bool = True
    sne_menu_selector: str = (
        "//mat-nav-list[@role='navigation']"
        "//mat-list-item[.//div[@matlistitemtitle and contains(@class,'mat-mdc-list-item-title') "
        "and contains(@class,'text-menu-parent') and normalize-space()='Casilla Electrónica del SNE']]"
        "//div[@matlistitemtitle and contains(@class,'mat-mdc-list-item-title') "
        "and contains(@class,'text-menu-parent') and normalize-space()='Casilla Electrónica del SNE']"
    )
    sne_target_url: str = "https://notificaciones.osinergmin.gob.pe/sne-web/pages/notificacion/inicio"
    sne_expected_text: str = "Sistema de Notificaciones Electrónicas|Bandeja de Entrada"
    fecha_notificacion_inicio: str = ""
    fecha_notificacion_fin: str = ""
    sne_fecha_inicio_id: str = "fechaNotificacionInicio"
    sne_fecha_fin_id: str = "fechaNotificacionFin"
    sne_buscar_button_id: str = "buscar-boton"
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "si", "on"}


def _load_dotenv(dotenv_path: str = ".env") -> None:
    """Carga variables de entorno desde un archivo .env sin dependencias externas."""
    path = Path(dotenv_path)
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and os.getenv(key) is None:
            os.environ[key] = value


def _extract_hidden_inputs(form) -> dict[str, str]:
    payload: dict[str, str] = {}
    if not form:
        return payload

    for hidden in form.select("input[type='hidden']"):
        name = hidden.get("name")
        value = hidden.get("value", "")
        if name:
            payload[name] = value

    return payload


def _is_authenticated_by_url(url: str, success_url_keyword: str) -> bool:
    # Si seguimos en acceso-sistema, asumimos que no se autenticó.
    return success_url_keyword.lower() not in url.lower()


def _resolve_input_name_by_id(form, input_id: str, fallback_name: str) -> str:
    if not form:
        return fallback_name

    input_el = form.select_one(f"#{input_id}")
    if not input_el:
        return fallback_name

    return input_el.get("name") or fallback_name


def _login_with_requests(session: requests.Session, cfg: AuthConfig, username: str, password: str) -> bool:
    try:
        login_page = session.get(cfg.login_url, timeout=cfg.timeout)
        login_page.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Error de conexion al abrir login: {exc}") from exc

    soup = BeautifulSoup(login_page.text, "html.parser")
    form = soup.select_one(cfg.login_form_selector)
    if not form:
        return False

    payload = _extract_hidden_inputs(form)
    username_key = _resolve_input_name_by_id(form, cfg.username_id, cfg.username_id)
    password_key = _resolve_input_name_by_id(form, cfg.password_id, cfg.password_id)
    payload[username_key] = username
    payload[password_key] = password

    post_url = cfg.login_url
    if form.get("action"):
        post_url = urljoin(cfg.base_url, form.get("action"))

    try:
        response = session.post(post_url, data=payload, timeout=cfg.timeout, allow_redirects=True)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Error al enviar credenciales: {exc}") from exc

    final_url = response.url or cfg.login_url
    if _is_authenticated_by_url(final_url, cfg.success_url_keyword):
        return True

    # Verificacion adicional navegando nuevamente a login.
    check = session.get(cfg.login_url, timeout=cfg.timeout, allow_redirects=True)
    check.raise_for_status()
    return _is_authenticated_by_url(check.url, cfg.success_url_keyword)


def _new_driver(headless: bool):
    if webdriver is None:
        raise RuntimeError("Selenium no esta disponible en el entorno.")

    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)


def _find_first(driver, selectors: list[tuple[str, str]], timeout: int):
    for by, value in selectors:
        try:
            return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, value)))
        except Exception:
            continue
    return None


def _find_ingresar_button(driver, cfg: AuthConfig, timeout: int):
    # Prioriza boton/input cuyo texto o value sea exactamente 'Ingresar'.
    return _find_first(
        driver,
        [
            (By.XPATH, "//button[contains(translate(normalize-space(.), 'INGRESAR', 'ingresar'), 'ingresar') ]"),
            (By.XPATH, "//button[contains(@class,'bg-primary') and contains(translate(normalize-space(.), 'INGRESAR', 'ingresar'), 'ingresar') ]"),
            (By.CSS_SELECTOR, "input[type='submit'][value='Ingresar']"),
            (By.XPATH, "//button[normalize-space()='Ingresar']"),
            (By.XPATH, "//input[@type='submit' and @value='Ingresar']"),
            (By.CSS_SELECTOR, cfg.login_submit_selector),
            (By.CSS_SELECTOR, "button[type='submit']"),
            (By.CSS_SELECTOR, "button"),
            (By.CSS_SELECTOR, "input[type='submit']"),
        ],
        timeout,
    )


def _click_ingresar_fallback(driver, password_id: str) -> bool:
    """Fallback: clic por texto 'Ingresar' via JS o envio con Enter."""
    script = """
const pwdId = arguments[0];
const nodes = Array.from(document.querySelectorAll('button, input[type="submit"]'));
const btn = nodes.find((n) => {
    const raw = (n.innerText || n.textContent || n.value || '').trim().toLowerCase();
    return raw.includes('ingresar');
});

if (btn) {
    btn.click();
    return true;
}

const pwd = document.getElementById(pwdId);
if (pwd) {
    pwd.focus();
    const ev = new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true });
    pwd.dispatchEvent(ev);
    return true;
}

return false;
"""
    try:
        return bool(driver.execute_script(script, password_id))
    except Exception:
        return False


def _click_ingresar_button(driver, submit) -> bool:
    """Hace clic explícito en Ingresar con múltiples estrategias."""
    if submit is None:
        return False

    # Lleva el botón al viewport antes del clic.
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit)
    except Exception:
        pass

    # Estrategia 1: click directo Selenium.
    try:
        submit.click()
        return True
    except Exception:
        pass

    # Estrategia 2: ActionChains (algunos overlays lo requieren).
    try:
        if ActionChains is not None:
            ActionChains(driver).move_to_element(submit).click().perform()
            return True
    except Exception:
        pass

    # Estrategia 3: disparo explícito de eventos de mouse + click JS.
    try:
        driver.execute_script(
            """
const el = arguments[0];
['mouseenter','mouseover','mousemove','mousedown','mouseup','click'].forEach((type) => {
  const evt = new MouseEvent(type, { bubbles: true, cancelable: true, view: window });
  el.dispatchEvent(evt);
});
""",
            submit,
        )
        return True
    except Exception:
        return False


def _click_ingresar_with_retries(driver, cfg: AuthConfig, retries: int = 6, delay_seconds: float = 1.0) -> bool:
    """Intenta hacer clic en Ingresar varias veces por cambios dinámicos del DOM."""
    for intento in range(1, retries + 1):
        submit = _find_ingresar_button(driver, cfg, 2)
        if submit is not None:
            logging.info("Intento %s: clic en boton Ingresar por selector.", intento)
            if _click_ingresar_button(driver, submit):
                return True

        logging.info("Intento %s: fallback JS/Enter para enviar login.", intento)
        if _click_ingresar_fallback(driver, cfg.password_id):
            return True

        time.sleep(delay_seconds)

    return False


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _wait_for_sne_home(driver, cfg: AuthConfig) -> bool:
    """Valida que estamos en la pagina SNE esperada por URL y texto visible."""
    expected_tokens = [_normalize_text(part.strip()) for part in cfg.sne_expected_text.split("|") if part.strip()]
    expected_url = _normalize_text(cfg.sne_target_url)

    def _condition(drv) -> bool:
        current_url = _normalize_text(drv.current_url or "")

        # Se exige estar en el dominio/ruta objetivo del SNE.
        if expected_url and expected_url not in current_url:
            return False

        try:
            page_text = drv.execute_script(
                "return (document.body && document.body.innerText) ? document.body.innerText : '';"
            )
        except Exception:
            page_text = drv.page_source or ""

        page = _normalize_text(page_text)

        # Se valida el contenido principal que aparece en la vista esperada.
        if expected_tokens and not all(token in page for token in expected_tokens):
            return False

        return True

    try:
        WebDriverWait(driver, cfg.timeout).until(_condition)
        return True
    except TimeoutException:
        return False


def _set_input_value(driver, element, value: str) -> None:
    try:
        element.clear()
    except Exception:
        pass

    try:
        element.send_keys(Keys.CONTROL, "a")
        element.send_keys(Keys.DELETE)
    except Exception:
        pass

    try:
        element.send_keys(value)
    except Exception:
        driver.execute_script(
            "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input', {bubbles: true})); arguments[0].dispatchEvent(new Event('change', {bubbles: true}));",
            element,
            value,
        )


def _apply_sne_filters(driver, cfg: AuthConfig) -> bool:
    """Llena fechas y ejecuta la busqueda en la bandeja del SNE."""
    if not cfg.fecha_notificacion_inicio and not cfg.fecha_notificacion_fin:
        return True

    if not cfg.fecha_notificacion_inicio or not cfg.fecha_notificacion_fin:
        logging.warning("Se omite la busqueda en SNE porque falta una de las dos fechas requeridas.")
        return False

    inicio = _find_first(driver, [(By.ID, cfg.sne_fecha_inicio_id)], cfg.timeout)
    fin = _find_first(driver, [(By.ID, cfg.sne_fecha_fin_id)], cfg.timeout)
    buscar = _find_first(
        driver,
        [
            (By.ID, cfg.sne_buscar_button_id),
            (By.XPATH, "//input[@type='button' and @id='buscar-boton']"),
            (By.XPATH, "//input[@type='button' and @value='Buscar']"),
        ],
        cfg.timeout,
    )

    if inicio is None or fin is None or buscar is None:
        logging.warning("No se encontraron los campos de fecha o el boton Buscar en la bandeja del SNE.")
        return False

    _set_input_value(driver, inicio, cfg.fecha_notificacion_inicio)
    _set_input_value(driver, fin, cfg.fecha_notificacion_fin)
    logging.info(
        "Fechas de notificacion cargadas en SNE: inicio=%s fin=%s",
        cfg.fecha_notificacion_inicio,
        cfg.fecha_notificacion_fin,
    )

    if not _click_ingresar_button(driver, buscar):
        try:
            driver.execute_script("arguments[0].click();", buscar)
        except Exception:
            logging.warning("No se pudo hacer clic en el boton Buscar del SNE.")
            return False

    logging.info("Busqueda ejecutada en la bandeja del SNE.")
    return True


def _resolve_sne_menu_targets(driver, menu):
    if menu is None:
        return None, None

    try:
        container = driver.execute_script(
            """
const el = arguments[0];
if (!el) return null;
return el.closest('mat-list-item, .mat-mdc-list-item, .mdc-list-item, [role="listitem"]') || el;
""",
            menu,
        )
    except Exception:
        container = menu

    try:
        title = driver.execute_script(
            """
const el = arguments[0];
if (!el) return null;
return el.querySelector('.mat-mdc-list-item-title, .text-menu-parent') || el;
""",
            container,
        )
    except Exception:
        title = menu

    return container, title


def _find_sne_menu(driver, cfg: AuthConfig):
    return _find_first(
        driver,
        [
            (By.XPATH, cfg.sne_menu_selector),
            (
                By.XPATH,
                "//mat-nav-list[@role='navigation']//mat-list-item"
                "[.//div[@matlistitemtitle and contains(@class,'mat-mdc-list-item-title') "
                "and contains(@class,'text-menu-parent') and normalize-space()='Casilla Electrónica del SNE']]",
            ),
            (
                By.XPATH,
                "//mat-nav-list[@role='navigation']//div[@matlistitemtitle "
                "and contains(@class,'mat-mdc-list-item-title') "
                "and contains(@class,'text-menu-parent') "
                "and normalize-space()='Casilla Electrónica del SNE']",
            ),
            (
                By.XPATH,
                "//mat-list-item[.//mat-icon[@data-mat-icon-name='ico-mail'] "
                "and .//div[normalize-space()='Casilla Electrónica del SNE']]",
            ),
            (
                By.XPATH,
                "//div[contains(@class, 'mat-mdc-list-item-title') "
                "and contains(normalize-space(.), 'Casilla Electrónica del SNE')]",
            ),
            (
                By.XPATH,
                "//*[contains(normalize-space(.), 'Casilla Electrónica del SNE') "
                "and (self::div or self::span or self::a)]",
            ),
        ],
        cfg.timeout,
    )


def _perform_sne_click_attempt(driver, target, description: str, mode: str) -> bool:
    if target is None:
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target)
    except Exception:
        pass

    try:
        if mode == "selenium":
            if _click_ingresar_button(driver, target):
                logging.info("Clic real ejecutado sobre %s.", description)
                return True
        elif mode == "js-click":
            clicked = driver.execute_script(
                """
const el = arguments[0];
if (!el) return false;
el.click();
return true;
""",
                target,
            )
            if clicked:
                logging.info("Clic JS ejecutado sobre %s.", description)
                return True
        elif mode == "js-events":
            clicked = driver.execute_script(
                """
const el = arguments[0];
if (!el) return false;
['pointerdown','mousedown','pointerup','mouseup','click'].forEach((type) => {
  el.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window }));
});
return true;
""",
                target,
            )
            if clicked:
                logging.info("Eventos de click disparados sobre %s.", description)
                return True
        elif mode == "keyboard" and Keys is not None:
            target.send_keys(Keys.ENTER)
            logging.info("Enter enviado sobre %s.", description)
            return True
    except Exception:
        return False

    return False


def _switch_to_sne_window_if_any(driver, cfg: AuthConfig) -> bool:
    """Busca entre todas las ventanas la que pertenece al SNE y cambia el foco."""
    target_host = "notificaciones.osinergmin.gob.pe"
    expected_url = _normalize_text(cfg.sne_target_url)

    for handle in driver.window_handles:
        try:
            driver.switch_to.window(handle)
            current_url = driver.current_url or ""
        except Exception:
            continue

        current_url_norm = _normalize_text(current_url)
        if target_host in current_url.lower() or (expected_url and expected_url in current_url_norm):
            return True

    return False


def _wait_for_sne_navigation_after_click(driver, cfg: AuthConfig, windows_before: list[str], url_before: str) -> bool:
    """Confirma que el clic produjo navegacion real al SNE."""
    end_time = time.time() + cfg.timeout
    while time.time() < end_time:
        windows_now = driver.window_handles

        if len(windows_now) > len(windows_before) and _switch_to_sne_window_if_any(driver, cfg):
            return True

        current_url = driver.current_url or ""
        if _normalize_text(cfg.sne_target_url) in _normalize_text(current_url):
            return True

        if current_url and current_url != url_before and "notificaciones.osinergmin.gob.pe" in current_url.lower():
            return True

        time.sleep(0.25)

    return False


def _attempt_sne_click_navigation(driver, menu, cfg: AuthConfig, windows_before: list[str], url_before: str) -> bool:
    container, title = _resolve_sne_menu_targets(driver, menu)
    attempts = [
        (title, "el titulo de Casilla Electrónica del SNE", "selenium"),
        (container, "el contenedor del menu SNE", "selenium"),
        (title, "el titulo de Casilla Electrónica del SNE", "js-click"),
        (container, "el contenedor del menu SNE", "js-click"),
        (container, "el contenedor del menu SNE", "js-events"),
        (container, "el contenedor del menu SNE", "keyboard"),
    ]

    for target, description, mode in attempts:
        if not _perform_sne_click_attempt(driver, target, description, mode):
            continue

        if _wait_for_sne_navigation_after_click(driver, cfg, windows_before, url_before):
            return True

    return False


def _click_sne_menu_and_switch_window(driver, cfg: AuthConfig) -> bool:
    """Hace clic en Casilla Electrónica del SNE y cambia a la ventana nueva si se abre."""
    windows_before = driver.window_handles
    url_before = driver.current_url or ""

    try:
        menu = _find_sne_menu(driver, cfg)
    except Exception:
        menu = None

    if menu is None:
        logging.warning("No se encontro el menu 'Casilla Electrónica del SNE'.")
        return False

    clicked = _attempt_sne_click_navigation(driver, menu, cfg, windows_before, url_before)
    if not clicked:
        logging.warning("No se pudo hacer clic en el menu 'Casilla Electrónica del SNE'.")
        return False

    if cfg.sne_target_url:
        logging.info("Clic automatico realizado en Casilla Electrónica del SNE; abriendo el link objetivo del SNE.")
        try:
            driver.get(cfg.sne_target_url)
        except Exception as exc:
            logging.warning("No se pudo abrir la URL objetivo del SNE: %s", exc)
            return False

    if _wait_for_sne_home(driver, cfg):
        logging.info("El clic abrio el SNE en la URL esperada y con el contenido de bandeja.")
    else:
        logging.warning("El clic abrio una vista relacionada, pero no se pudo validar la pantalla esperada del SNE.")
    return True


def _element_exists(driver, css_selector: str) -> bool:
    if not css_selector:
        return False
    try:
        return len(driver.find_elements(By.CSS_SELECTOR, css_selector)) > 0
    except Exception:
        return False


def _has_captcha(driver) -> bool:
    return (
        _element_exists(driver, "iframe[src*='recaptcha']")
        or _element_exists(driver, "iframe[title*='recaptcha' i]")
        or "captcha" in (driver.page_source or "").lower()
    )


def _wait_until_submit_enabled(driver, cfg: AuthConfig) -> bool:
    btn = _find_ingresar_button(driver, cfg, 2)
    if btn is None:
        return False

    disabled = (btn.get_attribute("disabled") or "").strip().lower()
    aria_disabled = (btn.get_attribute("aria-disabled") or "").strip().lower()
    return disabled in {"", "false"} and aria_disabled != "true"


def _wait_for_captcha_resolution(driver, cfg: AuthConfig) -> bool:
    """Espera a que el captcha se resuelva en modo manual (navegador visible)."""

    end_time = time.time() + cfg.captcha_timeout
    while time.time() < end_time:
        token_nodes = driver.find_elements(By.CSS_SELECTOR, "textarea[name='g-recaptcha-response']")
        if token_nodes:
            token = (token_nodes[0].get_attribute("value") or "").strip()
            if token:
                return True

        if _wait_until_submit_enabled(driver, cfg):
            return True

        time.sleep(0.5)

    return False


def _wait_for_login_result(driver, cfg: AuthConfig) -> bool:
    """Espera señales de éxito/fallo sin depender solo del cambio de URL."""

    def _condition(drv) -> bool:
        current_url = drv.current_url or ""

        # Exito 1: URL cambia a una ruta diferente de login.
        if _is_authenticated_by_url(current_url, cfg.success_url_keyword):
            return True

        # Exito 2: aparece un selector conocido de pagina autenticada.
        if cfg.post_login_selector and _element_exists(drv, cfg.post_login_selector):
            return True

        # Exito 3: desaparece el campo de password del login.
        if not drv.find_elements(By.ID, cfg.password_id):
            return True

        # Fallo temprano: aparece un mensaje/selector de error de autenticacion.
        if cfg.login_error_selector and _element_exists(drv, cfg.login_error_selector):
            return False

        return False

    try:
        WebDriverWait(driver, cfg.timeout).until(_condition)
    except TimeoutException:
        logging.warning("Timeout esperando resultado de login con Selenium.")

    current_url = driver.current_url or ""
    if _is_authenticated_by_url(current_url, cfg.success_url_keyword):
        return True

    if cfg.post_login_selector and _element_exists(driver, cfg.post_login_selector):
        return True

    # Si seguimos viendo el input de password, normalmente seguimos en login.
    return len(driver.find_elements(By.ID, cfg.password_id)) == 0


def _login_with_selenium(session: requests.Session, cfg: AuthConfig, username: str, password: str) -> bool:
    driver = _new_driver(cfg.selenium_headless)
    try:
        driver.get(cfg.login_url)

        user_input = _find_first(
            driver,
            [
                (By.ID, cfg.username_id),
                (By.CSS_SELECTOR, "input[type='text']"),
                (By.CSS_SELECTOR, "input[autocomplete='username']"),
                (By.CSS_SELECTOR, "input[id*='user'], input[name*='user']"),
            ],
            cfg.timeout,
        )
        pass_input = _find_first(
            driver,
            [
                (By.ID, cfg.password_id),
                (By.CSS_SELECTOR, "input[type='password']"),
                (By.CSS_SELECTOR, "input[autocomplete='current-password']"),
            ],
            cfg.timeout,
        )

        if user_input is None or pass_input is None:
            raise RuntimeError("No se encontraron campos de usuario/contrasena en la pagina.")

        user_input.clear()
        user_input.send_keys(username)
        pass_input.clear()
        pass_input.send_keys(password)

        submit = _find_ingresar_button(driver, cfg, cfg.timeout)
        if submit is None:
            logging.warning("No se encontro boton inicialmente por selectores.")

        # Este portal usa captcha; forzamos flujo manual si se detecta.
        disabled = (submit.get_attribute("disabled") or "").strip().lower() if submit is not None else "<sin-boton>"
        has_captcha = _has_captcha(driver)
        logging.info("Selenium pre-submit: has_captcha=%s disabled=%s", has_captcha, disabled or "<vacio>")

        if has_captcha:
            if cfg.selenium_headless:
                raise RuntimeError(
                    "Se detecto captcha. Ejecuta con OSI_HEADLESS=false para resolverlo manualmente en el navegador."
                )

            print("Captcha detectado: resuelvelo en la ventana del navegador.")
            print("Esperando resolucion del captcha...")
            if not _wait_for_captcha_resolution(driver, cfg):
                raise RuntimeError("No se detecto captcha resuelto dentro del tiempo permitido.")

            logging.info("Captcha resuelto detectado. Ejecutando clic automatico en Ingresar...")

        if not _click_ingresar_with_retries(driver, cfg):
            raise RuntimeError("No se pudo hacer clic automático en el boton Ingresar.")

        if not _wait_for_login_result(driver, cfg):
            return False

        if cfg.open_sne_after_login:
            _click_sne_menu_and_switch_window(driver, cfg)
            _apply_sne_filters(driver, cfg)

        # Copiamos cookies de Selenium a la sesion requests para siguientes pasos.
        for cookie in driver.get_cookies():
            session.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain"), path=cookie.get("path", "/"))

        return True
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def login(session: requests.Session, cfg: AuthConfig, username: str, password: str) -> None:
    """Autentica contra OsiVirtual usando requests y, si es necesario, Selenium."""
    ok = False
    try:
        ok = _login_with_requests(session, cfg, username, password)
        if ok:
            logging.info("Login exitoso con requests.")
            return
        logging.info("Login por requests no concluyente; se intentara con Selenium.")
    except Exception as exc:
        logging.warning("Fallo login con requests: %s", exc)

    ok = _login_with_selenium(session, cfg, username, password)
    if not ok:
        raise RuntimeError("No se pudo autenticar con las credenciales proporcionadas.")

    logging.info("Login exitoso con Selenium.")


def main() -> None:
    _load_dotenv()

    parser = argparse.ArgumentParser(description="Prueba de autenticacion a OsiVirtual.")
    parser.add_argument("--username", default=os.getenv("OSI_USERNAME"), help="Usuario de OsiVirtual (env: OSI_USERNAME)")
    parser.add_argument("--password", default=os.getenv("OSI_PASSWORD"), help="Contrasena de OsiVirtual (env: OSI_PASSWORD)")
    parser.add_argument(
        "--login-url",
        default=os.getenv("OSI_LOGIN_URL", "https://osivirtual.osinergmin.gob.pe/autenticacion/acceso-sistema"),
        help="URL de login (env: OSI_LOGIN_URL)",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("OSI_BASE_URL", "https://osivirtual.osinergmin.gob.pe"),
        help="URL base (env: OSI_BASE_URL)",
    )
    parser.add_argument(
        "--username-id",
        default=os.getenv("OSI_USERNAME_ID", "documentoIdentidad"),
        help="ID del campo usuario (env: OSI_USERNAME_ID)",
    )
    parser.add_argument(
        "--password-id",
        default=os.getenv("OSI_PASSWORD_ID", "contrasena"),
        help="ID del campo contrasena (env: OSI_PASSWORD_ID)",
    )
    parser.add_argument(
        "--login-form-selector",
        default=os.getenv("OSI_LOGIN_FORM_SELECTOR", "form"),
        help="Selector CSS del formulario (env: OSI_LOGIN_FORM_SELECTOR)",
    )
    parser.add_argument(
        "--login-submit-selector",
        default=os.getenv("OSI_LOGIN_SUBMIT_SELECTOR", "button[type='submit']"),
        help="Selector CSS del boton enviar (env: OSI_LOGIN_SUBMIT_SELECTOR)",
    )
    parser.add_argument(
        "--post-login-selector",
        default=os.getenv("OSI_POST_LOGIN_SELECTOR", ""),
        help="Selector CSS visible tras login exitoso (env: OSI_POST_LOGIN_SELECTOR)",
    )
    parser.add_argument(
        "--login-error-selector",
        default=os.getenv("OSI_LOGIN_ERROR_SELECTOR", ""),
        help="Selector CSS de error de login (env: OSI_LOGIN_ERROR_SELECTOR)",
    )
    parser.add_argument(
        "--success-url-keyword",
        default=os.getenv("OSI_SUCCESS_URL_KEYWORD", "autenticacion/acceso-sistema"),
        help="Texto que indica que sigue en login (env: OSI_SUCCESS_URL_KEYWORD)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("OSI_TIMEOUT", "30")),
        help="Timeout en segundos (env: OSI_TIMEOUT)",
    )
    parser.add_argument(
        "--captcha-timeout",
        type=int,
        default=int(os.getenv("OSI_CAPTCHA_TIMEOUT", "180")),
        help="Tiempo maximo para resolver captcha (env: OSI_CAPTCHA_TIMEOUT)",
    )
    parser.add_argument(
        "--open-sne-after-login",
        action="store_true",
        default=_env_bool("OSI_OPEN_SNE_AFTER_LOGIN", True),
        help="Abre Casilla Electrónica del SNE tras login (env: OSI_OPEN_SNE_AFTER_LOGIN)",
    )
    parser.add_argument(
        "--require-sne-click-navigation",
        action="store_true",
        default=_env_bool("OSI_REQUIRE_SNE_CLICK_NAVIGATION", True),
        help="Exige que el SNE se abra por clic real y no por navegacion forzada (env: OSI_REQUIRE_SNE_CLICK_NAVIGATION)",
    )
    parser.add_argument(
        "--sne-menu-selector",
        default=os.getenv(
            "OSI_SNE_MENU_SELECTOR",
            "//mat-nav-list[@role='navigation']"
            "//mat-list-item[.//div[@matlistitemtitle and contains(@class,'mat-mdc-list-item-title') "
            "and contains(@class,'text-menu-parent') and normalize-space()='Casilla Electrónica del SNE']]"
            "//div[@matlistitemtitle and contains(@class,'mat-mdc-list-item-title') "
            "and contains(@class,'text-menu-parent') and normalize-space()='Casilla Electrónica del SNE']",
        ),
        help="XPath/CSS (XPath recomendado) del menu SNE (env: OSI_SNE_MENU_SELECTOR)",
    )
    parser.add_argument(
        "--sne-target-url",
        default=os.getenv(
            "OSI_SNE_TARGET_URL",
            "https://notificaciones.osinergmin.gob.pe/sne-web/pages/notificacion/inicio",
        ),
        help="URL objetivo de Casilla Electronica del SNE (env: OSI_SNE_TARGET_URL)",
    )
    parser.add_argument(
        "--sne-expected-text",
        default=os.getenv(
            "OSI_SNE_EXPECTED_TEXT",
            "Sistema de Notificaciones Electrónicas|Bandeja de Entrada",
        ),
        help="Textos esperados en pantalla SNE separados por | (env: OSI_SNE_EXPECTED_TEXT)",
    )
    parser.add_argument(
        "--fecha-notificacion-inicio",
        default=os.getenv("OSI_FECHA_NOTIFICACION_INICIO", ""),
        help="Fecha inicial para buscar en SNE con formato dd/mm/yyyy (env: OSI_FECHA_NOTIFICACION_INICIO)",
    )
    parser.add_argument(
        "--fecha-notificacion-fin",
        default=os.getenv("OSI_FECHA_NOTIFICACION_FIN", ""),
        help="Fecha final para buscar en SNE con formato dd/mm/yyyy (env: OSI_FECHA_NOTIFICACION_FIN)",
    )
    parser.add_argument(
        "--sne-fecha-inicio-id",
        default=os.getenv("OSI_SNE_FECHA_INICIO_ID", "fechaNotificacionInicio"),
        help="ID del campo fecha inicial del SNE (env: OSI_SNE_FECHA_INICIO_ID)",
    )
    parser.add_argument(
        "--sne-fecha-fin-id",
        default=os.getenv("OSI_SNE_FECHA_FIN_ID", "fechaNotificacionFin"),
        help="ID del campo fecha final del SNE (env: OSI_SNE_FECHA_FIN_ID)",
    )
    parser.add_argument(
        "--sne-buscar-button-id",
        default=os.getenv("OSI_SNE_BUSCAR_BUTTON_ID", "buscar-boton"),
        help="ID del boton Buscar en el SNE (env: OSI_SNE_BUSCAR_BUTTON_ID)",
    )
    parser.add_argument(
        "--user-agent",
        default=os.getenv(
            "OSI_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        ),
        help="User-Agent HTTP (env: OSI_USER_AGENT)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=_env_bool("OSI_HEADLESS", True),
        help="Ejecuta Selenium en modo headless (env: OSI_HEADLESS)",
    )
    args = parser.parse_args()

    if not args.username or not args.password:
        raise RuntimeError("Faltan credenciales. Define OSI_USERNAME y OSI_PASSWORD o pasalas por CLI.")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    cfg = AuthConfig(
        login_url=args.login_url,
        base_url=args.base_url,
        username_id=args.username_id,
        password_id=args.password_id,
        login_form_selector=args.login_form_selector,
        login_submit_selector=args.login_submit_selector,
        post_login_selector=args.post_login_selector,
        login_error_selector=args.login_error_selector,
        success_url_keyword=args.success_url_keyword,
        selenium_headless=args.headless,
        timeout=args.timeout,
        captcha_timeout=args.captcha_timeout,
        open_sne_after_login=args.open_sne_after_login,
        require_sne_click_navigation=args.require_sne_click_navigation,
        sne_menu_selector=args.sne_menu_selector,
        sne_target_url=args.sne_target_url,
        sne_expected_text=args.sne_expected_text,
        fecha_notificacion_inicio=args.fecha_notificacion_inicio,
        fecha_notificacion_fin=args.fecha_notificacion_fin,
        sne_fecha_inicio_id=args.sne_fecha_inicio_id,
        sne_fecha_fin_id=args.sne_fecha_fin_id,
        sne_buscar_button_id=args.sne_buscar_button_id,
        user_agent=args.user_agent,
    )

    session = requests.Session()
    session.headers.update({"User-Agent": cfg.user_agent})

    try:
        login(session, cfg, args.username, args.password)
        print("Autenticacion completada correctamente.")
    except KeyboardInterrupt:
        print("Ejecucion interrumpida por el usuario.")


if __name__ == "__main__":
    main()
