# Autenticacion OsiVirtual

Script base para autenticarse primero contra:
https://osivirtual.osinergmin.gob.pe/autenticacion/acceso-sistema

## Que hace

- Usa sesion HTTP persistente con requests.Session().
- Intenta login por requests (si existe formulario tradicional).
- Si la pagina carga por JavaScript (SPA), hace fallback a Selenium.
- Valida si el login fue exitoso y deja la sesion autenticada para siguientes pasos.

## Instalacion

```bash
pip install -r requirements.txt
```

## Configuracion por variables de entorno

Configura todo por entorno (recomendado). Variables soportadas:

- OSI_USERNAME
- OSI_PASSWORD
- OSI_LOGIN_URL
- OSI_BASE_URL
- OSI_USERNAME_ID
- OSI_PASSWORD_ID
- OSI_LOGIN_FORM_SELECTOR
- OSI_LOGIN_SUBMIT_SELECTOR
- OSI_SUCCESS_URL_KEYWORD
- OSI_HEADLESS
- OSI_TIMEOUT
- OSI_USER_AGENT

Puedes usar el archivo [.env.example](.env.example) como plantilla.

Ejemplo en PowerShell:

```powershell
$env:OSI_USERNAME="tu_usuario"
$env:OSI_PASSWORD="tu_contrasena"
$env:OSI_USERNAME_ID="documentoIdentidad"
$env:OSI_PASSWORD_ID="contrasena"
```

## Ejecucion

```bash
python osinergmin_auth.py
```

Tambien puedes sobreescribir por CLI si lo necesitas.

## Siguiente paso

Con esta base de autenticacion lista, se puede acoplar la extraccion de documentos filtrados y descargas.
