# 📟 MACRO-TERM v4.88 // Global Macroeconomic Retro CRT Terminal

Aplicación web de monitoreo macroeconómico global con estética de **Terminal Financiera Retro CRT de los años 80** (estilo Bloomberg Terminal monocromática), construida con **FastAPI + HTML5 Canvas / JS puro** y conectada a **datos reales 100% gratuitos** (Yahoo Finance, FRED y BCRP, sin API keys).

---

## Características Principales

- **Fluidez Instantánea (0 ms en UI):** Frontend en HTML5 Canvas + JavaScript puro con caché en navegador para cambios de activo, temporalidad, modo **Línea / Velas Retro**, crosshair interactivo y selector de tema **Fósforo Verde (`#00ff41`) / Ámbar (`#ffb000`)** sin recargas ni parpadeos.
- **Datos Reales Auto-Actualizables (cada 30 s):**
  - **Renta Variable & Volatilidad:** S&P 500 (`^GSPC`), VIX (`^VIX`), MSCI Emerging Markets (`EEM`), DAX (`^GDAXI`), Hang Seng (`^HSI`).
  - **Tasas & Divisas:** US Treasury 10Y (`^TNX`), US 2Y (`2YY=F`), US 3M (`^IRX`), DXY (`DX-Y.NYB`), USD/PEN (`PEN=X`), EUR/USD (`EURUSD=X`).
  - **Materias Primas:** Cobre / *Dr. Copper* (`HG=F`), Petróleo WTI (`CL=F`), Brent (`BZ=F`), Oro (`GC=F`).
  - **Indicadores Macro & Riesgo País (FRED + BCRP):** US CPI (YoY), US Core PCE (YoY), Fed Funds Target, US Mfg Output (YoY), China CPI (YoY), EMBI+ Perú y EMBI+ LatAm en puntos básicos.
- **Spread Curva 10Y - 2Y & Alertas:** Cálculo continuo del diferencial `10Y - 2Y` con alerta visual en rojo si la curva se invierte (`< 0`), más consola `[SYS_LOG]` con alertas automáticas si algún activo varía más de `±1.5%`.
- **Manejo de Mercado Cerrado:** Si es fin de semana o fuera de horario, muestra el último cierre disponible con la etiqueta `[MARKET CLOSED / LAST CLOSE]`.

---

## Estructura del Repositorio

```text
.
├── app.py                # Servidor FastAPI (API REST + entrega del frontend CRT)
├── data_fetcher.py       # Motor de datos reales (yfinance, FRED CSV público, BCRP JSON)
├── styles.css            # Hoja de estilos retro CRT (scanlines, glow, marquee)
├── static/
│   └── index.html        # Terminal HTML5 Canvas + JS (0 ms UI + auto-sync 30s)
├── requirements.txt      # Dependencias Python
├── render.yaml           # Blueprint para despliegue gratuito en Render
├── .gitignore
└── README.md
```

---

## Ejecución Local

```bash
# 1. Crear entorno virtual e instalar dependencias
python3 -m venv .venv
source .venv/bin/activate   # En Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Iniciar la terminal FastAPI
uvicorn app:app --reload --port 8000
```

Abre `http://localhost:8000` en tu navegador.

---

## Despliegue Gratuito en la Nube (Render) + Embebido en Google Sites

### 1. Subir a GitHub
```bash
git init
git branch -M main
git add .
git commit -m "feat: MACRO-TERM v4.88 FastAPI + HTML5 Canvas CRT with real data"
git remote add origin https://github.com/TU_USUARIO/macro-term.git
git push -u origin main
```

### 2. Desplegar gratis en Render
1. Entra a [render.com](https://render.com/) e inicia sesión con GitHub.
2. Haz clic en **New → Blueprint** (detecta automáticamente `render.yaml`) o **New → Web Service** y selecciona tu repositorio `macro-term`.
3. Si usas Web Service manual, configura:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn app:app --host 0.0.0.0 --port $PORT`
   - **Plan:** `Free`
4. Haz clic en **Deploy**. Obtendrás una URL HTTPS pública (`https://macro-term-crt.onrender.com`).

### 3. Incrustar en Google Sites (Opcional)
En Google Sites → **Insertar → Incorporar → Por URL** y pega tu URL de Render (`https://macro-term-crt.onrender.com`). La terminal se mostrará a pantalla completa dentro de tu sitio con datos 100% reales.
