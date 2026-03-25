import streamlit as st
import yfinance as yf
import pandas as pd
import requests
import time
import pytz
from datetime import datetime, timedelta

# ==========================================
# CONFIGURACIÓN DE PÁGINA Y CONSTANTES
# ==========================================
st.set_page_config(page_title="Arbitraje PRO CEDEARs", layout="wide", page_icon="📈")
TZ_AR = pytz.timezone('America/Buenos_Aires')
TZ_NY = pytz.timezone('America/New_York')

# Top 50 CEDEARs más líquidos
CEDEARS = {
    'AAPL': 20, 'MSFT': 30, 'AMZN': 144, 'GOOGL': 58, 'TSLA': 15,
    'NVDA': 24, 'META': 3, 'NFLX': 16, 'MELI': 60, 'SPY': 20,
    'QQQ': 20, 'DIA': 20, 'IWM': 20, 'EEM': 5, 'AMD': 10,
    'BA': 24, 'DIS': 4, 'V': 18, 'MA': 33, 'JNJ': 15,
    'JPM': 5, 'PG': 5, 'WMT': 18, 'PFE': 2, 'MCD': 18,
    'PEP': 6, 'CSCO': 5, 'BABA': 9, 'HD': 22, 'PYPL': 6,
    'NKE': 3, 'QCOM': 11, 'KO': 5, 'XLE': 2, 'XLF': 2,
    'ARKK': 10, 'BHP': 2, 'CAT': 10, 'CRM': 18, 'CVX': 8,
    'INTC': 5, 'LLY': 14, 'UBER': 10, 'UNH': 33, 'VIST': 1
}

# ==========================================
# FUNCIONES DE TIEMPO Y MERCADO
# ==========================================
def obtener_estado_mercado():
    """Calcula si el mercado en USA está abierto y cuánto falta para abrir/cerrar"""
    ahora_ny = datetime.now(TZ_NY)
    
    # Horarios estándar de Wall Street (9:30 a 16:00 ET)
    apertura = ahora_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    cierre = ahora_ny.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # Si es fin de semana (5=Sábado, 6=Domingo)
    if ahora_ny.weekday() >= 5:
        dias_faltantes = 7 - ahora_ny.weekday()
        proxima_apertura = apertura + timedelta(days=dias_faltantes)
        tiempo_restante = proxima_apertura - ahora_ny
        horas, resto = divmod(tiempo_restante.total_seconds(), 3600)
        minutos = resto // 60
        return False, f"🔴 Mercado Cerrado (Abre en {int(dias_faltantes)}d {int(horas)%24}h {int(minutos)}m)"

    # Lunes a Viernes
    if ahora_ny < apertura:
        tiempo_restante = apertura - ahora_ny
        horas, resto = divmod(tiempo_restante.total_seconds(), 3600)
        return False, f"🔴 Mercado Cerrado (Abre en {int(horas)}h {int(resto//60)}m)"
    elif ahora_ny > cierre:
        # Si ya cerró hoy, y es Viernes (4), suma 3 días. Sino suma 1.
        dias_faltantes = 3 if ahora_ny.weekday() == 4 else 1
        proxima_apertura = apertura + timedelta(days=dias_faltantes)
        tiempo_restante = proxima_apertura - ahora_ny
        horas, resto = divmod(tiempo_restante.total_seconds(), 3600)
        dia_texto = f"{int(dias_faltantes)}d " if dias_faltantes > 1 else ""
        return False, f"🔴 Mercado Cerrado (Abre en {dia_texto}{int(horas)%24}h {int(resto//60)}m)"
    else:
        tiempo_restante = cierre - ahora_ny
        horas, resto = divmod(tiempo_restante.total_seconds(), 3600)
        return True, f"🟢 Mercado Abierto (Cierra en {int(horas)}h {int(resto//60)}m)"

# ==========================================
# FUNCIONES DE EXTRACCIÓN (CON CACHÉ)
# ==========================================
def obtener_dolares_api():
    urls =[
        ("https://dolarapi.com/v1/dolares/contadoconliqui", "https://dolarapi.com/v1/dolares/bolsa"),
        ("https://dolarapi.com/v1/ambito/dolares/contadoconliqui", "https://dolarapi.com/v1/ambito/dolares/mep")
    ]
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0 Chrome/120.0.0.0"}
    
    ultimo_error = ""
    for url_ccl, url_mep in urls:
        try:
            r_ccl = requests.get(url_ccl, headers=headers, timeout=5)
            r_mep = requests.get(url_mep, headers=headers, timeout=5)
            r_ccl.raise_for_status()
            r_mep.raise_for_status()
            return float(r_ccl.json()['venta']), float(r_mep.json()['venta']), ""
        except Exception as e:
            ultimo_error = str(e)
            continue
    return None, None, ultimo_error

@st.cache_data(ttl=60)
def obtener_indices():
    """Descarga datos del MERVAL y SPY"""
    try:
        data = yf.download(['^MERV', 'SPY'], period="5d", interval="1d", progress=False)['Close']
        merv_val = data['^MERV'].dropna().iloc[-1]
        merv_prev = data['^MERV'].dropna().iloc[-2]
        merv_pct = ((merv_val / merv_prev) - 1) * 100
        
        spy_val = data['SPY'].dropna().iloc[-1]
        spy_prev = data['SPY'].dropna().iloc[-2]
        spy_pct = ((spy_val / spy_prev) - 1) * 100
        return merv_val, merv_pct, spy_val, spy_pct
    except:
        return 0.0, 0.0, 0.0, 0.0

@st.cache_data(ttl=60)
def obtener_datos_vivo(tickers_dict):
    us_tickers = list(tickers_dict.keys())
    ar_tickers =[f"{t}.BA" for t in us_tickers]
    all_tickers = us_tickers + ar_tickers
    
    # 1. Datos diarios para saber el "Cierre Anterior" de USA
    daily_us = yf.download(us_tickers, period="5d", interval="1d", progress=False)['Close']
    
    # 2. Datos de 15 mins con PREPOST=TRUE para atrapar movimientos After-hours / Pre-market
    live_data = yf.download(all_tickers, period="5d", interval="15m", prepost=True, progress=False)
    return daily_us, live_data

# ==========================================
# UI: BARRA LATERAL
# ==========================================
st.sidebar.header("⚙️ Controles de Referencia")
usar_api = st.sidebar.toggle("🌐 Obtener Dólar Web", value=True)
st.sidebar.caption("Dólar de Emergencia (Manual)")
ccl_manual = st.sidebar.number_input("CCL Manual ($)", min_value=1.0, value=1300.0, step=10.0)
mep_manual = st.sidebar.number_input("MEP Manual ($)", min_value=1.0, value=1250.0, step=10.0)

st.sidebar.markdown("---")
st.sidebar.header("⚙️ Filtros de Mercado")
vol_min_millones = st.sidebar.slider("Liquidez: Vol. Mín. Local (Millones ARS)", 0.0, 50.0, 5.0, step=1.0)
mostrar_todo = st.sidebar.checkbox("Mostrar todos (ignorar liquidez)", False)

# ==========================================
# LÓGICA PRINCIPAL
# ==========================================
st.title("⚡ Dashboard de Arbitraje Institucional")

# Timer de Mercado
_, texto_mercado = obtener_estado_mercado()
st.subheader(texto_mercado)
st.markdown("---")

ccl_ref, mep_ref, error_api = None, None, ""

if usar_api:
    ccl_ref, mep_ref, error_api = obtener_dolares_api()

if not ccl_ref:
    if usar_api:
        st.warning("⚠️ DolarAPI falló. Cambiando a valores manuales.")
    ccl_ref, mep_ref = float(ccl_manual), float(mep_manual)

with st.spinner("Analizando mercado en tiempo real (incluyendo Ext. Hours)..."):
    merv_val, merv_pct, spy_val, spy_pct = obtener_indices()
    daily_us, datos_vivo = obtener_datos_vivo(CEDEARS)
    
# Métricas Superiores (Añadido MERVAL y SPY)
col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
col_m1.metric("💵 Dólar CCL", f"${ccl_ref:.2f}")
col_m2.metric("💵 Dólar MEP", f"${mep_ref:.2f}")
col_m3.metric("↔️ Brecha Dólares", f"{((ccl_ref/mep_ref)-1)*100:.2f}%")
col_m4.metric("🇦🇷 MERVAL", f"{merv_val:,.0f}", f"{merv_pct:+.2f}%")
col_m5.metric("🇺🇸 S&P 500 (SPY)", f"${spy_val:.2f}", f"{spy_pct:+.2f}%")

resultados =[]

if datos_vivo is not None and isinstance(datos_vivo.columns, pd.MultiIndex):
    closes = datos_vivo['Close']
    volumes = datos_vivo['Volume']
    highs = datos_vivo['High']
    lows = datos_vivo['Low']
    
    fecha_hoy_ny = datetime.now(TZ_NY).date()
    
    for t, ratio in CEDEARS.items():
        t_ba = f"{t}.BA"
        try:
            if t in closes.columns and t_ba in closes.columns:
                serie_us = closes[t].dropna()
                serie_ar = closes[t_ba].dropna()
                
                if not serie_us.empty and not serie_ar.empty:
                    us_price = float(serie_us.iloc[-1])
                    ar_price = float(serie_ar.iloc[-1])
                    
                    # Timestamp AR
                    last_time_ar = serie_ar.index[-1]
                    if last_time_ar.tz is None: last_time_ar = pytz.utc.localize(last_time_ar)
                    hora_str = last_time_ar.astimezone(TZ_AR).strftime("%H:%M")
                    
                    # Filtro Volumen
                    fecha_hoy_ar = last_time_ar.astimezone(TZ_AR).date()
                    vol_serie = volumes[t_ba].dropna()
                    vol_hoy = vol_serie[vol_serie.index.date == fecha_hoy_ar].sum()
                    monto_millones = (ar_price * vol_hoy) / 1_000_000
                    
                    if not mostrar_todo and monto_millones < vol_min_millones:
                        continue
                    
                    # Spread Intradía
                    high_hoy = highs[t_ba][highs[t_ba].index.date == fecha_hoy_ar].max()
                    low_hoy = lows[t_ba][lows[t_ba].index.date == fecha_hoy_ar].min()
                    spread_intra = ((high_hoy / low_hoy) - 1) * 100 if low_hoy > 0 else 0
                    
                    # Cálculo Var US % (Movimiento incluyendo After-Hours/Pre-Market)
                    us_daily_series = daily_us[t].dropna()
                    if us_daily_series.index.tz is None:
                        us_daily_series.index = us_daily_series.index.tz_localize('UTC')
                    
                    # Buscamos el cierre del día anterior para usar de base
                    prev_closes = us_daily_series[us_daily_series.index.tz_convert('America/New_York').date < fecha_hoy_ny]
                    var_us_pct = 0.0
                    if not prev_closes.empty:
                        us_prev_close = float(prev_closes.iloc[-1])
                        var_us_pct = ((us_price / us_prev_close) - 1) * 100
                    
                    if us_price > 0:
                        ccl_implicito = (ar_price * ratio) / us_price
                        diff_ccl = ((ccl_implicito / ccl_ref) - 1) * 100
                        
                        if abs(diff_ccl) > 10.0:
                            ratio_calc = round((ccl_ref * us_price) / ar_price)
                            if ratio_calc > 0:
                                ratio = ratio_calc
                                ccl_implicito = (ar_price * ratio) / us_price
                                diff_ccl = ((ccl_implicito / ccl_ref) - 1) * 100
                        
                        resultados.append({
                            "Ticker": t,
                            "Vol. (Mill)": monto_millones,
                            "Hora": hora_str,
                            "Rango Intradía": spread_intra,
                            "Var. US %": var_us_pct,
                            "CCL Imp": ccl_implicito,
                            "vs CCL %": diff_ccl
                        })
        except Exception:
            continue

# ==========================================
# RENDERIZADO DE TABLAS
# ==========================================
if resultados:
    df = pd.DataFrame(resultados)
    
    df_pos = df[df['vs CCL %'] > 0].sort_values(by="vs CCL %", ascending=False).head(10)
    df_neg = df[df['vs CCL %'] < 0].sort_values(by="vs CCL %", ascending=True).head(10)
    
    def color_verde(row):
        # Color verde puro si la diferencia al CCL > 2%
        if row['vs CCL %'] > 2.0:
            return['background-color: #1b5e20; color: white'] * len(row)
        return [''] * len(row)
        
    def color_rojo(row):
        # Color rojo puro si la diferencia al CCL < -2%
        if row['vs CCL %'] < -2.0:
            return['background-color: #b71c1c; color: white'] * len(row)
        return [''] * len(row)

    formato = {
        "Vol. (Mill)": "${:.1f}M",
        "Rango Intradía": "{:.1f}%",
        "Var. US %": "{:+.2f}%",
        "CCL Imp": "${:.2f}",
        "vs CCL %": "{:+.2f}%"
    }

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🟢 Top 10 Caros (Spread Positivo)")
        st.caption("Filtro Verde: Mayor al +2% sobre el Dólar CCL.")
        if not df_pos.empty:
            st.dataframe(df_pos.style.format(formato).apply(color_verde, axis=1), use_container_width=True, hide_index=True)
        else:
            st.write("No hay activos.")
            
    with col2:
        st.subheader("🔴 Top 10 Baratos (Spread Negativo)")
        st.caption("Filtro Rojo: Menor al -2% sobre el Dólar CCL.")
        if not df_neg.empty:
            st.dataframe(df_neg.style.format(formato).apply(color_rojo, axis=1), use_container_width=True, hide_index=True)
        else:
            st.write("No hay activos.")
else:
    st.warning("No se encontraron oportunidades.")

# ==========================================
# AUTO-REFRESH (60 Segundos)
# ==========================================
contador = st.empty()
for i in range(60, 0, -1):
    contador.caption(f"🔄 Actualizando en {i} seg...")
    time.sleep(1)
st.rerun()