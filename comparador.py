import streamlit as st
import yfinance as yf
import pandas as pd
import requests
import time
import pytz
from datetime import datetime, timedelta

# ==========================================
# CONFIGURACIÓN DE PÁGINA Y ESTILOS CSS
# ==========================================
st.set_page_config(page_title="Arbitraje PRO CEDEARs", layout="wide", page_icon="⚡")
TZ_AR = pytz.timezone('America/Buenos_Aires')
TZ_NY = pytz.timezone('America/New_York')

# Inyección de CSS para diseño UI/UX más profesional
st.markdown("""
<style>
    /* Reducir márgenes superiores para aprovechar la pantalla */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 1rem;
    }
    /* Estilizar la fuente general */
    html, body, [class*="css"] {
        font-family: 'Inter', 'Helvetica Neue', sans-serif;
    }
    /* Agrandar y destacar los valores de las métricas (CCL, MEP, etc) */[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
    }
    /* Suavizar el borde de las tablas */[data-testid="stDataFrame"] {
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

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
    ahora_ny = datetime.now(TZ_NY)
    apertura = ahora_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    cierre = ahora_ny.replace(hour=16, minute=0, second=0, microsecond=0)
    
    if ahora_ny.weekday() >= 5:
        dias = 7 - ahora_ny.weekday()
        proxima = apertura + timedelta(days=dias)
        h, r = divmod((proxima - ahora_ny).total_seconds(), 3600)
        return False, f"Mercado Cerrado (Abre en {int(dias)}d {int(h)%24}h {int(r//60)}m)"

    if ahora_ny < apertura:
        h, r = divmod((apertura - ahora_ny).total_seconds(), 3600)
        return False, f"Pre-Market (Abre en {int(h)}h {int(r//60)}m)"
    elif ahora_ny > cierre:
        dias = 3 if ahora_ny.weekday() == 4 else 1
        proxima = apertura + timedelta(days=dias)
        h, r = divmod((proxima - ahora_ny).total_seconds(), 3600)
        txt_d = f"{int(dias)}d " if dias > 1 else ""
        return False, f"Mercado Cerrado (Abre en {txt_d}{int(h)%24}h {int(r//60)}m)"
    else:
        h, r = divmod((cierre - ahora_ny).total_seconds(), 3600)
        return True, f"Mercado Abierto (Cierra en {int(h)}h {int(r//60)}m)"

# ==========================================
# FUNCIONES DE EXTRACCIÓN
# ==========================================
def obtener_dolares_api():
    # URL actualizada para el dólar MEP a /bolsa
    urls =[
        ("https://dolarapi.com/v1/dolares/contadoconliqui", "https://dolarapi.com/v1/dolares/bolsa"),
        ("https://dolarapi.com/v1/ambito/dolares/contadoconliqui", "https://dolarapi.com/v1/ambito/dolares/bolsa")
    ]
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0 Chrome/120.0.0.0"}
    
    error = ""
    for url_ccl, url_mep in urls:
        try:
            r_ccl = requests.get(url_ccl, headers=headers, timeout=4)
            r_mep = requests.get(url_mep, headers=headers, timeout=4)
            return float(r_ccl.json()['venta']), float(r_mep.json()['venta']), ""
        except Exception as e:
            error = str(e)
            continue
    return None, None, error

@st.cache_data(ttl=60)
def obtener_indices():
    try:
        data = yf.download(['^MERV', 'SPY'], period="5d", interval="1d", progress=False)['Close']
        m_val, m_prev = data['^MERV'].dropna().iloc[-1], data['^MERV'].dropna().iloc[-2]
        s_val, s_prev = data['SPY'].dropna().iloc[-1], data['SPY'].dropna().iloc[-2]
        return m_val, ((m_val/m_prev)-1)*100, s_val, ((s_val/s_prev)-1)*100
    except:
        return 0,0,0,0

@st.cache_data(ttl=60)
def obtener_datos_vivo(tickers_dict):
    us_t = list(tickers_dict.keys())
    ar_t =[f"{t}.BA" for t in us_t]
    daily = yf.download(us_t, period="5d", interval="1d", progress=False)['Close']
    live = yf.download(us_t + ar_t, period="5d", interval="15m", prepost=True, progress=False)
    return daily, live

# ==========================================
# UI: BARRA LATERAL (SIDEBAR)
# ==========================================
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/2942/2942259.png", width=60) # Ícono de terminal
st.sidebar.title("Configuración")
st.sidebar.markdown("---")

st.sidebar.subheader("🔌 Conexión Dólar API")
usar_api = st.sidebar.toggle("🌐 Usar valor web (Automático)", value=True)
ccl_manual = st.sidebar.number_input("💵 CCL Manual ($)", value=1300.0, step=5.0, disabled=usar_api)
mep_manual = st.sidebar.number_input("💵 MEP Manual ($)", value=1250.0, step=5.0, disabled=usar_api)

st.sidebar.markdown("---")
st.sidebar.subheader("⚙️ Filtros Algorítmicos")
vol_min_millones = st.sidebar.slider("Liquidez Mínima (Millones ARS)", 0.0, 50.0, 5.0, step=1.0, 
                                     help="Oculta activos sin liquidez real para evitar variaciones fantasma.")
mostrar_todo = st.sidebar.checkbox("Ignorar filtro de liquidez", False)

st.sidebar.markdown("---")
espacio_timer = st.sidebar.empty() # Espacio reservado para el contador sin romper la UI

# ==========================================
# LÓGICA PRINCIPAL Y DASHBOARD
# ==========================================
st.title("⚡ Dashboard de Arbitraje Institucional")

# Banner del Estado del Mercado
abierto, txt_mercado = obtener_estado_mercado()
if abierto:
    st.success(f"🟢 **{txt_mercado}** - Operaciones en Tiempo Real")
else:
    st.warning(f"🟠 **{txt_mercado}** - Mostrando Ext. Hours y último cierre local")

# Extracción de Datos Macro
ccl_ref, mep_ref, err = (None, None, "") if not usar_api else obtener_dolares_api()
if not ccl_ref:
    ccl_ref, mep_ref = float(ccl_manual), float(mep_manual)
    if usar_api: st.toast("⚠️ Fallo en DolarAPI. Usando valores manuales.", icon="🚨")

with st.spinner("Sincronizando con Wall Street y BYMA..."):
    merv_v, merv_p, spy_v, spy_p = obtener_indices()
    daily_us, datos_vivo = obtener_datos_vivo(CEDEARS)

# Tarjetas de Métricas UI
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("💵 Dólar CCL (Ref)", f"${ccl_ref:.2f}")
col2.metric("💵 Dólar MEP", f"${mep_ref:.2f}")
col3.metric("⚖️ Brecha Dólares", f"{((ccl_ref/mep_ref)-1)*100:.2f}%")
col4.metric("🇦🇷 Índice MERVAL", f"{merv_v:,.0f}", f"{merv_p:+.2f}%")
col5.metric("🇺🇸 Índice S&P 500", f"${spy_v:.2f}", f"{spy_p:+.2f}%")

st.markdown("---")

# Procesamiento de Datos
resultados =[]
if datos_vivo is not None and isinstance(datos_vivo.columns, pd.MultiIndex):
    closes, volumes = datos_vivo['Close'], datos_vivo['Volume']
    highs, lows = datos_vivo['High'], datos_vivo['Low']
    fecha_hoy_ny = datetime.now(TZ_NY).date()
    
    for t, ratio in CEDEARS.items():
        t_ba = f"{t}.BA"
        try:
            if t in closes.columns and t_ba in closes.columns:
                s_us, s_ar = closes[t].dropna(), closes[t_ba].dropna()
                if not s_us.empty and not s_ar.empty:
                    us_pr = float(s_us.iloc[-1])
                    ar_pr = float(s_ar.iloc[-1])
                    
                    # Tiempos
                    lt_ar = s_ar.index[-1]
                    if lt_ar.tz is None: lt_ar = pytz.utc.localize(lt_ar)
                    hora_str = lt_ar.astimezone(TZ_AR).strftime("%H:%M")
                    
                    # Liquidez
                    hoy_ar = lt_ar.astimezone(TZ_AR).date()
                    v_serie = volumes[t_ba].dropna()
                    v_hoy = v_serie[v_serie.index.date == hoy_ar].sum()
                    millones = (ar_pr * v_hoy) / 1_000_000
                    
                    if not mostrar_todo and millones < vol_min_millones: continue
                    
                    # Volatilidad
                    h_hoy = highs[t_ba][highs[t_ba].index.date == hoy_ar].max()
                    l_hoy = lows[t_ba][lows[t_ba].index.date == hoy_ar].min()
                    spread = ((h_hoy / l_hoy) - 1) * 100 if l_hoy > 0 else 0
                    
                    # Var US
                    s_dly = daily_us[t].dropna()
                    if s_dly.index.tz is None: s_dly.index = s_dly.index.tz_localize('UTC')
                    prev = s_dly[s_dly.index.tz_convert('America/New_York').date < fecha_hoy_ny]
                    var_us = ((us_pr / float(prev.iloc[-1])) - 1) * 100 if not prev.empty else 0.0
                    
                    if us_pr > 0:
                        ccl_imp = (ar_pr * ratio) / us_pr
                        d_ccl = ((ccl_imp / ccl_ref) - 1) * 100
                        
                        # Auto-corrección Ratio
                        if abs(d_ccl) > 10.0:
                            n_ratio = round((ccl_ref * us_pr) / ar_pr)
                            if n_ratio > 0:
                                ccl_imp = (ar_pr * n_ratio) / us_pr
                                d_ccl = ((ccl_imp / ccl_ref) - 1) * 100
                                
                        resultados.append({
                            "Ticker": f"**{t}**",
                            "⏱️ Hora": hora_str,
                            "🇺🇸 USD": us_pr,
                            "🇦🇷 ARS": ar_pr,
                            "📊 Vol(M)": millones,
                            "↕️ Rango": spread,
                            "US Var%": var_us,
                            "💵 CCL Imp": ccl_imp,
                            "⚖️ vs CCL%": d_ccl
                        })
        except Exception:
            continue

# ==========================================
# RENDERIZADO DE TABLAS PROFESIONALES (MODIFICADO)
# ==========================================
if resultados:
    df = pd.DataFrame(resultados)
    
    # Quitamos los asteriscos de los Tickers
    df['Ticker'] = df['Ticker'].str.replace('**', '', regex=False)
    
    # Definimos los Top 10
    df_pos = df[df['⚖️ vs CCL%'] > 0].sort_values(by="⚖️ vs CCL%", ascending=False).head(10)
    df_neg = df[df['⚖️ vs CCL%'] < 0].sort_values(by="⚖️ vs CCL%", ascending=True).head(10)
    
    def color_fuerte_invertido(val):
        """Invertimos: Rojo para caro (>0), Verde para barato (<0)"""
        if val > 0:
            color = '#e74c3c' # Rojo
        elif val < 0:
            color = '#2ecc71' # Verde
        else:
            color = 'white'
        return f'color: {color}; font-weight: bold;'

    def pintar_filas_invertido(row):
        """Fondo rojo para caros, verde para baratos"""
        if row['⚖️ vs CCL%'] > 2.0:
            return ['background-color: rgba(231, 76, 60, 0.2)'] * len(row) # Rojo suave
        elif row['⚖️ vs CCL%'] < -2.0:
            return ['background-color: rgba(46, 204, 113, 0.2)'] * len(row) # Verde suave
        return [''] * len(row)

    fmt = {
        "🇺🇸 USD": "${:.2f}",
        "🇦🇷 ARS": "${:,.2f}",
        "📊 Vol(M)": "${:.1f}M",
        "↕️ Rango": "{:.1f}%",
        "US Var%": "{:+.2f}%",
        "💵 CCL Imp": "${:.2f}",
        "⚖️ vs CCL%": "{:+.2f}%"
    }

    t1, t2 = st.columns(2)
    
    with t1:
        st.markdown("### 🔴 Oportunidades VENTA (Caros)")
        st.caption("Fondo resaltado si cotiza a más del **+2%** del Dólar CCL.")
        if not df_pos.empty:
            styled_pos = (df_pos.style
                          .format(fmt)
                          .apply(pintar_filas_invertido, axis=1)
                          .map(color_fuerte_invertido, subset=['⚖️ vs CCL%']))
            st.dataframe(styled_pos, use_container_width=True, hide_index=True)
        else:
            st.info("Sin activos en esta categoría.")
            
    with t2:
        st.markdown("### 🟢 Oportunidades COMPRA (Baratos)")
        st.caption("Fondo resaltado si cotiza a menos del **-2%** del Dólar CCL.")
        if not df_neg.empty:
            styled_neg = (df_neg.style
                          .format(fmt)
                          .apply(pintar_filas_invertido, axis=1)
                          .map(color_fuerte_invertido, subset=['⚖️ vs CCL%']))
            st.dataframe(styled_neg, use_container_width=True, hide_index=True)
        else:
            st.info("Sin activos en esta categoría.")

# ==========================================
# AUTO-REFRESH LIMPIO (EN LA BARRA LATERAL)
# ==========================================
for i in range(60, 0, -1):
    espacio_timer.caption(f"⏱️ Próximo escaneo de mercado en **{i}s**")
    time.sleep(1)
st.rerun()