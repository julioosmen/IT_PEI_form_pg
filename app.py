
import re
import streamlit as st
import pandas as pd
import os
import base64
from datetime import datetime
from textwrap import dedent
from sqlalchemy import create_engine


def guardar_en_historial_excel(nuevo: dict, path: str):
    """
    Guarda un nuevo registro en el Excel historial_it_pei.xlsx
    - Si el archivo no existe, lo crea
    - Si existe, agrega una nueva fila
    """

    # Normalización robusta del código
    def normalizar_codigo(x):
        if pd.isna(x) or x is None:
            return ""
        try:
            return str(int(float(x)))   # 23.0 -> "23"
        except Exception:
            return str(x).strip()

    # 1) Dict -> DataFrame (1 fila)
    df_nuevo = pd.DataFrame([nuevo])

    # 2) Asegurar columna normalizada
    df_nuevo["codigo_ue_norm"] = df_nuevo["codigo"].apply(normalizar_codigo)

    # 3) Crear archivo si no existe
    if not os.path.exists(path):
        df_nuevo.to_excel(path, index=False, engine="openpyxl")
        return

    # 4) Leer historial existente
    df_hist = pd.read_excel(path, engine="openpyxl")
    df_hist.columns = df_hist.columns.astype(str).str.strip()

    # 5) Concatenar y sobrescribir
    df_final = pd.concat([df_hist, df_nuevo], ignore_index=True, sort=False)
    df_final.to_excel(path, index=False, engine="openpyxl")


# =====================================
# ✅ PARTE INTEGRADA
# =====================================
HISTORIAL_PATH = "data/historial_it_pei.xlsx"

FORM_DEFAULTS = {
    "tipo_pei": "Formulado",
    "etapa_revision": "IT Emitido",
    "fecha_recepcion": None,
    "articulacion": "",
    "fecha_derivacion": None,
    "periodo": "",
    "cantidad_revisiones": 0,
    "comentario": "",
    "vigencia": "Sí",
    "estado": "En proceso",
    "expediente": "",
    "fecha_it": None,
    "fecha_oficio": None,
    "numero_it": "",
    "numero_oficio": "",
}

FORM_STATE_KEY = "pei_form_data"

def init_form_state():
    st.session_state.setdefault(FORM_STATE_KEY, FORM_DEFAULTS.copy())

def reset_form_state():
    st.session_state[FORM_STATE_KEY] = FORM_DEFAULTS.copy()

def index_of(options, value, fallback=0):
    try:
        return options.index(value)
    except Exception:
        return fallback

def set_form_state_from_row(row: pd.Series):
    form = FORM_DEFAULTS.copy()

    def _safe_str(x):
        return "" if pd.isna(x) else str(x).strip()

    def _safe_int(x):
        try:
            return int(x)
        except Exception:
            return 0

    def _safe_date(x):
        if pd.isna(x) or x is None or str(x).strip() == "":
            return None
        try:
            return pd.to_datetime(x).date()
        except Exception:
            return None

    # Normalizador tolerante para valores de selectbox (mayúsculas, espacios, guiones bajos)
    def norm_choice(val, mapping: dict, default: str):
        s = _safe_str(val).lower()
        s = re.sub(r"\s+", " ", s)      # colapsa espacios múltiples
        s = s.replace("_", " ").strip() # "en_proceso" -> "en proceso"
        return mapping.get(s, default)

    # Mapeos a los valores EXACTOS que usa tu formulario
    estado_map = {
        "emitido": "Emitido",
        "en proceso": "En proceso",
        "proceso": "En proceso",
    }

    vigencia_map = {
        "sí": "Sí",
        "si": "Sí",
        "no": "No",
    }

    tipo_pei_map = {
        "formulado": "Formulado",
        "ampliado": "Ampliado",
        "actualizado": "Actualizado",
    }

    etapa_map = {
        "it emitido": "IT Emitido",
        "para emisión de it": "Para emisión de IT",
        "para emision de it": "Para emisión de IT",
        "revisión dncp": "Revisión DNCP",
        "revision dncp": "Revisión DNCP",
        "revisión dnse": "Revisión DNSE",
        "revision dnse": "Revisión DNSE",
        "revisión dnpe": "Revisión DNPE",
        "revision dnpe": "Revisión DNPE",
        "subsanación del pliego": "Subsanación del pliego",
        "subsanacion del pliego": "Subsanación del pliego",
    }

    # --- CARGA NORMALIZADA ---
    form["tipo_pei"] = norm_choice(
        row.get("tipo_pei", FORM_DEFAULTS["tipo_pei"]),
        tipo_pei_map,
        FORM_DEFAULTS["tipo_pei"]
    )

    form["etapa_revision"] = norm_choice(
        row.get("etapa_revision", FORM_DEFAULTS["etapa_revision"]),
        etapa_map,
        FORM_DEFAULTS["etapa_revision"]
    )

    form["fecha_recepcion"] = _safe_date(row.get("fecha_recepcion"))
    form["articulacion"] = _safe_str(row.get("articulacion", ""))
    form["fecha_derivacion"] = _safe_date(row.get("fecha_derivacion"))
    form["periodo"] = _safe_str(row.get("periodo", ""))
    form["cantidad_revisiones"] = _safe_int(row.get("cantidad_revisiones", 0))
    form["comentario"] = _safe_str(row.get("comentario", ""))

    form["vigencia"] = norm_choice(
        row.get("vigencia", FORM_DEFAULTS["vigencia"]),
        vigencia_map,
        FORM_DEFAULTS["vigencia"]
    )

    # ESTE ES EL CAMBIO CLAVE PARA TU BUG:
    form["estado"] = norm_choice(
        row.get("estado", FORM_DEFAULTS["estado"]),
        estado_map,
        FORM_DEFAULTS["estado"]
    )

    form["expediente"] = _safe_str(row.get("expediente", ""))
    form["fecha_it"] = _safe_date(row.get("fecha_it"))
    form["numero_it"] = _safe_str(row.get("numero_it", ""))
    form["fecha_oficio"] = _safe_date(row.get("fecha_oficio"))
    form["numero_oficio"] = _safe_str(row.get("numero_oficio", ""))

    st.session_state[FORM_STATE_KEY] = form


# =====================================
# 🏛️ Carga y búsqueda de unidades ejecutoras
# =====================================
@st.cache_data
def cargar_unidades_ejecutoras():
    return pd.read_excel("data/unidades_ejecutoras.xlsx", engine="openpyxl")

df_ue = cargar_unidades_ejecutoras()
df_ue["codigo"] = df_ue["codigo"].astype(str).str.strip()
df_ue["NG"] = df_ue["NG"].astype(str).str.strip()

# ================================
# 1) Validar y preparar responsables
# ================================
if "Responsable_Institucional" not in df_ue.columns:
    st.error("❌ Falta la columna 'Responsable_Institucional' en unidades_ejecutoras.xlsx")
    st.stop()

df_ue["Responsable_Institucional"] = (
    df_ue["Responsable_Institucional"]
    .fillna("")
    .astype(str)
    .str.strip()
)

responsables = sorted([r for r in df_ue["Responsable_Institucional"].unique() if r])

# st.image("logo.png", width=160)
#"st.title("Registro de IT del Plan Estratégico Institucional (PEI)")

def get_image_base64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

def render_header():
    logo_base64 = get_image_base64("logo.png")

    html = f"""
<div style="display:flex; align-items:center; gap:16px; margin-top:-10px; padding:6px 0;">
  <img src="data:image/png;base64,{logo_base64}" width="140" style="display:block;">
  <h1 style="margin:0; font-size:2.1rem; font-weight:600; line-height:1.2;">
    Registro de IT del Plan Estratégico Institucional (PEI)
  </h1>
</div>
"""

    st.markdown(dedent(html), unsafe_allow_html=True)


render_header()

#st.markdown("<h1 style='color:red'>PRUEBA</h1>", unsafe_allow_html=True)

st.markdown(
    """
    <style>
    .footer {
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        background-color: #F5F7FA;
        color: #9AA0A6;
        text-align: center;
        padding: 10px 0;
        font-size: 13px;
        border-top: 1px solid #E0E6ED;
        z-index: 100;
    }
    </style>

    <div class="footer">
        App elaborada por la <b>Dirección Nacional de Coordinación y Planeamiento (DNCP)</b> – <b>CEPLAN</b>
    </div>
    """,
    unsafe_allow_html=True
)


# ================================
# 2) Filtro 1: Responsable Institucional
# ================================
#st.subheader("Responsable Institucional")

resp_sel = st.selectbox(
    "Escriba o seleccione el responsable institucional",
    options=responsables,
    index=None,
    placeholder="Escribe el nombre del responsable..."
)

if not resp_sel:
    st.info("Selecciona un responsable para habilitar la búsqueda de Unidades Ejecutoras.")
    st.stop()

# ================================
# 3) Filtrar df_ue por responsable + Filtro 2: UE (código o nombre)
# ================================
df_ue_filtrado = df_ue[df_ue["Responsable_Institucional"] == resp_sel].copy() 

st.caption(f"Unidades ejecutoras asignadas: {len(df_ue_filtrado)}") 

if df_ue_filtrado.empty: 
    st.warning("No hay unidades ejecutoras asociadas a este responsable.") 
    st.stop() 

# Crear opciones combinadas para búsqueda (solo del filtrado) 
opciones = [ 
    f"{str(row['codigo']).strip()} - {str(row['nombre']).strip()}" for _, row in df_ue_filtrado.iterrows() ] 
seleccion = st.selectbox( 
    "Escriba o seleccione el código o nombre del pliego", 
    opciones, 
    index=None, 
    placeholder="Escribe el código o nombre..." 
)


# ================================
# Opciones
# ================================
if seleccion:
    codigo = seleccion.split(" - ")[0].strip()

    # 4) Ajuste: usar df_ue_filtrado en vez de df_ue
    fila = df_ue_filtrado[df_ue_filtrado["codigo"] == codigo]

    if not fila.empty:
        sector = fila["sector"].iloc[0] if "sector" in fila.columns else ""
        nivel_gob = fila["NG"].iloc[0]
        responsable = fila["Responsable_Institucional"].iloc[0] if "Responsable_Institucional" in fila.columns else "No registrado"

        st.markdown(
            f"""
            <div style="
                padding: 14px 18px;
                border-radius: 10px;
                background-color: #F5F7FA;
                margin-top: 10px;
                border: 1px solid #E0E6ED;
                font-size: 14px;
                color: #333;
            ">
                <div><strong>Sector:</strong> {sector}</div>
                <div><strong>Nivel de gobierno:</strong> {nivel_gob}</div>
                <div><strong>Responsable institucional:</strong> {responsable}</div>
            </div>
            """,
            unsafe_allow_html=True
        )


    col1, col2 = st.columns(2)
    with col1:
        if st.button("📂 Historial PEI"):
            st.session_state["modo"] = "historial"

    with col2:
        if st.button("📝 Nuevo registro"):
            st.session_state["modo"] = "nuevo"
            reset_form_state()
            st.rerun()

engine = create_engine(st.secrets["postgres"]["url"], pool_pre_ping=True)

# ================================
# Procesamiento según opción
# ================================
if "modo" in st.session_state and seleccion:
    codigo = seleccion.split(" - ")[0].strip()

    # 4) Normalizador de código (robusto)
    def normalizar_codigo(x):
        if pd.isna(x) or x is None:
            return ""
        try:
            return str(int(float(x)))
        except Exception:
            return str(x).strip()

    # ================================
    # MODO: HISTORIAL (POSTGRES)
    # ================================
    if st.session_state["modo"] == "historial":
        codigo_norm = normalizar_codigo(codigo)

        try:
            # 1) Leer historial desde Postgres filtrado por Id_UE (SIN columna extra)
            #    Importante: aquí el equivalente de tu columna "codigo" es "id_ue"
            query = """
                SELECT
                  anio,
                  ng1,
                  ng2,
                  fecha_recepcion,
                  periodo_pei,
                  vigencia,
                  tipo_pei,
                  estado,
                  responsable_institucional,
                  cantidad_revisiones,
                  fecha_derivacion,
                  etapas_revision,
                  comentario_adicional_emisor_it,
                  articulacion,
                  expediente,
                  fecha_it,
                  numero_it,
                  fecha_oficio,
                  numero_oficio,
                  created_at
                FROM it_pei_historial
                WHERE id_ue = %(id_ue)s
                ORDER BY fecha_recepcion DESC NULLS LAST, created_at DESC
            """

            df_historial = pd.read_sql(query, con=engine, params={"id_ue": codigo_norm})

        except Exception as e:
            st.error(f"❌ Error al consultar el historial desde Postgres: {e}")
            st.stop()

        st.write("Filas encontradas para este pliego:", len(df_historial))

        if df_historial.empty:
            st.info("No existe historial para este pliego (según la clave de comparación).")

        else:
            # 6) Preparar fecha para identificar último registro
            if "fecha_recepcion" in df_historial.columns:
                df_historial["fecha_recepcion"] = pd.to_datetime(
                    df_historial["fecha_recepcion"], errors="coerce"
                )

            # 7) Mostrar últimas filas (ya vienen ordenadas DESC, pero conservamos tu lógica)
            st.dataframe(
                df_historial.tail(5),
                use_container_width=True,
                hide_index=True
            )

            # 8) Detectar último registro
            #    Nota: la consulta ya ordena por fecha_recepcion DESC y created_at DESC.
            #    Aun así, dejamos tu fallback.
            if "fecha_recepcion" in df_historial.columns:
                ultimo = df_historial.sort_values(
                    "fecha_recepcion", ascending=False
                ).iloc[0]
            else:
                ultimo = df_historial.iloc[0]

            st.success("Último registro encontrado.")

            # 9) Cargar último registro al formulario
            colx, coly = st.columns([1, 2])
            with colx:
                if st.button(
                    "⬇️ Cargar último registro disponible al formulario",
                    type="primary"
                ):
                    init_form_state()
                    set_form_state_from_row(ultimo)  # usa tus keys actuales
                    st.session_state["modo"] = "nuevo"
                    st.rerun()

    # ================================
    # MODO: NUEVO
    # ================================
    elif st.session_state["modo"] == "nuevo":
        st.subheader("📝 Crear nuevo registro PEI")

        init_form_state()
        form = st.session_state[FORM_STATE_KEY]

        with st.form("form_pei"):

            st.write("## Datos de identificación y revisión")

            col1, col2, col3, col4 = st.columns([1, 1, 1.3, 1])

            with col1:
                year_now = datetime.now().year
                año = st.text_input("Año", value=str(year_now), disabled=True)

                tipo_pei_opts = ["Formulado", "Ampliado", "Actualizado"]
                tipo_pei = st.selectbox(
                    "Tipo de PEI",
                    tipo_pei_opts,
                    index=index_of(tipo_pei_opts, form["tipo_pei"], 0)
                )

                etapas_opts = [
                    "IT Emitido",
                    "Para emisión de IT",
                    "Revisión DNCP",
                    "Revisión DNSE",
                    "Revisión DNPE",
                    "Subsanación del pliego"
                ]
                etapa_revision = st.selectbox(
                    "Etapas de revisión",
                    etapas_opts,
                    index=index_of(etapas_opts, form["etapa_revision"], 0)
                )

            with col2:
                fecha_recepcion = st.date_input(
                    "Fecha de recepción",
                    value=form["fecha_recepcion"] if form["fecha_recepcion"] else datetime.now().date()
                )

                # 4) Ajuste: nivel desde df_ue_filtrado
                nivel = df_ue_filtrado.loc[df_ue_filtrado["codigo"] == codigo, "NG"].values[0]

                if nivel == "Gobierno regional":
                    opciones_articulacion = ["PEDN 2050", "PDRC"]
                elif nivel == "Gobierno nacional":
                    opciones_articulacion = ["PEDN 2050", "PESEM NO vigente", "PESEM vigente"]
                elif nivel in ["Municipalidad distrital", "Municipalidad provincial"]:
                    opciones_articulacion = ["PEDN 2050", "PDRC", "PDLC Provincial", "PDLC Distrital"]
                else:
                    opciones_articulacion = []

                articulacion = st.selectbox(
                    "Articulación",
                    opciones_articulacion,
                    index=index_of(opciones_articulacion, form["articulacion"], 0) if opciones_articulacion else 0
                )

                fecha_derivacion = st.date_input(
                    "Fecha de derivación",
                    value=form["fecha_derivacion"] if form["fecha_derivacion"] else datetime.now().date()
                )

            with col3:
                periodo = st.text_input(
                    "Periodo PEI (ej: 2025-2027)",
                    value=form["periodo"]
                )

                pattern = r"^\d{4}-\d{4}$"
                if periodo and not re.match(pattern, periodo):
                    st.error("⚠️ Formato inválido. Usa el formato: 2025-2027")

                cantidad_revisiones = st.number_input(
                    "Cantidad de revisiones",
                    min_value=0,
                    step=1,
                    value=int(form["cantidad_revisiones"] or 0)
                )

                comentario = st.text_area(
                    "Comentario adicional / Emisor de IT",
                    height=140,
                    value=form["comentario"]
                )

            with col4:
                vigencia_opts = ["Sí", "No"]
                vigencia = st.selectbox(
                    "Vigencia",
                    vigencia_opts,
                    index=index_of(vigencia_opts, form["vigencia"], 0)
                )

                estado_opts = ["En proceso", "Emitido"]
                estado = st.selectbox(
                    "Estado",
                    estado_opts,
                    index=index_of(estado_opts, form["estado"], 0)
                )

            st.write("## Datos del Informe Técnico")

            colA, colB, colC = st.columns(3)

            with colA:
                expediente = st.text_input("Expediente (SGD)", value=form["expediente"])

            with colB:
                fecha_it = st.date_input(
                    "Fecha de I.T",
                    value=form["fecha_it"] if form["fecha_it"] else datetime.now().date()
                )
                fecha_oficio = st.date_input(
                    "Fecha del Oficio",
                    value=form["fecha_oficio"] if form["fecha_oficio"] else datetime.now().date()
                )

            with colC:
                numero_it = st.text_input("Número de I.T", value=form["numero_it"])
                numero_oficio = st.text_input("Número del Oficio", value=form["numero_oficio"])

            expediente_ok = bool(str(expediente).strip())
            fecha_it_ok = fecha_it is not None
            numero_it_ok = bool(str(numero_it).strip())
            puede_emitir = expediente_ok and fecha_it_ok and numero_it_ok

            if estado == "Emitido" and not puede_emitir:
                st.caption(
                    "⚠️ Para marcar como *Emitido* debes registrar: "
                    "Expediente (SGD), Fecha de I.T y Número de I.T."
                )

            submitted = st.form_submit_button("💾 Guardar Registro")

            if submitted:
                if estado == "Emitido" and not puede_emitir:
                    st.error("❌ No se puede guardar como 'Emitido'. Completa Expediente (SGD), Fecha de I.T y Número de I.T.")
                    st.stop()

                nombre_ue = seleccion.split(" - ")[1].strip()

                # responsable ya viene del bloque de tarjeta, pero por seguridad:
                responsable_actual = resp_sel

                nuevo_sharepoint = {
                    "codigo": codigo,
                    "nombre": nombre_ue,
                    "año": año,
                    "periodo": periodo,
                    "vigencia": vigencia,
                    "tipo_pei": tipo_pei,
                    "estado": estado,
                    "responsable_institucional": responsable_actual,
                    "cantidad_revisiones": cantidad_revisiones,
                    "fecha_recepcion": str(fecha_recepcion),
                    "fecha_derivacion": str(fecha_derivacion),
                    "etapa_revision": etapa_revision,
                    "comentario": comentario,
                    "articulacion": articulacion,
                    "expediente": expediente,
                    "fecha_it": str(fecha_it),
                    "numero_it": numero_it,
                    "fecha_oficio": str(fecha_oficio),
                    "numero_oficio": numero_oficio,
                }

                try:
                    sharepoint_append_row_to_excel(st.secrets, nuevo_sharepoint)
                    st.success("✅ Registro guardado en el historial.")
                    st.session_state["modo"] = "historial"
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al guardar en el Excel: {e}")

                st.write("📄 HISTORIAL_PATH:", HISTORIAL_PATH)
                st.write("📍 Ruta absoluta:", os.path.abspath(HISTORIAL_PATH))

                if os.path.exists(HISTORIAL_PATH):
                    st.write("✅ Existe en este entorno.")
                    st.write("🕒 Última modificación (mtime):", datetime.fromtimestamp(os.path.getmtime(HISTORIAL_PATH)))
                    st.write("📦 Tamaño (bytes):", os.path.getsize(HISTORIAL_PATH))
                else:
                    st.write("❌ No existe en este entorno.")
