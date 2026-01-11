import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import calendar
import sqlite3
import os
import numpy as np
import io

# Configurar pandas para manejar más celdas en el styler
pd.set_option("styler.render.max_elements", 500000)

st.set_page_config(page_title="OTIF Proveedores - KAVE HOME", page_icon="📦", layout="wide")

# Ruta de la base de datos
DB_PATH = "proveedores.db"

# CSS personalizado para diseño moderno
st.markdown("""
<style>
    /* Fondo general */
    .stApp {
        background-color: white;
    }
    
    /* Asegurar que todo el texto sea oscuro y visible */
    .stApp * {
        color: #3D3D3D;
    }
    
    /* Header personalizado */
    .main-header {
        background: linear-gradient(135deg, #D4C5B9 0%, #B8A898 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .main-title {
        color: white !important;
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
        text-align: center;
    }
    
    .subtitle {
        color: white !important;
        font-size: 1.2rem;
        text-align: center;
        margin-top: 0.5rem;
    }
    
    /* Métricas personalizadas */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #5B7C8D;
        margin-bottom: 1rem;
    }
    
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #3D3D3D !important;
        margin: 0;
    }
    
    .metric-label {
        font-size: 0.9rem;
        color: #8B7355 !important;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 0.5rem;
    }
    
    /* Botones */
    .stButton>button {
        background-color: #5B7C8D;
        color: white;
        border-radius: 8px;
        padding: 0.5rem 2rem;
        border: none;
        font-weight: 600;
        transition: all 0.3s;
    }
    
    .stButton>button:hover {
        background-color: #4A6A7A;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: white;
        border-right: 3px solid #D4C5B9;
    }
    
    [data-testid="stSidebar"] .stMarkdown h3 {
        color: #5B7C8D;
        font-weight: 700;
        font-size: 1.1rem;
    }
    
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stMarkdown li {
        color: #3D3D3D;
        font-size: 0.95rem;
    }
    
    [data-testid="stSidebar"] label {
        color: #3D3D3D !important;
        font-weight: 500;
    }
    
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stDateInput label {
        color: #3D3D3D !important;
    }
    
    /* Tabs personalizados */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: white;
        border-radius: 8px 8px 0 0;
        padding: 12px 24px;
        color: #3D3D3D !important;
        font-weight: 600;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #5B7C8D;
        color: white !important;
    }
    
    /* Asegurar texto visible en todo Streamlit */
    .stMarkdown, .stMarkdown p, .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        color: #3D3D3D !important;
    }
    
    /* Info boxes */
    .stAlert {
        color: #3D3D3D !important;
    }
    
    /* Footer */
    .footer {
        text-align: right;
        padding: 1rem;
        color: #8B7355;
        font-size: 0.9rem;
        margin-top: 2rem;
    }
    
    /* Filtros */
    .filter-section {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
</style>
""", unsafe_allow_html=True)

# Funciones de base de datos
def init_db():
    """Inicializa la base de datos de proveedores"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Crear tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS proveedores (
            codigo INTEGER PRIMARY KEY,
            nombre TEXT NOT NULL,
            alias TEXT,
            tipo TEXT,
            responsable_compras TEXT,
            centro_responsabilidad TEXT,
            almacen TEXT,
            email TEXT,
            fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Verificar si la columna email existe, si no, añadirla
    cursor.execute("PRAGMA table_info(proveedores)")
    columns = [column[1] for column in cursor.fetchall()]
    
    if 'email' not in columns:
        try:
            cursor.execute('ALTER TABLE proveedores ADD COLUMN email TEXT')
            conn.commit()
            print("✅ Columna 'email' añadida a la tabla proveedores")
        except Exception as e:
            print(f"Nota: {e}")
    
    conn.commit()
    conn.close()

def cargar_proveedores_desde_excel(df_excel):
    """Carga proveedores desde un DataFrame de Excel a la base de datos"""
    conn = sqlite3.connect(DB_PATH)
    
    # Limpiar tabla existente
    conn.execute('DELETE FROM proveedores')
    
    # Insertar nuevos proveedores
    for _, row in df_excel.iterrows():
        conn.execute('''
            INSERT OR REPLACE INTO proveedores 
            (codigo, nombre, alias, tipo, responsable_compras, centro_responsabilidad, almacen, email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            int(row['Nº']),
            str(row['Nombre']),
            str(row.get('Alias', '')) if pd.notna(row.get('Alias')) else '',
            str(row.get('Tipo Proveedor', '')) if pd.notna(row.get('Tipo Proveedor')) else '',
            str(row.get('Responsable compras', '')) if pd.notna(row.get('Responsable compras')) else '',
            str(row.get('Centro responsabilidad', '')) if pd.notna(row.get('Centro responsabilidad')) else '',
            str(row.get('Cód. almacén', '')) if pd.notna(row.get('Cód. almacén')) else '',
            str(row.get('Correo electrónico', '')) if pd.notna(row.get('Correo electrónico')) else ''
        ))
    
    conn.commit()
    conn.close()

def obtener_todos_proveedores():
    """Obtiene todos los proveedores de la base de datos"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query('SELECT * FROM proveedores', conn)
        conn.close()
        return df
    except:
        return pd.DataFrame()

def obtener_nombre_proveedor(codigo):
    """Obtiene el nombre de un proveedor por su código"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT nombre, alias FROM proveedores WHERE codigo = ?', (int(codigo),))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            nombre = result[0]
            alias = result[1]
            return f"{alias}" if alias else nombre
        else:
            return f"Proveedor {codigo}"
    except:
        return f"Proveedor {codigo}"

def obtener_email_proveedor(codigo):
    """Obtiene el email de un proveedor por su código"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT email FROM proveedores WHERE codigo = ?', (int(codigo),))
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            return result[0]
        else:
            return None
    except:
        return None

def generar_reporte_proveedor_html(nombre_proveedor, df_pedidos, metricas, imagen_base64):
    """Genera el HTML del reporte para el proveedor con diseño mejorado"""
    
    # Separar pedidos por estado
    no_entregados = df_pedidos[df_pedidos['Estado'] == 'NO ENTREGADO']
    atrasados = df_pedidos[df_pedidos['Estado'].isin(['ENTREGADO TARDE', 'EXCEPCIÓN (2 DÍAS TARDE)'])]
    entregados = df_pedidos[df_pedidos['Es OTIF'] == True]
    
    # Determinar color según OTIF
    if metricas['otif_pct'] >= 85:
        color_otif = "#5B7C8D"  # Azul (Excelente)
        estado_texto = "EXCELENTE"
    elif metricas['otif_pct'] >= 70:
        color_otif = "#8B9AA5"  # Azul claro (Bueno)
        estado_texto = "BUENO"
    elif metricas['otif_pct'] >= 50:
        color_otif = "#D4C5B9"  # Beige (Regular)
        estado_texto = "MEJORABLE"
    else:
        color_otif = "#8B7355"  # Marrón (Crítico)
        estado_texto = "CRÍTICO"
    
    html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 1000px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f9f9f9;
            }}
            .header {{
                background: linear-gradient(135deg, {color_otif} 0%, #B8A898 100%);
                color: white;
                padding: 40px;
                text-align: center;
                border-radius: 15px;
                margin-bottom: 30px;
                box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
            }}
            .header h1 {{
                margin: 0;
                font-size: 2.5em;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            }}
            .header .periodo {{
                font-size: 1.1em;
                margin-top: 10px;
                opacity: 0.95;
            }}
            .estado-badge {{
                display: inline-block;
                background: white;
                color: {color_otif};
                padding: 10px 25px;
                border-radius: 25px;
                font-weight: bold;
                font-size: 1.2em;
                margin-top: 15px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.2);
            }}
            .metrics {{
                display: flex;
                justify-content: space-around;
                margin: 30px 0;
                flex-wrap: wrap;
                gap: 20px;
            }}
            .metric-box {{
                background: white;
                border-radius: 15px;
                padding: 25px;
                text-align: center;
                min-width: 180px;
                flex: 1;
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                transition: transform 0.3s;
            }}
            .metric-box:hover {{
                transform: translateY(-5px);
                box-shadow: 0 6px 20px rgba(0,0,0,0.15);
            }}
            .metric-value {{
                font-size: 3em;
                font-weight: bold;
                color: {color_otif};
                margin: 10px 0;
            }}
            .metric-label {{
                font-size: 0.95em;
                color: #666;
                text-transform: uppercase;
                letter-spacing: 1px;
            }}
            .metric-icon {{
                font-size: 2em;
                margin-bottom: 10px;
            }}
            .chart-container {{
                text-align: center;
                margin: 40px 0;
                background: white;
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            }}
            .chart-container h3 {{
                color: {color_otif};
                margin-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                background: white;
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                border-radius: 10px;
                overflow: hidden;
            }}
            th {{
                background-color: {color_otif};
                color: white;
                padding: 15px;
                text-align: left;
                font-weight: bold;
                font-size: 0.95em;
            }}
            td {{
                padding: 12px 15px;
                border-bottom: 1px solid #eee;
            }}
            tr:last-child td {{
                border-bottom: none;
            }}
            tr:hover {{
                background-color: #f8f8f8;
            }}
            .section-title {{
                color: {color_otif};
                font-size: 1.8em;
                margin-top: 40px;
                padding-bottom: 10px;
                border-bottom: 3px solid {color_otif};
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            .footer {{
                text-align: center;
                margin-top: 50px;
                padding: 30px;
                background: linear-gradient(135deg, #f5f5f5 0%, #e9e9e9 100%);
                border-radius: 15px;
                color: #666;
            }}
            .footer-logo {{
                font-size: 1.5em;
                font-weight: bold;
                color: {color_otif};
                margin-bottom: 10px;
            }}
            .alert {{
                background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
                border-left: 5px solid #ffc107;
                padding: 20px;
                margin: 25px 0;
                border-radius: 10px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }}
            .alert-title {{
                font-weight: bold;
                font-size: 1.2em;
                margin-bottom: 10px;
                color: #856404;
            }}
            .priority-high {{
                background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%);
                border-left: 5px solid #dc3545;
            }}
            .priority-high .alert-title {{
                color: #721c24;
            }}
            .icon {{
                display: inline-block;
                margin-right: 8px;
            }}
            .badge {{
                display: inline-block;
                padding: 5px 12px;
                border-radius: 12px;
                font-size: 0.85em;
                font-weight: bold;
                margin-left: 8px;
            }}
            .badge-danger {{
                background: #dc3545;
                color: white;
            }}
            .badge-warning {{
                background: #ffc107;
                color: #333;
            }}
            .badge-success {{
                background: #28a745;
                color: white;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📦 REPORTE OTIF</h1>
            <h2>{nombre_proveedor}</h2>
            <div class="periodo">📅 Período: {df_pedidos['Fecha Esperada'].min().strftime('%d/%m/%Y')} - {df_pedidos['Fecha Esperada'].max().strftime('%d/%m/%Y')}</div>
            <div class="estado-badge">{estado_texto}</div>
        </div>
        
        <div class="metrics">
            <div class="metric-box">
                <div class="metric-icon">📋</div>
                <div class="metric-value">{len(df_pedidos)}</div>
                <div class="metric-label">Total Pedidos</div>
            </div>
            <div class="metric-box">
                <div class="metric-icon">{'✅' if metricas['otif_pct'] >= 70 else '⚠️'}</div>
                <div class="metric-value">{metricas['otif_pct']:.1f}%</div>
                <div class="metric-label">% OTIF</div>
            </div>
            <div class="metric-box">
                <div class="metric-icon">❌</div>
                <div class="metric-value">{len(no_entregados)}</div>
                <div class="metric-label">No Entregados</div>
            </div>
            <div class="metric-box">
                <div class="metric-icon">⏰</div>
                <div class="metric-value">{len(atrasados)}</div>
                <div class="metric-label">Atrasados</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h3>📊 Análisis Visual de Cumplimiento</h3>
            <img src="data:image/png;base64,{imagen_base64}" alt="Gráfico OTIF" style="max-width: 100%; height: auto; border-radius: 10px;">
        </div>
    """
    
    # Pedidos NO ENTREGADOS
    if len(no_entregados) > 0:
        html += f"""
        <h2 class="section-title"><span class="icon">❌</span> Pedidos NO ENTREGADOS<span class="badge badge-danger">{len(no_entregados)}</span></h2>
        <div class="alert priority-high">
            <div class="alert-title">⚠️ ACCIÓN REQUERIDA</div>
            Los siguientes pedidos están pendientes de entrega. Por favor, priorice su envío.
        </div>
        <table>
            <tr>
                <th>Nº Documento</th>
                <th>Artículo</th>
                <th>Descripción</th>
                <th>Fecha Esperada</th>
                <th>Cantidad Pendiente</th>
                <th>Días Retraso</th>
            </tr>
        """
        for _, pedido in no_entregados.iterrows():
            dias_retraso = (datetime.now().date() - pedido['Fecha Esperada'].date()).days if pd.notna(pedido['Fecha Esperada']) else 0
            color_fila = '#ffebee' if dias_retraso > 30 else '#fff9e6' if dias_retraso > 15 else ''
            html += f"""
            <tr style="background-color: {color_fila}">
                <td><strong>{pedido['Nº documento']}</strong></td>
                <td>{pedido['Nº Artículo']}</td>
                <td>{pedido['Descripción']}</td>
                <td>{pedido['Fecha Esperada'].strftime('%d/%m/%Y')}</td>
                <td>{pedido['Cantidad Pendiente']:.0f}</td>
                <td><strong style="color: {'#d32f2f' if dias_retraso > 30 else '#f57c00' if dias_retraso > 15 else '#333'}">{dias_retraso} días</strong></td>
            </tr>
            """
        html += "</table>"
    
    # Pedidos ATRASADOS
    if len(atrasados) > 0:
        html += f"""
        <h2 class="section-title"><span class="icon">⚠️</span> Pedidos ATRASADOS<span class="badge badge-warning">{len(atrasados)}</span></h2>
        <div class="alert">
            <div class="alert-title">📋 PARA SU CONOCIMIENTO</div>
            Estos pedidos se entregaron con retraso. Le pedimos mejorar la puntualidad en futuros envíos.
        </div>
        <table>
            <tr>
                <th>Nº Documento</th>
                <th>Artículo</th>
                <th>Fecha Esperada</th>
                <th>Fecha Real</th>
                <th>Días Diferencia</th>
                <th>Estado</th>
            </tr>
        """
        for _, pedido in atrasados.iterrows():
            html += f"""
            <tr>
                <td><strong>{pedido['Nº documento']}</strong></td>
                <td>{pedido['Nº Artículo']}</td>
                <td>{pedido['Fecha Esperada'].strftime('%d/%m/%Y')}</td>
                <td>{pedido['Fecha Real'].strftime('%d/%m/%Y') if pd.notna(pedido['Fecha Real']) else 'N/A'}</td>
                <td><strong style="color: #f57c00">+{pedido['Días Diferencia']} días</strong></td>
                <td>{pedido['Estado']}</td>
            </tr>
            """
        html += "</table>"
    
    # Pedidos ENTREGADOS CORRECTAMENTE
    if len(entregados) > 0:
        html += f"""
        <h2 class="section-title"><span class="icon">✅</span> Pedidos ENTREGADOS CORRECTAMENTE<span class="badge badge-success">{len(entregados)}</span></h2>
        <p style="color: #28a745; font-weight: bold; margin: 20px 0;">¡Excelente trabajo! Estos pedidos cumplieron con los plazos establecidos.</p>
        <table>
            <tr>
                <th>Nº Documento</th>
                <th>Artículo</th>
                <th>Fecha Esperada</th>
                <th>Fecha Real</th>
                <th>Cantidad</th>
            </tr>
        """
        # Mostrar solo los primeros 10
        for _, pedido in entregados.head(10).iterrows():
            html += f"""
            <tr>
                <td><strong>{pedido['Nº documento']}</strong></td>
                <td>{pedido['Nº Artículo']}</td>
                <td>{pedido['Fecha Esperada'].strftime('%d/%m/%Y')}</td>
                <td>{pedido['Fecha Real'].strftime('%d/%m/%Y') if pd.notna(pedido['Fecha Real']) else 'N/A'}</td>
                <td>{pedido['Cantidad Total']:.0f}</td>
            </tr>
            """
        if len(entregados) > 10:
            html += f"""<tr><td colspan='5' style='text-align:center; font-style:italic; padding: 15px; background: #f8f9fa;'>
            ✨ ... y {len(entregados) - 10} pedidos más cumplieron correctamente</td></tr>"""
        html += "</table>"
    
    html += f"""
        <div class="footer">
            <div class="footer-logo">🏠 KAVE HOME</div>
            <p style="font-size: 1.1em; margin: 10px 0;"><strong>Planning Department</strong></p>
            <p style="margin: 15px 0;">Este es un reporte automático del sistema de medición OTIF</p>
            <p style="color: #999; font-size: 0.9em; margin-top: 20px;">
                Para cualquier consulta o aclaración, por favor contacte con su responsable de compras
            </p>
            <p style="margin-top: 15px; font-size: 0.85em; color: #999;">
                📧 Generado automáticamente el {datetime.now().strftime('%d/%m/%Y a las %H:%M')}
            </p>
        </div>
    </body>
    </html>
    """
    
    return html

def hay_proveedores_en_bd():
    """Verifica si hay proveedores en la base de datos"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM proveedores')
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except:
        return False

# Inicializar base de datos
init_db()

def calcular_otif(df):
    """Calcula el OTIF según la lógica del diagrama de flujo - VERSIÓN OPTIMIZADA"""
    
    # Crear DataFrame de resultados usando operaciones vectorizadas
    df_result = pd.DataFrame()
    
    # Copiar columnas necesarias
    df_result['Nº documento'] = df['Nº documento']
    df_result['Código Proveedor'] = df['Compra a-Nº proveedor']
    df_result['Nº Artículo'] = df['Nº']
    df_result['Descripción'] = df['Descripción']
    df_result['Almacén'] = df['Cód. almacén']
    
    # Convertir fechas
    df_result['Fecha Esperada'] = pd.to_datetime(df['Fecha recepción esperada'])
    df_result['Fecha Real'] = pd.to_datetime(df['Fecha recepción real'])
    df_result['Fecha Pedido'] = pd.to_datetime(df['Fecha pedido'])
    
    # Cantidades
    df_result['Cantidad Total'] = df['Cantidad (base)']
    df_result['Cantidad Pendiente'] = df['Cdad. pendiente (base)']
    df_result['Cantidad Entregada'] = df_result['Cantidad Total'] - df_result['Cantidad Pendiente']
    df_result['Coste Unitario'] = df['Coste unit. directo excl. IVA']
    
    # Calcular días diferencia
    df_result['Días Diferencia'] = (df_result['Fecha Real'] - df_result['Fecha Esperada']).dt.days
    df_result['Días Diferencia'] = df_result['Días Diferencia'].fillna(0).astype(int)
    
    # Determinar si está completo
    entregado_completo = df_result['Cantidad Pendiente'] == 0
    tiene_fecha_real = df_result['Fecha Real'].notna()
    
    # Calcular estado usando condiciones vectorizadas
    condiciones = [
        ~tiene_fecha_real & entregado_completo,  # SIN FECHA REAL (COMPLETO)
        ~tiene_fecha_real & ~entregado_completo,  # NO ENTREGADO
        tiene_fecha_real & ~entregado_completo,  # NO ENTREGADO
        tiene_fecha_real & entregado_completo & (df_result['Días Diferencia'] == 0),  # OTIF
        tiene_fecha_real & entregado_completo & (df_result['Días Diferencia'] > 0) & (df_result['Días Diferencia'] <= 2),  # EXCEPCIÓN
        tiene_fecha_real & entregado_completo & (df_result['Días Diferencia'] > 2),  # ENTREGADO TARDE
        tiene_fecha_real & entregado_completo & (df_result['Días Diferencia'] < 0),  # ENTREGADO ANTES
    ]
    
    estados = [
        'SIN FECHA REAL (COMPLETO)',
        'NO ENTREGADO',
        'NO ENTREGADO',
        'OTIF',
        'EXCEPCIÓN (2 DÍAS TARDE)',
        'ENTREGADO TARDE',
        'ENTREGADO ANTES'
    ]
    
    df_result['Estado'] = np.select(condiciones, estados, default='NO ENTREGADO')
    
    # Calcular Es OTIF
    df_result['Es OTIF'] = df_result['Estado'].isin(['OTIF', 'EXCEPCIÓN (2 DÍAS TARDE)'])
    
    # Obtener nombres de proveedores en batch (mucho más rápido)
    codigos_unicos = df_result['Código Proveedor'].unique()
    nombres_dict = {}
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    placeholders = ','.join(['?'] * len(codigos_unicos))
    cursor.execute(f'SELECT codigo, nombre, alias FROM proveedores WHERE codigo IN ({placeholders})', 
                   tuple(int(c) for c in codigos_unicos))
    
    for codigo, nombre, alias in cursor.fetchall():
        nombres_dict[codigo] = alias if alias else nombre
    conn.close()
    
    # Mapear nombres
    df_result['Proveedor'] = df_result['Código Proveedor'].map(
        lambda x: nombres_dict.get(int(x), f"Proveedor {x}")
    )
    
    return df_result

def calcular_metricas_proveedor(df_otif):
    """Calcula métricas de OTIF por proveedor"""
    metricas = df_otif.groupby('Proveedor').agg({
        'Es OTIF': ['sum', 'count'],
        'Cantidad Total': 'sum',
        'Cantidad Entregada': 'sum',
        'Días Diferencia': 'mean'
    }).reset_index()
    
    metricas.columns = ['Proveedor', 'OTIF Count', 'Total Pedidos', 
                        'Cantidad Total', 'Cantidad Entregada', 'Días Diferencia Promedio']
    
    metricas['% OTIF'] = (metricas['OTIF Count'] / metricas['Total Pedidos'] * 100).round(2)
    metricas['% Fill Rate'] = (metricas['Cantidad Entregada'] / metricas['Cantidad Total'] * 100).round(2)
    
    return metricas

def calcular_evolucion_mensual(df_otif):
    """Calcula la evolución del OTIF mes a mes"""
    df_otif['Año-Mes'] = df_otif['Fecha Esperada'].dt.to_period('M').astype(str)
    
    evolucion = df_otif.groupby('Año-Mes').agg({
        'Es OTIF': ['sum', 'count']
    }).reset_index()
    
    evolucion.columns = ['Año-Mes', 'OTIF Count', 'Total Pedidos']
    evolucion['% OTIF'] = (evolucion['OTIF Count'] / evolucion['Total Pedidos'] * 100).round(2)
    
    return evolucion

def calcular_evolucion_por_proveedor(df_otif, top_n=10):
    """Calcula la evolución del OTIF por proveedor a lo largo del tiempo"""
    top_proveedores = df_otif.groupby('Proveedor')['Proveedor'].count().sort_values(ascending=False).head(top_n).index
    df_top = df_otif[df_otif['Proveedor'].isin(top_proveedores)].copy()
    df_top['Año-Mes'] = df_top['Fecha Esperada'].dt.to_period('M').astype(str)
    
    evolucion = df_top.groupby(['Proveedor', 'Año-Mes']).agg({
        'Es OTIF': ['sum', 'count']
    }).reset_index()
    
    evolucion.columns = ['Proveedor', 'Año-Mes', 'OTIF Count', 'Total Pedidos']
    evolucion['% OTIF'] = (evolucion['OTIF Count'] / evolucion['Total Pedidos'] * 100).round(2)
    
    return evolucion

def crear_grafico_pastel_proveedor(df_proveedor, nombre_proveedor):
    """Crea un gráfico de pastel elegante para un proveedor específico"""
    estado_counts = df_proveedor['Estado'].value_counts()
    
    # Colores KAVE HOME
    colores = {
        'OTIF': '#5B7C8D',
        'ADELANTADO': '#8B9AA5',
        'EXCEPCIÓN (2 DÍAS TARDE)': '#D4C5B9',
        'ENTREGADO TARDE': '#8B7355',
        'NO ENTREGADO': '#3D3D3D',
        'ENTREGADO ANTES': '#8B9AA5',
        'SIN FECHA REAL (COMPLETO)': '#B8A898'
    }
    
    colors_list = [colores.get(estado, '#CCCCCC') for estado in estado_counts.index]
    
    fig = go.Figure(data=[go.Pie(
        labels=estado_counts.index,
        values=estado_counts.values,
        hole=0.4,
        marker=dict(colors=colors_list, line=dict(color='white', width=2)),
        textposition='inside',
        textinfo='label+percent',
        textfont=dict(color='white', size=12, family='Arial Black'),
        insidetextorientation='horizontal',
        hovertemplate='<b>%{label}</b><br>Cantidad: %{value}<br>%{percent}<extra></extra>'
    )])
    
    # Calcular OTIF
    total = len(df_proveedor)
    otif = df_proveedor['Es OTIF'].sum()
    otif_pct = (otif / total * 100) if total > 0 else 0
    
    fig.update_layout(
        title={
            'text': f'<b>{nombre_proveedor}</b><br><sup>OTIF {otif_pct:.1f}%</sup>',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16, 'color': '#3D3D3D'}
        },
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.05,
            font=dict(size=10, color='#3D3D3D')
        ),
        height=350,
        margin=dict(l=20, r=20, t=80, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#3D3D3D')
    )
    
    return fig

# Header personalizado
st.markdown("""
<div class="main-header">
    <h1 class="main-title">📦 OTIF - Medición de Proveedores</h1>
    <p class="subtitle">KAVE HOME | Planning Department</p>
</div>
""", unsafe_allow_html=True)

# Sidebar con estilo
with st.sidebar:
    st.markdown("### ⚙️ Configuración")
    
    # Gestión de proveedores
    with st.expander("📋 Gestión de Proveedores", expanded=not hay_proveedores_en_bd()):
        if hay_proveedores_en_bd():
            df_proveedores = obtener_todos_proveedores()
            st.success(f"✅ {len(df_proveedores)} proveedores en base de datos")
            
            if st.button("🔄 Actualizar lista de proveedores"):
                st.session_state['actualizar_proveedores'] = True
        else:
            st.warning("⚠️ No hay proveedores cargados")
        
        if not hay_proveedores_en_bd() or st.session_state.get('actualizar_proveedores', False):
            uploaded_proveedores = st.file_uploader(
                "Cargar Excel de proveedores",
                type=['xlsx', 'xls'],
                help="Archivo con columnas: Nº (código) y Nombre",
                key="proveedores_uploader"
            )
            
            if uploaded_proveedores:
                try:
                    df_prov = pd.read_excel(uploaded_proveedores)
                    if 'Nº' in df_prov.columns and 'Nombre' in df_prov.columns:
                        cargar_proveedores_desde_excel(df_prov)
                        st.success(f"✅ {len(df_prov)} proveedores cargados correctamente")
                        st.session_state['actualizar_proveedores'] = False
                        st.rerun()
                    else:
                        st.error("❌ El archivo debe tener columnas 'Nº' y 'Nombre'")
                except Exception as e:
                    st.error(f"❌ Error al cargar proveedores: {str(e)}")
    
    st.markdown("---")
    
    # Configuración de email para envío directo
    with st.expander("📧 Email para Envío Directo", expanded=False):
        st.markdown("**Configura una vez tu email:**")
        
        email_usuario = st.text_input(
            "Tu email:",
            value=st.session_state.get('email_user', ''),
            placeholder="tu_email@kavehome.com"
        )
        
        password_usuario = st.text_input(
            "Contraseña:",
            type="password",
            help="Para Gmail usa App Password",
            value=st.session_state.get('email_pass', '')
        )
        
        col1, col2 = st.columns(2)
        with col1:
            servidor_smtp = st.selectbox(
                "Servidor:",
                options=['smtp.office365.com', 'smtp.gmail.com', 'smtp.mail.yahoo.com', 'Otro'],
                index=0 if st.session_state.get('smtp_server', '') == 'smtp.office365.com' else 1
            )
            if servidor_smtp == 'Otro':
                servidor_smtp = st.text_input("Servidor SMTP:", value="smtp.office365.com")
        
        with col2:
            puerto_smtp = st.number_input(
                "Puerto:",
                value=587,
                min_value=1,
                max_value=65535
            )
        
        if st.button("💾 Guardar Email", use_container_width=True):
            if email_usuario and password_usuario:
                st.session_state['email_user'] = email_usuario
                st.session_state['email_pass'] = password_usuario
                st.session_state['smtp_server'] = servidor_smtp
                st.session_state['smtp_port'] = puerto_smtp
                st.success("✅ Email configurado")
            else:
                st.error("❌ Completa email y contraseña")
    
    st.markdown("---")
    uploaded_file = st.file_uploader(
        "Cargar archivo de pedidos (Excel)",
        type=['xlsx', 'xls'],
        help="Sube el archivo Excel con los pedidos de compra"
    )
    
    st.markdown("---")
    st.markdown("### 📊 Criterios OTIF")
    st.markdown("""
    - ✅ **OTIF**: Entregado completo en fecha
    - ✅ **EXCEPCIÓN**: Máximo 2 días tarde
    - ❌ **NO OTIF**: Resto de casos
    """)

if uploaded_file is not None and hay_proveedores_en_bd():
    try:
        # Usar caché para evitar recargar el archivo constantemente
        @st.cache_data(ttl=3600)
        def cargar_archivo(file_bytes):
            return pd.read_excel(io.BytesIO(file_bytes))
        
        # Leer archivo Excel
        file_bytes = uploaded_file.read()
        uploaded_file.seek(0)  # Reset file pointer
        
        with st.spinner('⏳ Cargando archivo...'):
            df = cargar_archivo(file_bytes)
        
        columnas_necesarias = [
            'Nº documento', 'Compra a-Nº proveedor', 'Nº', 'Descripción',
            'Cód. almacén', 'Fecha recepción esperada', 'Fecha recepción real',
            'Fecha pedido', 'Cantidad (base)', 'Cdad. pendiente (base)',
            'Coste unit. directo excl. IVA'
        ]
        
        if all(col in df.columns for col in columnas_necesarias):
            # Calcular OTIF con caché
            @st.cache_data(ttl=3600)
            def calcular_otif_cached(df_hash):
                return calcular_otif(df)
            
            with st.spinner('🔄 Calculando OTIF...'):
                # Crear hash único del dataframe para el caché
                df_hash = hash(tuple(df.values.tobytes()))
                df_otif = calcular_otif_cached(df_hash)
            
            # FILTROS TEMPORALES
            st.sidebar.markdown("---")
            st.sidebar.markdown("### 📅 Filtro de Fechas")
            
            fecha_min = df_otif['Fecha Esperada'].min().date()
            fecha_max = df_otif['Fecha Esperada'].max().date()
            
            # Obtener fecha de hoy y primer día del mes actual
            hoy = datetime.now().date()
            primer_dia_mes = datetime(hoy.year, hoy.month, 1).date()
            
            # Selector rápido de períodos
            periodo_rapido = st.sidebar.selectbox(
                "Período rápido:",
                ["Mes actual", "Personalizado", "Último mes", "Últimos 3 meses", "Últimos 6 meses", "Año actual", "Todo el período"]
            )
            
            # Calcular fechas según selección rápida
            if periodo_rapido == "Mes actual":
                fecha_inicio_default = primer_dia_mes if primer_dia_mes >= fecha_min else fecha_min
                fecha_fin_default = hoy if hoy <= fecha_max else fecha_max
            elif periodo_rapido == "Último mes":
                fecha_inicio_default = hoy - timedelta(days=30)
                fecha_fin_default = hoy if hoy <= fecha_max else fecha_max
            elif periodo_rapido == "Últimos 3 meses":
                fecha_inicio_default = hoy - timedelta(days=90)
                fecha_fin_default = hoy if hoy <= fecha_max else fecha_max
            elif periodo_rapido == "Últimos 6 meses":
                fecha_inicio_default = hoy - timedelta(days=180)
                fecha_fin_default = hoy if hoy <= fecha_max else fecha_max
            elif periodo_rapido == "Año actual":
                fecha_inicio_default = datetime(hoy.year, 1, 1).date()
                fecha_fin_default = hoy if hoy <= fecha_max else fecha_max
            elif periodo_rapido == "Todo el período":
                fecha_inicio_default = fecha_min
                fecha_fin_default = fecha_max
            else:  # Personalizado
                fecha_inicio_default = primer_dia_mes if primer_dia_mes >= fecha_min else fecha_min
                fecha_fin_default = hoy if hoy <= fecha_max else fecha_max
            
            # Mostrar selectores de fecha
            st.sidebar.markdown("**Fecha de Recepción Esperada:**")
            col1, col2 = st.sidebar.columns(2)
            with col1:
                fecha_inicio = st.date_input(
                    "Desde:",
                    value=fecha_inicio_default,
                    min_value=fecha_min,
                    max_value=fecha_max,
                    key="fecha_desde"
                )
            with col2:
                fecha_fin = st.date_input(
                    "Hasta:",
                    value=fecha_fin_default,
                    min_value=fecha_min,
                    max_value=fecha_max,
                    key="fecha_hasta"
                )
            
            # Aplicar filtro de fechas
            df_filtrado = df_otif[
                (df_otif['Fecha Esperada'].dt.date >= fecha_inicio) &
                (df_otif['Fecha Esperada'].dt.date <= fecha_fin)
            ].copy()
            
            # Mostrar info del filtrado
            st.sidebar.info(f"📊 {len(df_filtrado):,} de {len(df_otif):,} pedidos")
            
            # Métricas principales con diseño moderno
            total_pedidos = len(df_filtrado)
            otif_count = df_filtrado['Es OTIF'].sum()
            otif_percentage = (otif_count / total_pedidos * 100) if total_pedidos > 0 else 0
            
            # Calcular datos del mes anterior para comparación
            fecha_fin_mes_anterior = fecha_inicio - timedelta(days=1)
            fecha_inicio_mes_anterior = datetime(fecha_fin_mes_anterior.year, fecha_fin_mes_anterior.month, 1).date()
            
            df_mes_anterior = df_otif[
                (df_otif['Fecha Esperada'].dt.date >= fecha_inicio_mes_anterior) &
                (df_otif['Fecha Esperada'].dt.date <= fecha_fin_mes_anterior)
            ]
            
            if len(df_mes_anterior) > 0:
                otif_mes_anterior = (df_mes_anterior['Es OTIF'].sum() / len(df_mes_anterior) * 100)
                diferencia_otif = otif_percentage - otif_mes_anterior
                mostrar_comparativa = True
            else:
                diferencia_otif = 0
                mostrar_comparativa = False
            
            # Mostrar comparativa
            if mostrar_comparativa:
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); 
                            padding: 20px; border-radius: 10px; margin-bottom: 20px;
                            border-left: 5px solid {'#28a745' if diferencia_otif >= 0 else '#dc3545'};">
                    <h3 style="margin: 0; color: #495057;">📊 Comparativa con Mes Anterior</h3>
                    <div style="display: flex; justify-content: space-around; margin-top: 15px; flex-wrap: wrap;">
                        <div style="text-align: center; padding: 10px;">
                            <div style="font-size: 0.9em; color: #6c757d;">MES ANTERIOR</div>
                            <div style="font-size: 2em; font-weight: bold; color: #6c757d;">{otif_mes_anterior:.1f}%</div>
                            <div style="font-size: 0.85em; color: #6c757d;">{len(df_mes_anterior)} pedidos</div>
                        </div>
                        <div style="text-align: center; padding: 10px;">
                            <div style="font-size: 3em; color: {'#28a745' if diferencia_otif >= 0 else '#dc3545'};">
                                {'↗️' if diferencia_otif > 0 else '↘️' if diferencia_otif < 0 else '➡️'}
                            </div>
                            <div style="font-size: 1.5em; font-weight: bold; color: {'#28a745' if diferencia_otif >= 0 else '#dc3545'};">
                                {'+' if diferencia_otif > 0 else ''}{diferencia_otif:.1f}%
                            </div>
                        </div>
                        <div style="text-align: center; padding: 10px;">
                            <div style="font-size: 0.9em; color: #5B7C8D;">MES ACTUAL</div>
                            <div style="font-size: 2em; font-weight: bold; color: #5B7C8D;">{otif_percentage:.1f}%</div>
                            <div style="font-size: 0.85em; color: #5B7C8D;">{total_pedidos} pedidos</div>
                        </div>
                    </div>
                    <div style="text-align: center; margin-top: 15px; font-size: 0.95em; color: #495057;">
                        {'🎉 <strong>¡Mejora!</strong> El OTIF ha aumentado respecto al mes anterior' if diferencia_otif > 0 else 
                         '⚠️ <strong>Atención:</strong> El OTIF ha disminuido respecto al mes anterior' if diferencia_otif < 0 else
                         '➡️ El OTIF se mantiene igual que el mes anterior'}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <p class="metric-value">{total_pedidos:,}</p>
                    <p class="metric-label">Total Pedidos</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <p class="metric-value">{otif_count:,}</p>
                    <p class="metric-label">OTIF Cumplidos</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                color = "#5B7C8D" if otif_percentage >= 80 else "#8B7355" if otif_percentage >= 60 else "#3D3D3D"
                st.markdown(f"""
                <div class="metric-card" style="border-left-color: {color}">
                    <p class="metric-value" style="color: {color}">{otif_percentage:.1f}%</p>
                    <p class="metric-label">% OTIF Global</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                st.markdown(f"""
                <div class="metric-card">
                    <p class="metric-value">{df_filtrado['Proveedor'].nunique()}</p>
                    <p class="metric-label">Proveedores</p>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Tabs
            tab1, tab2, tab3 = st.tabs([
                "📊 Por Proveedor",
                "📧 Enviar Reportes",
                "⚠️ Reclamaciones"
            ])
            
            with tab1:
                # Solo mostrar análisis por proveedor
                st.markdown("### Análisis por Proveedor")
                
                # Obtener top proveedores
                metricas_proveedor = calcular_metricas_proveedor(df_filtrado)
                top_proveedores = metricas_proveedor.nlargest(12, 'Total Pedidos')
                
                # Opción para mostrar más o menos gráficos
                num_graficos = st.select_slider(
                    "Número de gráficos a mostrar:",
                    options=[6, 9, 12, 15, 20],
                    value=12
                )
                
                top_proveedores = metricas_proveedor.nlargest(num_graficos, 'Total Pedidos')
                
                # Crear grid de gráficos de pastel (3 columnas)
                num_cols = 3
                num_proveedores = len(top_proveedores)
                
                with st.spinner(f'Generando {num_proveedores} gráficos...'):
                    for i in range(0, num_proveedores, num_cols):
                        cols = st.columns(num_cols)
                        for j in range(num_cols):
                            idx = i + j
                            if idx < num_proveedores:
                                proveedor = top_proveedores.iloc[idx]['Proveedor']
                                df_prov = df_filtrado[df_filtrado['Proveedor'] == proveedor]
                                
                                with cols[j]:
                                    fig = crear_grafico_pastel_proveedor(df_prov, str(proveedor))
                                    st.plotly_chart(fig, use_container_width=True, key=f"chart_tab1_{proveedor}_{i}_{j}")
                
                st.markdown("---")
                
                # Tabla completa
                st.markdown("### 📊 Tabla Completa de Proveedores")
                st.dataframe(
                    metricas_proveedor.sort_values('% OTIF', ascending=False),
                    use_container_width=True,
                    height=400,
                    column_config={
                        "% OTIF": st.column_config.ProgressColumn("% OTIF", format="%.1f%%", min_value=0, max_value=100),
                        "% Fill Rate": st.column_config.NumberColumn("% Fill Rate", format="%.2f%%"),
                        "Días Diferencia Promedio": st.column_config.NumberColumn("Días Promedio", format="%.1f"),
                    }
                )
                
                st.download_button(
                    label="📥 Descargar Métricas",
                    data=metricas_proveedor.to_csv(index=False).encode('utf-8'),
                    file_name=f"otif_proveedores_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            
            with tab2:
                st.markdown("### 📧 Enviar Reporte OTIF a Proveedor")
                
                # Selector de proveedor
                proveedores_con_pedidos = sorted(df_filtrado['Proveedor'].unique())
                proveedor_seleccionado = st.selectbox(
                    "Selecciona un proveedor:",
                    options=proveedores_con_pedidos
                )
                
                if proveedor_seleccionado:
                    # Obtener datos del proveedor
                    df_proveedor = df_filtrado[df_filtrado['Proveedor'] == proveedor_seleccionado]
                    codigo_proveedor = df_proveedor.iloc[0]['Código Proveedor']
                    email_proveedor = obtener_email_proveedor(codigo_proveedor)
                    
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown(f"**Proveedor:** {proveedor_seleccionado}")
                        st.markdown(f"**Código:** {codigo_proveedor}")
                        st.markdown(f"**Email:** {email_proveedor if email_proveedor else '❌ No disponible'}")
                        st.markdown(f"**Período:** {fecha_inicio.strftime('%d/%m/%Y')} - {fecha_fin.strftime('%d/%m/%Y')}")
                        st.markdown(f"**Total Pedidos:** {len(df_proveedor)}")
                    
                    with col2:
                        # Métricas del proveedor
                        otif_count = df_proveedor['Es OTIF'].sum()
                        otif_pct = (otif_count / len(df_proveedor) * 100) if len(df_proveedor) > 0 else 0
                        
                        st.metric("% OTIF", f"{otif_pct:.1f}%")
                        st.metric("OTIF Cumplidos", f"{otif_count}/{len(df_proveedor)}")
                    
                    st.markdown("---")
                    
                    # Vista previa del gráfico
                    st.markdown("### 📊 Gráfico que se enviará")
                    fig = crear_grafico_pastel_proveedor(df_proveedor, proveedor_seleccionado)
                    st.plotly_chart(fig, use_container_width=True, key=f"chart_tab2_{proveedor_seleccionado}")
                    
                    st.markdown("---")
                    
                    # Desglose de pedidos
                    col1, col2, col3 = st.columns(3)
                    
                    no_entregados = df_proveedor[df_proveedor['Estado'] == 'NO ENTREGADO']
                    atrasados = df_proveedor[df_proveedor['Estado'].isin(['ENTREGADO TARDE', 'EXCEPCIÓN (2 DÍAS TARDE)'])]
                    entregados_ok = df_proveedor[df_proveedor['Es OTIF'] == True]
                    
                    with col1:
                        st.markdown(f"""
                        <div class="metric-card" style="border-left-color: #3D3D3D">
                            <p class="metric-value">{len(no_entregados)}</p>
                            <p class="metric-label">NO ENTREGADOS</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if len(no_entregados) > 0:
                            with st.expander("Ver detalle"):
                                st.dataframe(
                                    no_entregados[['Nº documento', 'Descripción', 'Fecha Esperada', 'Cantidad Pendiente']],
                                    use_container_width=True
                                )
                    
                    with col2:
                        st.markdown(f"""
                        <div class="metric-card" style="border-left-color: #8B7355">
                            <p class="metric-value">{len(atrasados)}</p>
                            <p class="metric-label">ATRASADOS</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if len(atrasados) > 0:
                            with st.expander("Ver detalle"):
                                st.dataframe(
                                    atrasados[['Nº documento', 'Fecha Esperada', 'Fecha Real', 'Días Diferencia', 'Estado']],
                                    use_container_width=True
                                )
                    
                    with col3:
                        st.markdown(f"""
                        <div class="metric-card" style="border-left-color: #5B7C8D">
                            <p class="metric-value">{len(entregados_ok)}</p>
                            <p class="metric-label">ENTREGADOS OK</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if len(entregados_ok) > 0:
                            with st.expander("Ver detalle"):
                                st.dataframe(
                                    entregados_ok[['Nº documento', 'Fecha Esperada', 'Fecha Real', 'Cantidad Total']].head(10),
                                    use_container_width=True
                                )
                    
                    st.markdown("---")
                    
                    # Email del destinatario
                    st.markdown("### 📧 Enviar Email al Proveedor")
                    
                    # Mostrar email del proveedor del Excel
                    if email_proveedor:
                        st.success(f"✅ Email del proveedor: **{email_proveedor}**")
                        email_destino = email_proveedor
                    else:
                        st.warning("⚠️ Este proveedor no tiene email en la base de datos")
                        email_destino = st.text_input(
                            "Email del proveedor:",
                            placeholder="proveedor@ejemplo.com"
                        )
                    
                    asunto_email = st.text_input(
                        "Asunto del email:",
                        value=f"Reporte OTIF - {proveedor_seleccionado} - {datetime.now().strftime('%B %Y')}"
                    )
                    
                    st.markdown("---")
                    
                    # Botón para generar email
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        generar_email = st.button("📧 Abrir en Outlook/Email", type="primary", use_container_width=True)
                    
                    with col2:
                        descargar_html = st.button("📥 Descargar HTML", use_container_width=True)
                    
                    if generar_email or descargar_html:
                        if not email_destino:
                            st.error("❌ Por favor, introduce el email del proveedor")
                        else:
                            with st.spinner("Generando reporte..."):
                                import io
                                import base64
                                import urllib.parse
                                
                                # Generar imagen del gráfico
                                try:
                                    img_bytes = fig.to_image(format="png", width=800, height=600)
                                    img_base64 = base64.b64encode(img_bytes).decode()
                                except Exception as e:
                                    st.warning(f"⚠️ No se pudo generar la imagen del gráfico. Instala: pip install kaleido")
                                    img_base64 = ""
                                
                                # Calcular métricas
                                metricas = {
                                    'otif_pct': otif_pct,
                                    'otif_count': otif_count,
                                    'total': len(df_proveedor)
                                }
                                
                                # Generar HTML
                                html_content = generar_reporte_proveedor_html(
                                    proveedor_seleccionado,
                                    df_proveedor,
                                    metricas,
                                    img_base64
                                )
                                
                                if generar_email:
                                    # Generar cuerpo de email en texto plano para el mailto
                                    cuerpo_texto = f"""Estimado proveedor,

Adjunto encontrará el reporte OTIF del período {fecha_inicio.strftime('%d/%m/%Y')} - {fecha_fin.strftime('%d/%m/%Y')}.

RESUMEN:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Total de pedidos: {len(df_proveedor)}
• % OTIF: {otif_pct:.1f}%
• Pedidos OTIF: {otif_count}
• Pedidos no entregados: {len(no_entregados)}
• Pedidos atrasados: {len(atrasados)}

"""
                                    
                                    # Añadir pedidos NO ENTREGADOS
                                    if len(no_entregados) > 0:
                                        cuerpo_texto += f"\n❌ PEDIDOS NO ENTREGADOS ({len(no_entregados)}):\n"
                                        cuerpo_texto += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                        for idx, pedido in no_entregados.head(10).iterrows():
                                            dias_retraso = (datetime.now().date() - pedido['Fecha Esperada'].date()).days if pd.notna(pedido['Fecha Esperada']) else 0
                                            cuerpo_texto += f"• {pedido['Nº documento']} - {pedido['Descripción'][:50]} - Fecha esperada: {pedido['Fecha Esperada'].strftime('%d/%m/%Y')} ({dias_retraso} días de retraso)\n"
                                        if len(no_entregados) > 10:
                                            cuerpo_texto += f"... y {len(no_entregados) - 10} pedidos más\n"
                                    
                                    # Añadir pedidos ATRASADOS
                                    if len(atrasados) > 0:
                                        cuerpo_texto += f"\n⚠️ PEDIDOS ATRASADOS ({len(atrasados)}):\n"
                                        cuerpo_texto += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                        for idx, pedido in atrasados.head(10).iterrows():
                                            cuerpo_texto += f"• {pedido['Nº documento']} - {pedido['Descripción'][:50]} - Retraso: {pedido['Días Diferencia']} días\n"
                                        if len(atrasados) > 10:
                                            cuerpo_texto += f"... y {len(atrasados) - 10} pedidos más\n"
                                    
                                    cuerpo_texto += f"""

Para ver el reporte completo con gráficos, descargue el archivo HTML adjunto.

Saludos cordiales,
KAVE HOME - Planning Department
"""
                                    
                                    # Codificar para URL
                                    mailto_link = f"mailto:{email_destino}?subject={urllib.parse.quote(asunto_email)}&body={urllib.parse.quote(cuerpo_texto)}"
                                    
                                    st.success("✅ Email generado correctamente")
                                    
                                    # Botón para abrir Outlook
                                    st.markdown(f"""
                                    <a href="{mailto_link}" target="_blank">
                                        <button style="
                                            background-color: #5B7C8D;
                                            color: white;
                                            padding: 15px 30px;
                                            font-size: 18px;
                                            border: none;
                                            border-radius: 8px;
                                            cursor: pointer;
                                            width: 100%;
                                            font-weight: bold;
                                            margin: 20px 0;
                                        ">
                                            📧 Abrir Outlook con Email Pre-rellenado
                                        </button>
                                    </a>
                                    """, unsafe_allow_html=True)
                                    
                                    st.info("""
                                    **Pasos:**
                                    1. Click en el botón azul de arriba
                                    2. Se abrirá tu cliente de email (Outlook, Gmail, etc.)
                                    3. El email estará pre-rellenado con destinatario, asunto y contenido
                                    4. Descarga el HTML de abajo y adjúntalo al email
                                    5. ¡Envía!
                                    """)
                                    
                                    # Botón para descargar HTML
                                    st.download_button(
                                        label="📥 Descargar Reporte HTML (para adjuntar)",
                                        data=html_content,
                                        file_name=f"reporte_otif_{proveedor_seleccionado}_{datetime.now().strftime('%Y%m%d')}.html",
                                        mime="text/html",
                                        use_container_width=True
                                    )
                                    
                                elif descargar_html:
                                    st.success("✅ Reporte HTML generado")
                                    
                                    st.download_button(
                                        label="📥 Descargar Reporte HTML",
                                        data=html_content,
                                        file_name=f"reporte_otif_{proveedor_seleccionado}_{datetime.now().strftime('%Y%m%d')}.html",
                                        mime="text/html",
                                        use_container_width=True
                                    )
                                
                                # Vista previa
                                with st.expander("👁️ Vista previa del reporte HTML"):
                                    st.components.v1.html(html_content, height=800, scrolling=True)
            
            with tab3:
                st.markdown("### ⚠️ Gestión de Reclamaciones")
                st.markdown("Selecciona los pedidos no entregados que deseas reclamar al proveedor")
                
                # Filtrar solo pedidos NO ENTREGADOS hasta hoy
                hoy = datetime.now().date()
                df_no_entregados = df_filtrado[
                    (df_filtrado['Estado'] == 'NO ENTREGADO') &
                    (df_filtrado['Fecha Esperada'].dt.date <= hoy)
                ].copy()
                
                if len(df_no_entregados) == 0:
                    st.success("🎉 ¡Excelente! No hay pedidos pendientes de entrega")
                else:
                    st.warning(f"⚠️ Hay **{len(df_no_entregados)}** pedidos sin entregar hasta hoy")
                    
                    # Calcular días de retraso
                    df_no_entregados['Días Retraso'] = df_no_entregados['Fecha Esperada'].apply(
                        lambda x: (hoy - x.date()).days
                    )
                    
                    # Filtros
                    st.markdown("---")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        proveedores_disponibles = sorted(df_no_entregados['Proveedor'].unique())
                        proveedor_filtro = st.selectbox(
                            "Filtrar por proveedor:",
                            options=['Todos'] + proveedores_disponibles
                        )
                    
                    with col2:
                        almacenes_disponibles = sorted(df_no_entregados['Almacén'].unique())
                        almacen_filtro = st.multiselect(
                            "Filtrar por almacén:",
                            options=almacenes_disponibles,
                            default=almacenes_disponibles
                        )
                    
                    with col3:
                        dias_minimos = st.number_input(
                            "Días mínimos de retraso:",
                            min_value=0,
                            value=0,
                            step=1,
                            help="Mostrar solo pedidos con al menos X días de retraso"
                        )
                    
                    # Aplicar filtros
                    df_filtrado_reclamacion = df_no_entregados.copy()
                    
                    if proveedor_filtro != 'Todos':
                        df_filtrado_reclamacion = df_filtrado_reclamacion[
                            df_filtrado_reclamacion['Proveedor'] == proveedor_filtro
                        ]
                    
                    if almacen_filtro:
                        df_filtrado_reclamacion = df_filtrado_reclamacion[
                            df_filtrado_reclamacion['Almacén'].isin(almacen_filtro)
                        ]
                    
                    df_filtrado_reclamacion = df_filtrado_reclamacion[
                        df_filtrado_reclamacion['Días Retraso'] >= dias_minimos
                    ]
                    
                    st.markdown("---")
                    
                    if len(df_filtrado_reclamacion) == 0:
                        st.info("No hay pedidos que cumplan los criterios de filtrado")
                    else:
                        # Métricas de reclamación
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.markdown(f"""
                            <div class="metric-card" style="border-left-color: #dc3545">
                                <p class="metric-value">{len(df_filtrado_reclamacion)}</p>
                                <p class="metric-label">Pedidos Pendientes</p>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        with col2:
                            dias_promedio = df_filtrado_reclamacion['Días Retraso'].mean()
                            st.markdown(f"""
                            <div class="metric-card" style="border-left-color: #ff6b6b">
                                <p class="metric-value">{dias_promedio:.0f}</p>
                                <p class="metric-label">Días Retraso Promedio</p>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        with col3:
                            cantidad_total = df_filtrado_reclamacion['Cantidad Pendiente'].sum()
                            st.markdown(f"""
                            <div class="metric-card" style="border-left-color: #ffa502">
                                <p class="metric-value">{cantidad_total:.0f}</p>
                                <p class="metric-label">Unidades Pendientes</p>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        with col4:
                            proveedores_afectados = df_filtrado_reclamacion['Proveedor'].nunique()
                            st.markdown(f"""
                            <div class="metric-card" style="border-left-color: #ff4757">
                                <p class="metric-value">{proveedores_afectados}</p>
                                <p class="metric-label">Proveedores Afectados</p>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        
                        # Tabla con checkbox para seleccionar pedidos
                        st.markdown("### 📋 Selecciona los pedidos a reclamar")
                        
                        # Crear columnas para mostrar
                        df_display = df_filtrado_reclamacion[[
                            'Proveedor', 'Nº documento', 'Nº Artículo', 'Descripción',
                            'Almacén', 'Fecha Esperada', 'Cantidad Pendiente', 'Días Retraso'
                        ]].copy()
                        
                        # Ordenar por días de retraso descendente
                        df_display = df_display.sort_values('Días Retraso', ascending=False)
                        
                        # Añadir checkbox de selección
                        df_display.insert(0, 'Seleccionar', False)
                        
                        # Usar data_editor para permitir selección
                        df_edited = st.data_editor(
                            df_display,
                            use_container_width=True,
                            height=400,
                            column_config={
                                "Seleccionar": st.column_config.CheckboxColumn(
                                    "✓",
                                    help="Selecciona los pedidos a reclamar",
                                    default=False,
                                ),
                                "Fecha Esperada": st.column_config.DateColumn(
                                    "Fecha Esperada",
                                    format="DD/MM/YYYY"
                                ),
                                "Días Retraso": st.column_config.NumberColumn(
                                    "Días Retraso",
                                    help="Días desde la fecha esperada hasta hoy",
                                    format="%d días"
                                ),
                                "Cantidad Pendiente": st.column_config.NumberColumn(
                                    "Cantidad Pendiente",
                                    format="%.0f"
                                )
                            },
                            disabled=["Proveedor", "Nº documento", "Nº Artículo", "Descripción", 
                                     "Almacén", "Fecha Esperada", "Cantidad Pendiente", "Días Retraso"],
                            hide_index=True,
                        )
                        
                        # Obtener pedidos seleccionados
                        pedidos_seleccionados = df_edited[df_edited['Seleccionar'] == True]
                        
                        st.markdown("---")
                        
                        if len(pedidos_seleccionados) > 0:
                            st.success(f"✅ {len(pedidos_seleccionados)} pedidos seleccionados para reclamar")
                            
                            # Agrupar por proveedor
                            proveedores_reclamar = pedidos_seleccionados.groupby('Proveedor').size()
                            
                            col1, col2 = st.columns([2, 1])
                            
                            with col1:
                                st.markdown("**Resumen de reclamación:**")
                                for proveedor, count in proveedores_reclamar.items():
                                    st.markdown(f"- **{proveedor}**: {count} pedidos")
                            
                            with col2:
                                st.markdown("<br>", unsafe_allow_html=True)
                                enviar_reclamacion = st.button(
                                    "📧 Enviar Reclamación",
                                    type="primary",
                                    use_container_width=True
                                )
                            
                            if enviar_reclamacion:
                                st.success("✅ Reclamaciones listas")
                                
                                st.info("""
                                ### 📧 Proceso SÚPER SIMPLE (1 click):
                                
                                1. **Click** en "📧 ABRIR EN OUTLOOK"
                                
                                ➡️ **Outlook se abre INMEDIATAMENTE** con:
                                - ✅ Destinatario ya puesto
                                - ✅ Asunto pre-rellenado
                                - ✅ Email estructurado y profesional
                                - ✅ Todos los pedidos listados claramente
                                
                                💡 Revisa y click "Enviar" - ¡Listo!
                                
                                ⚠️ **Sin descargar archivos** - Se abre directamente
                                """)
                                
                                st.markdown("---")
                                
                                # Procesar cada proveedor con índice único
                                for idx_prov, proveedor in enumerate(proveedores_reclamar.index):
                                    pedidos_prov = pedidos_seleccionados[pedidos_seleccionados['Proveedor'] == proveedor]
                                    
                                    # Obtener email del proveedor
                                    codigo_prov = df_filtrado_reclamacion[
                                        df_filtrado_reclamacion['Proveedor'] == proveedor
                                    ]['Código Proveedor'].iloc[0]
                                    email_prov = obtener_email_proveedor(codigo_prov)
                                    
                                    if not email_prov:
                                        st.warning(f"⚠️ {proveedor}: No tiene email registrado")
                                        continue
                                    
                                    # Generar HTML para el email (versión optimizada para email)
                                    html_reclamacion = f"""
                                    <div style="font-family: Arial, sans-serif; max-width: 900px; margin: 0 auto;">
                                        <div style="background: linear-gradient(135deg, #dc3545 0%, #c82333 100%); color: white; padding: 30px; text-align: center; border-radius: 10px; margin-bottom: 20px;">
                                            <h1 style="margin: 0;">⚠️ RECLAMACIÓN</h1>
                                            <h2 style="margin: 10px 0;">Pedidos Pendientes de Entrega</h2>
                                            <p style="margin: 5px 0;">KAVE HOME - Planning Department</p>
                                        </div>
                                        
                                        <p>Estimado proveedor,</p>
                                        <p>Por medio de la presente, le informamos que los siguientes pedidos están <strong>PENDIENTES DE ENTREGA</strong> con retraso:</p>
                                        
                                        <div style="background: #fff3cd; border-left: 5px solid #ffc107; padding: 15px; margin: 20px 0; border-radius: 5px;">
                                            <strong>⏰ ACCIÓN REQUERIDA URGENTE</strong><br>
                                            Total de líneas afectadas: <strong>{len(pedidos_prov)}</strong><br>
                                            Retraso promedio: <strong>{pedidos_prov['Días Retraso'].mean():.0f} días</strong>
                                        </div>
                                    """
                                    
                                    # Agrupar por número de pedido
                                    pedidos_agrupados = pedidos_prov.groupby('Nº documento')
                                    
                                    for num_pedido, grupo in pedidos_agrupados:
                                        dias_retraso = grupo.iloc[0]['Días Retraso']
                                        fecha_esperada = grupo.iloc[0]['Fecha Esperada'].strftime('%d/%m/%Y')
                                        almacen = grupo.iloc[0]['Almacén']
                                        total_unidades = grupo['Cantidad Pendiente'].sum()
                                        
                                        html_reclamacion += f"""
                                        <div style="margin: 20px 0; border: 2px solid #dc3545; border-radius: 10px; overflow: hidden;">
                                            <div style="background: #dc3545; color: white; padding: 12px 15px; font-size: 16px; font-weight: bold;">
                                                📋 Pedido: {num_pedido} | 
                                                📅 {fecha_esperada} | 
                                                🏭 {almacen} | 
                                                ⚠️ RETRASO: {dias_retraso} DÍAS
                                            </div>
                                            <table style="width: 100%; border-collapse: collapse;">
                                                <thead>
                                                    <tr style="background-color: #f8f9fa;">
                                                        <th style="padding: 10px; text-align: left; border-bottom: 2px solid #dee2e6;">Artículo</th>
                                                        <th style="padding: 10px; text-align: left; border-bottom: 2px solid #dee2e6;">Descripción</th>
                                                        <th style="padding: 10px; text-align: right; border-bottom: 2px solid #dee2e6;">Cantidad</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                        """
                                        
                                        for _, linea in grupo.iterrows():
                                            html_reclamacion += f"""
                                                    <tr>
                                                        <td style="padding: 8px 10px; border-bottom: 1px solid #dee2e6;"><strong>{linea['Nº Artículo']}</strong></td>
                                                        <td style="padding: 8px 10px; border-bottom: 1px solid #dee2e6;">{linea['Descripción']}</td>
                                                        <td style="padding: 8px 10px; text-align: right; border-bottom: 1px solid #dee2e6;"><strong>{linea['Cantidad Pendiente']:.0f}</strong> uds</td>
                                                    </tr>
                                            """
                                        
                                        html_reclamacion += f"""
                                                    <tr style="background-color: #fff3cd; font-weight: bold;">
                                                        <td colspan="2" style="padding: 10px; text-align: right;">TOTAL PEDIDO:</td>
                                                        <td style="padding: 10px; text-align: right;">{total_unidades:.0f} uds ({len(grupo)} líneas)</td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                        </div>
                                        """
                                    
                                    # Resumen final
                                    total_pedidos = pedidos_prov['Nº documento'].nunique()
                                    total_articulos = len(pedidos_prov)
                                    total_unidades_global = pedidos_prov['Cantidad Pendiente'].sum()
                                    
                                    html_reclamacion += f"""
                                        <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin: 20px 0;">
                                            <h3 style="color: #dc3545; margin-top: 0;">📊 RESUMEN TOTAL</h3>
                                            <ul style="line-height: 1.8;">
                                                <li><strong>Pedidos afectados:</strong> {total_pedidos}</li>
                                                <li><strong>Líneas de artículos:</strong> {total_articulos}</li>
                                                <li><strong>Unidades pendientes:</strong> {total_unidades_global:.0f}</li>
                                                <li><strong>Retraso promedio:</strong> {pedidos_prov['Días Retraso'].mean():.0f} días</li>
                                            </ul>
                                        </div>
                                        
                                        <div style="background: #fff3cd; padding: 20px; border-radius: 10px; border-left: 5px solid #ffc107; margin: 20px 0;">
                                            <h3 style="margin-top: 0; color: #856404;">⚡ SOLICITAMOS URGENTEMENTE:</h3>
                                            <ol style="line-height: 1.8;">
                                                <li><strong>Confirmación de fechas de envío</strong> para cada pedido</li>
                                                <li><strong>Números de tracking/albaranes</strong> una vez realizados los envíos</li>
                                                <li><strong>Plan de acción</strong> para evitar futuros retrasos</li>
                                            </ol>
                                        </div>
                                        
                                        <p>Agradecemos su <strong>pronta respuesta</strong> y esperamos regularizar esta situación a la mayor brevedad posible.</p>
                                        
                                        <div style="text-align: center; margin-top: 30px; padding: 20px; background: #f8f9fa; border-radius: 10px;">
                                            <p style="margin: 5px 0;"><strong>KAVE HOME</strong></p>
                                            <p style="margin: 5px 0;">Planning Department</p>
                                            <p style="font-size: 12px; color: #999; margin-top: 15px;">
                                                Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M')}
                                            </p>
                                        </div>
                                    </div>
                                    """
                                    
                                    # Generar asunto
                                    asunto = f"⚠️ RECLAMACIÓN - {total_pedidos} Pedidos Pendientes - KAVE HOME"
                                    
                                    # Generar cuerpo en HTML con tablas reales (se copiará y pegará en Outlook)
                                    cuerpo_html_simple = f"""
<div style="font-family: Arial, sans-serif; max-width: 800px;">
    <div style="background: #dc3545; color: white; padding: 20px; text-align: center; border-radius: 8px; margin-bottom: 20px;">
        <h2 style="margin: 0;">⚠️ RECLAMACIÓN URGENTE ⚠️</h2>
        <h3 style="margin: 10px 0;">PEDIDOS PENDIENTES DE ENTREGA</h3>
        <p style="margin: 5px 0;">KAVE HOME - Planning Department</p>
    </div>
    
    <p>Estimado proveedor,</p>
    <p>Por medio de la presente, le informamos que los siguientes pedidos están <strong>PENDIENTES DE ENTREGA</strong> con retraso:</p>
    
    <div style="background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 15px 0;">
        <strong>⏰ ACCIÓN REQUERIDA URGENTE</strong><br>
        • Líneas afectadas: <strong>{total_articulos}</strong><br>
        • Retraso promedio: <strong>{pedidos_prov['Días Retraso'].mean():.0f} días</strong>
    </div>
"""
                                    
                                    # Agrupar por número de pedido
                                    for num_pedido, grupo in pedidos_agrupados:
                                        dias_retraso = grupo.iloc[0]['Días Retraso']
                                        fecha_esperada = grupo.iloc[0]['Fecha Esperada'].strftime('%d/%m/%Y')
                                        almacen = grupo.iloc[0]['Almacén']
                                        total_unidades = grupo['Cantidad Pendiente'].sum()
                                        
                                        cuerpo_html_simple += f"""
    <div style="border: 2px solid #dc3545; border-radius: 8px; margin: 20px 0; overflow: hidden;">
        <div style="background: #dc3545; color: white; padding: 12px; font-weight: bold;">
            📋 Pedido: {num_pedido} | 📅 {fecha_esperada} | 🏭 {almacen} | ⚠️ RETRASO: {dias_retraso} DÍAS
        </div>
        <table style="width: 100%; border-collapse: collapse;">
            <thead>
                <tr style="background: #f8f9fa;">
                    <th style="padding: 10px; text-align: left; border-bottom: 2px solid #dee2e6;">Artículo</th>
                    <th style="padding: 10px; text-align: left; border-bottom: 2px solid #dee2e6;">Descripción</th>
                    <th style="padding: 10px; text-align: right; border-bottom: 2px solid #dee2e6;">Cantidad</th>
                </tr>
            </thead>
            <tbody>
"""
                                        
                                        for _, linea in grupo.iterrows():
                                            cuerpo_html_simple += f"""
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #dee2e6;"><strong>{linea['Nº Artículo']}</strong></td>
                    <td style="padding: 8px; border-bottom: 1px solid #dee2e6;">{linea['Descripción']}</td>
                    <td style="padding: 8px; text-align: right; border-bottom: 1px solid #dee2e6;"><strong>{linea['Cantidad Pendiente']:.0f}</strong> uds</td>
                </tr>
"""
                                        
                                        cuerpo_html_simple += f"""
                <tr style="background: #fff3cd; font-weight: bold;">
                    <td colspan="2" style="padding: 10px; text-align: right;">TOTAL PEDIDO:</td>
                    <td style="padding: 10px; text-align: right;">{total_unidades:.0f} uds ({len(grupo)} líneas)</td>
                </tr>
            </tbody>
        </table>
    </div>
"""
                                    
                                    # Resumen final
                                    cuerpo_html_simple += f"""
    <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
        <h3 style="color: #dc3545; margin-top: 0;">📊 RESUMEN TOTAL</h3>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 5px;"><strong>Pedidos afectados:</strong></td>
                <td style="padding: 5px;">{total_pedidos} PC</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Líneas de artículos:</strong></td>
                <td style="padding: 5px;">{total_articulos}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Unidades pendientes:</strong></td>
                <td style="padding: 5px;">{total_unidades_global:.0f}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Retraso promedio:</strong></td>
                <td style="padding: 5px;">{pedidos_prov['Días Retraso'].mean():.0f} días</td>
            </tr>
        </table>
    </div>
    
    <div style="background: #fff3cd; padding: 20px; border-radius: 8px; border-left: 4px solid #ffc107; margin: 20px 0;">
        <h3 style="margin-top: 0; color: #856404;">⚡ SOLICITAMOS URGENTEMENTE:</h3>
        <ol style="line-height: 1.8;">
            <li><strong>Confirmación de FECHAS DE ENVÍO</strong> para cada pedido</li>
            <li><strong>Números de TRACKING/ALBARANES</strong> una vez enviados</li>
            <li><strong>PLAN DE ACCIÓN</strong> para evitar futuros retrasos</li>
        </ol>
    </div>
    
    <p>Agradecemos su <strong>PRONTA RESPUESTA</strong> y esperamos regularizar esta situación a la mayor brevedad posible.</p>
    
    <div style="text-align: center; margin-top: 30px; padding: 15px; background: #f8f9fa; border-radius: 8px;">
        <p style="margin: 5px 0;"><strong>🏠 KAVE HOME</strong></p>
        <p style="margin: 5px 0;">Planning Department</p>
        <p style="font-size: 11px; color: #999; margin-top: 10px;">
            📧 Email automático generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M')}
        </p>
    </div>
</div>
"""
                                    
                                    # Crear mailto básico (solo con asunto, el HTML se copiará)
                                    import urllib.parse
                                    mailto_link = f"mailto:{email_prov}?subject={urllib.parse.quote(asunto)}"
                                    
                                    # Mostrar controles
                                    st.markdown(f"""
                                    <div style="margin: 20px 0; padding: 25px; background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%); border-radius: 15px; border-left: 5px solid #ffc107; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">
                                        <h3 style="margin: 0 0 15px 0; color: #856404;">📧 {proveedor}</h3>
                                        <div style="background: white; padding: 15px; border-radius: 8px; margin: 10px 0;">
                                            <p style="margin: 5px 0;"><strong>Email:</strong> {email_prov}</p>
                                            <p style="margin: 5px 0;"><strong>Pedidos (PC):</strong> {total_pedidos}</p>
                                            <p style="margin: 5px 0;"><strong>Líneas de artículos:</strong> {total_articulos}</p>
                                            <p style="margin: 5px 0;"><strong>Unidades pendientes:</strong> {total_unidades_global:.0f}</p>
                                        </div>
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    # Generar asunto
                                    asunto = f"⚠️ RECLAMACIÓN - {total_pedidos} Pedidos Pendientes - KAVE HOME"
                                    
                                    # Generar cuerpo en TEXTO con formato de tabla
                                    cuerpo_texto = f"""⚠️ RECLAMACIÓN URGENTE - PEDIDOS PENDIENTES DE ENTREGA

Estimado proveedor,

Por medio de la presente, le informamos que los siguientes pedidos están PENDIENTES DE ENTREGA con retraso:

═══════════════════════════════════════════════════════════════════
RESUMEN
═══════════════════════════════════════════════════════════════════
• Total pedidos: {total_pedidos}
• Líneas afectadas: {total_articulos}
• Retraso promedio: {pedidos_prov['Días Retraso'].mean():.0f} días

"""
                                    
                                    # Agrupar por número de pedido con formato tabla
                                    for num_pedido, grupo in pedidos_agrupados:
                                        dias_retraso = grupo.iloc[0]['Días Retraso']
                                        fecha_esperada = grupo.iloc[0]['Fecha Esperada'].strftime('%d/%m/%Y')
                                        almacen = grupo.iloc[0]['Almacén']
                                        total_unidades = grupo['Cantidad Pendiente'].sum()
                                        
                                        cuerpo_texto += f"""
═══════════════════════════════════════════════════════════════════
📋 PEDIDO: {num_pedido}
📅 Fecha esperada: {fecha_esperada}  |  🏭 Almacén: {almacen}  |  ⚠️ RETRASO: {dias_retraso} DÍAS
═══════════════════════════════════════════════════════════════════

ARTÍCULO                    DESCRIPCIÓN                          CANTIDAD
───────────────────────────────────────────────────────────────────────
"""
                                        
                                        for _, linea in grupo.iterrows():
                                            articulo = str(linea['Nº Artículo'])[:25].ljust(25)
                                            descripcion = str(linea['Descripción'])[:35].ljust(35)
                                            cantidad = f"{linea['Cantidad Pendiente']:.0f} uds".rjust(10)
                                            
                                            cuerpo_texto += f"{articulo} {descripcion} {cantidad}\n"
                                        
                                        cuerpo_texto += f"""───────────────────────────────────────────────────────────────────────
TOTAL PEDIDO: {total_unidades:.0f} unidades ({len(grupo)} líneas)

"""
                                    
                                    # Resumen final
                                    cuerpo_texto += f"""

═══════════════════════════════════════════════════════════════════
📊 RESUMEN TOTAL
═══════════════════════════════════════════════════════════════════
• Pedidos afectados:      {total_pedidos}
• Líneas de artículos:    {total_articulos}
• Unidades pendientes:    {total_unidades_global:.0f}
• Retraso promedio:       {pedidos_prov['Días Retraso'].mean():.0f} días


═══════════════════════════════════════════════════════════════════
⚡ SOLICITAMOS URGENTEMENTE
═══════════════════════════════════════════════════════════════════

1. Confirmación de FECHAS DE ENVÍO para cada pedido
2. Números de TRACKING/ALBARANES una vez enviados  
3. PLAN DE ACCIÓN para evitar futuros retrasos


Agradecemos su PRONTA RESPUESTA y esperamos regularizar esta 
situación a la mayor brevedad posible.


Atentamente,

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏠 KAVE HOME
   Planning Department
   
📧 Email generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
                                    
                                    # Generar asunto
                                    asunto = f"RECLAMACION - {total_pedidos} Pedidos Pendientes - KAVE HOME"
                                    
                                    # Generar cuerpo en TEXTO SIMPLE pero bien estructurado
                                    cuerpo_texto = f"""Estimado proveedor,

Por medio de la presente, le informamos que los siguientes pedidos están PENDIENTES DE ENTREGA:

RESUMEN:
- Total pedidos: {total_pedidos}
- Lineas afectadas: {total_articulos}
- Unidades pendientes: {total_unidades_global:.0f}
- Retraso promedio: {pedidos_prov['Días Retraso'].mean():.0f} dias

"""
                                    
                                    # Listar cada pedido de forma simple
                                    for num_pedido, grupo in pedidos_agrupados:
                                        dias_retraso = grupo.iloc[0]['Días Retraso']
                                        fecha_esperada = grupo.iloc[0]['Fecha Esperada'].strftime('%d/%m/%Y')
                                        almacen = grupo.iloc[0]['Almacén']
                                        total_unidades = grupo['Cantidad Pendiente'].sum()
                                        
                                        cuerpo_texto += f"""
================================================================================
PEDIDO: {num_pedido}
Fecha esperada: {fecha_esperada}
Almacen destino: {almacen}
RETRASO: {dias_retraso} DIAS
================================================================================

"""
                                        
                                        # Listar artículos del pedido
                                        for idx, (_, linea) in enumerate(grupo.iterrows(), 1):
                                            cuerpo_texto += f"""  {idx}. {linea['Nº Artículo']}
     {linea['Descripción']}
     Cantidad pendiente: {linea['Cantidad Pendiente']:.0f} unidades

"""
                                        
                                        cuerpo_texto += f"""TOTAL PEDIDO: {total_unidades:.0f} unidades ({len(grupo)} lineas)

"""
                                    
                                    # Solicitud
                                    cuerpo_texto += f"""
================================================================================
SOLICITAMOS URGENTEMENTE:
================================================================================

1. Confirmacion de FECHAS DE ENVIO para cada pedido
2. Numeros de TRACKING/ALBARANES una vez enviados
3. PLAN DE ACCION para evitar futuros retrasos

Agradecemos su pronta respuesta.

Atentamente,
KAVE HOME - Planning Department
"""
                                    
                                    # Crear mailto - SE ABRE DIRECTAMENTE EN OUTLOOK
                                    import urllib.parse
                                    mailto_link = f"mailto:{email_prov}?subject={urllib.parse.quote(asunto)}&body={urllib.parse.quote(cuerpo_texto)}"
                                    
                                    # UN SOLO BOTÓN - SIMPLE
                                    st.markdown(f"""
                                    <a href="{mailto_link}" target="_blank" style="text-decoration: none;">
                                        <button style="
                                            background-color: #dc3545;
                                            color: white;
                                            padding: 20px 30px;
                                            font-size: 18px;
                                            border: none;
                                            border-radius: 10px;
                                            cursor: pointer;
                                            font-weight: bold;
                                            width: 100%;
                                            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                                        ">
                                            📧 ABRIR EN OUTLOOK - {proveedor}
                                        </button>
                                    </a>
                                    """, unsafe_allow_html=True)
                                    
                                    st.success(f"""
                                    ✅ **Email para: {proveedor}**
                                    
                                    • Destinatario: {email_prov}
                                    • Pedidos: {total_pedidos} | Líneas: {total_articulos} | Retraso: {pedidos_prov['Días Retraso'].mean():.0f} días
                                    
                                    👆 **Click en el botón y Outlook se abre directamente**
                                    """)
                                    
                                    st.markdown("---")
                                # Procesar cada proveedor
                                for proveedor in proveedores_reclamar.index:
                                    pedidos_prov = pedidos_seleccionados[pedidos_seleccionados['Proveedor'] == proveedor]
                                    
                                    # Obtener email del proveedor
                                    codigo_prov = df_filtrado_reclamacion[
                                        df_filtrado_reclamacion['Proveedor'] == proveedor
                                    ]['Código Proveedor'].iloc[0]
                                    email_prov = obtener_email_proveedor(codigo_prov)
                                    
                                    if not email_prov:
                                        st.warning(f"⚠️ {proveedor}: No tiene email registrado")
                                        continue
                                    
                                    # Generar HTML para el email
                                    html_reclamacion = f"""
                                    <html>
                                    <head>
                                        <style>
                                            body {{
                                                font-family: Arial, sans-serif;
                                                line-height: 1.6;
                                                color: #333;
                                                max-width: 1000px;
                                                margin: 0 auto;
                                                padding: 20px;
                                            }}
                                            .header {{
                                                background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);
                                                color: white;
                                                padding: 30px;
                                                text-align: center;
                                                border-radius: 10px;
                                                margin-bottom: 30px;
                                            }}
                                            .warning {{
                                                background: #fff3cd;
                                                border-left: 5px solid #ffc107;
                                                padding: 20px;
                                                margin: 20px 0;
                                                border-radius: 5px;
                                            }}
                                            .pedido-grupo {{
                                                margin: 30px 0;
                                                background: white;
                                                border: 2px solid #dc3545;
                                                border-radius: 10px;
                                                overflow: hidden;
                                            }}
                                            .pedido-header {{
                                                background: #dc3545;
                                                color: white;
                                                padding: 15px 20px;
                                                font-size: 1.2em;
                                                font-weight: bold;
                                            }}
                                            table {{
                                                width: 100%;
                                                border-collapse: collapse;
                                            }}
                                            th {{
                                                background-color: #f8f9fa;
                                                color: #333;
                                                padding: 12px;
                                                text-align: left;
                                                font-weight: bold;
                                                border-bottom: 2px solid #dee2e6;
                                            }}
                                            td {{
                                                padding: 10px 12px;
                                                border-bottom: 1px solid #dee2e6;
                                            }}
                                            tr:hover {{
                                                background-color: #f8f9fa;
                                            }}
                                            .resumen {{
                                                background: #f8f9fa;
                                                padding: 20px;
                                                border-radius: 10px;
                                                margin: 20px 0;
                                            }}
                                            .footer {{
                                                text-align: center;
                                                margin-top: 40px;
                                                padding: 20px;
                                                background: #f8f9fa;
                                                border-radius: 10px;
                                                color: #666;
                                            }}
                                        </style>
                                    </head>
                                    <body>
                                        <div class="header">
                                            <h1>⚠️ RECLAMACIÓN</h1>
                                            <h2>Pedidos Pendientes de Entrega</h2>
                                            <p>KAVE HOME - Planning Department</p>
                                        </div>
                                        
                                        <p>Estimado proveedor,</p>
                                        <p>Por medio de la presente, le informamos que los siguientes pedidos están <strong>PENDIENTES DE ENTREGA</strong> con retraso:</p>
                                        
                                        <div class="warning">
                                            <strong>⏰ ACCIÓN REQUERIDA URGENTE</strong><br>
                                            Total de pedidos afectados: <strong>{len(pedidos_prov)}</strong><br>
                                            Retraso promedio: <strong>{pedidos_prov['Días Retraso'].mean():.0f} días</strong>
                                        </div>
                                    """
                                    
                                    # Agrupar por número de pedido
                                    pedidos_agrupados = pedidos_prov.groupby('Nº documento')
                                    
                                    for num_pedido, grupo in pedidos_agrupados:
                                        dias_retraso = grupo.iloc[0]['Días Retraso']
                                        fecha_esperada = grupo.iloc[0]['Fecha Esperada'].strftime('%d/%m/%Y')
                                        almacen = grupo.iloc[0]['Almacén']
                                        total_lineas = len(grupo)
                                        total_unidades = grupo['Cantidad Pendiente'].sum()
                                        
                                        html_reclamacion += f"""
                                        <div class="pedido-grupo">
                                            <div class="pedido-header">
                                                📋 Pedido: {num_pedido} | 
                                                📅 Fecha esperada: {fecha_esperada} | 
                                                🏭 Almacén: {almacen} | 
                                                ⚠️ RETRASO: {dias_retraso} DÍAS
                                            </div>
                                            <table>
                                                <thead>
                                                    <tr>
                                                        <th>Artículo</th>
                                                        <th>Descripción</th>
                                                        <th style="text-align: right;">Cantidad Pendiente</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                        """
                                        
                                        for _, linea in grupo.iterrows():
                                            html_reclamacion += f"""
                                                    <tr>
                                                        <td><strong>{linea['Nº Artículo']}</strong></td>
                                                        <td>{linea['Descripción']}</td>
                                                        <td style="text-align: right;"><strong>{linea['Cantidad Pendiente']:.0f}</strong> uds</td>
                                                    </tr>
                                            """
                                        
                                        html_reclamacion += f"""
                                                    <tr style="background-color: #fff3cd; font-weight: bold;">
                                                        <td colspan="2" style="text-align: right;">TOTAL PEDIDO:</td>
                                                        <td style="text-align: right;">{total_unidades:.0f} uds ({total_lineas} líneas)</td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                        </div>
                                        """
                                    
                                    # Resumen final
                                    total_pedidos = pedidos_prov['Nº documento'].nunique()
                                    total_articulos = len(pedidos_prov)
                                    total_unidades_global = pedidos_prov['Cantidad Pendiente'].sum()
                                    
                                    html_reclamacion += f"""
                                        <div class="resumen">
                                            <h3 style="color: #dc3545; margin-top: 0;">📊 RESUMEN TOTAL</h3>
                                            <ul>
                                                <li><strong>Pedidos afectados:</strong> {total_pedidos}</li>
                                                <li><strong>Líneas de artículos:</strong> {total_articulos}</li>
                                                <li><strong>Unidades pendientes:</strong> {total_unidades_global:.0f}</li>
                                                <li><strong>Retraso promedio:</strong> {pedidos_prov['Días Retraso'].mean():.0f} días</li>
                                            </ul>
                                        </div>
                                        
                                        <div style="background: #fff3cd; padding: 20px; border-radius: 10px; border-left: 5px solid #ffc107; margin: 20px 0;">
                                            <h3 style="margin-top: 0; color: #856404;">⚡ SOLICITAMOS URGENTEMENTE:</h3>
                                            <ol>
                                                <li><strong>Confirmación de fechas de envío</strong> para cada pedido</li>
                                                <li><strong>Números de tracking/albaranes</strong> una vez realizados los envíos</li>
                                                <li><strong>Plan de acción</strong> para evitar futuros retrasos</li>
                                            </ol>
                                        </div>
                                        
                                        <p>Agradecemos su <strong>pronta respuesta</strong> y esperamos regularizar esta situación a la mayor brevedad posible.</p>
                                        
                                        <div class="footer">
                                            <p><strong>KAVE HOME</strong></p>
                                            <p>Planning Department</p>
                                            <p style="font-size: 0.9em; color: #999; margin-top: 15px;">
                                                📧 Este es un email automático de reclamación<br>
                                                Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M')}
                                            </p>
                                        </div>
                                    </body>
                                    </html>
                                    """
                                    
                                    # Generar asunto
                                    asunto = f"⚠️ RECLAMACIÓN - {total_pedidos} Pedidos Pendientes - KAVE HOME"
                                    
                                    # Generar versión texto plano (por si no soporta HTML)
                                    cuerpo_texto = f"""RECLAMACIÓN - Pedidos Pendientes de Entrega

Estimado proveedor,

Los siguientes pedidos están PENDIENTES DE ENTREGA con retraso:

"""
                                    for num_pedido, grupo in pedidos_agrupados:
                                        dias_retraso = grupo.iloc[0]['Días Retraso']
                                        fecha_esperada = grupo.iloc[0]['Fecha Esperada'].strftime('%d/%m/%Y')
                                        almacen = grupo.iloc[0]['Almacén']
                                        
                                        cuerpo_texto += f"""
PEDIDO: {num_pedido}
Fecha esperada: {fecha_esperada} | Almacén: {almacen} | RETRASO: {dias_retraso} DÍAS
{'─' * 70}
"""
                                        for _, linea in grupo.iterrows():
                                            cuerpo_texto += f"  • {linea['Nº Artículo']} - {linea['Descripción']}: {linea['Cantidad Pendiente']:.0f} uds\n"
                                        
                                        cuerpo_texto += f"  TOTAL: {grupo['Cantidad Pendiente'].sum():.0f} unidades\n\n"
                                    
                                    cuerpo_texto += f"""
RESUMEN:
- Pedidos: {total_pedidos}
- Líneas: {total_articulos}
- Unidades: {total_unidades_global:.0f}
- Retraso promedio: {pedidos_prov['Días Retraso'].mean():.0f} días

SOLICITAMOS URGENTEMENTE:
1. Confirmación de fechas de envío
2. Números de tracking/albaranes
3. Plan de acción para evitar futuros retrasos

Saludos cordiales,
KAVE HOME - Planning Department
"""
                                    
                                    # Para mailto usamos texto plano
                                    import urllib.parse
                                    mailto_link = f"mailto:{email_prov}?subject={urllib.parse.quote(asunto)}&body={urllib.parse.quote(cuerpo_texto)}"
                                    
                                    # Mostrar botón con preview HTML
                                    st.markdown(f"""
                                    <div style="margin: 15px 0; padding: 20px; background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%); border-radius: 10px; border-left: 5px solid #ffc107;">
                                        <h4 style="margin: 0 0 10px 0; color: #856404;">📧 {proveedor}</h4>
                                        <p style="margin: 5px 0;"><strong>Email:</strong> {email_prov}</p>
                                        <p style="margin: 5px 0;"><strong>Pedidos (PC):</strong> {total_pedidos}</p>
                                        <p style="margin: 5px 0;"><strong>Líneas de artículos:</strong> {total_articulos}</p>
                                        <div style="margin-top: 15px;">
                                            <a href="{mailto_link}" target="_blank">
                                                <button style="
                                                    background-color: #dc3545;
                                                    color: white;
                                                    padding: 12px 25px;
                                                    font-size: 16px;
                                                    border: none;
                                                    border-radius: 8px;
                                                    cursor: pointer;
                                                    font-weight: bold;
                                                    margin-right: 10px;
                                                ">
                                                    📧 Abrir Email de Reclamación
                                                </button>
                                            </a>
                                        </div>
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    # Botón para descargar HTML
                                    st.download_button(
                                        label=f"📥 Descargar HTML - {proveedor}",
                                        data=html_reclamacion,
                                        file_name=f"reclamacion_{proveedor}_{datetime.now().strftime('%Y%m%d')}.html",
                                        mime="text/html",
                                        key=f"download_{proveedor}"
                                    )
                                    
                                    # Vista previa
                                    with st.expander(f"👁️ Vista previa HTML - {proveedor}"):
                                        st.components.v1.html(html_reclamacion, height=600, scrolling=True)
                        else:
                            st.info("👆 Selecciona los pedidos que deseas reclamar marcando las casillas")
                        
                        # Botón para exportar
                        st.markdown("---")
                        st.download_button(
                            label="📥 Exportar Lista de Pendientes a CSV",
                            data=df_filtrado_reclamacion.to_csv(index=False).encode('utf-8'),
                            file_name=f"pedidos_pendientes_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
            
        else:
            st.error("❌ El archivo no contiene las columnas necesarias.")
            
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)
elif uploaded_file is not None and not hay_proveedores_en_bd():
    st.warning("⚠️ Por favor, carga primero el archivo de proveedores en el sidebar.")
else:
    st.info("👈 Carga los archivos para comenzar el análisis")

# Footer
st.markdown("""
<div class="footer">
    <strong>JAVIER MOLINA</strong> | KAVE HOME - Planning Department
</div>
""", unsafe_allow_html=True)