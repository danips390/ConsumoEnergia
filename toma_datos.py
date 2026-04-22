# -*- coding: utf-8 -*-
import os
import csv
import json
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from tuya_connector import TuyaOpenAPI

# ======================================================================================
# 1) CONFIGURACIÓN
# ======================================================================================
API_ENDPOINT = "https://openapi.tuyaus.com"
ACCESS_ID = os.getenv("TUYA_ACCESS_ID", "devg78x5vga3syrxjdsg")
ACCESS_KEY = os.getenv("TUYA_ACCESS_KEY", "723b31007c6c48c7bbac834688130ca7")

TZ_LOCAL = ZoneInfo("America/Monterrey")
BASE_DIR = "." 
DATA_CONV_FOLDER = os.path.join(BASE_DIR, "data_conv")

VIVIENDAS = {
    "Vivienda1 - Doctor": {"V1Extra": {"tipo": "V1EXTRA_DUAL", "device_id": "eb47caff2a29e9fe2aos9i"}},
    "Vivienda2 - Ángel": {
        "V2FaseA": {"tipo": "V2", "device_id": "eb1410cb74c0a62739mty3"},
        "V2FaseB": {"tipo": "V2", "device_id": "eb109bcaa77dc6fe8baoxs"}
    },
    "Vivienda3 - María": {
        "V3FaseA": {"tipo": "V3", "device_id": "ebc36f971d1011543dfifw"},
        "V3FaseB": {"tipo": "V3", "device_id": "ebe1be117fd2724821law2"}
    }
}

UNIFICACIONES = {
    "Vivienda1 - Doctor": {"archivo_salida": "Medidor1.csv", "latitud": 25.6189821, "longitud": -100.2950499},
    "Vivienda2 - Ángel": {"archivo_salida": "Medidor2.csv", "latitud": 25.646641, "longitud": -100.288357},
    "Vivienda3 - María": {"archivo_salida": "Medidor3.csv", "latitud": 25.648629, "longitud": -100.279387}
}

# ======================================================================================
# 2) FUNCIONES DE EXTRACCIÓN Y PROCESAMIENTO
# ======================================================================================

def get_properties(openapi, device_id):
    path = f"/v2.0/cloud/thing/{device_id}/shadow/properties"
    resp = openapi.get(path)
    if resp.get("success"):
        return {p["code"]: p.get("value") for p in resp["result"]["properties"]}
    return None

def procesar_v1_extra(props, ts):
    fase_a = {
        "voltage_V": props.get("voltage_a", 0)/10, "current_A": props.get("current_a", 0)/1000,
        "power_W": props.get("power_a", 0)/10, "energy_Wh": props.get("energy_forword_a", 0)
    }
    fase_b = {
        "voltage_V": props.get("voltage_b", 0)/10, "current_A": props.get("current_b", 0)/1000,
        "power_W": props.get("power_b", 0)/10, "energy_Wh": props.get("energy_forword_b", 0)
    }
    return fase_a, fase_b

def procesar_v2_v3(props, ts):
    d_conv = {
        "voltage_V": props.get("voltage_a", 0)/10,
        "current_A": props.get("current_b", 0)/1000,
        "power_W": props.get("power_b", 0)/10,
        "energy_Wh": props.get("energy_forword_b", 0)
    }
    return d_conv

# ======================================================================================
# 3) EXPORTACIÓN GEOJSON (INDIVIDUAL Y GLOBAL)
# ======================================================================================

def crear_feature(vivienda, unificado, config):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [config["longitud"], config["latitud"]]},
        "properties": {
            "vivienda": vivienda,
            "timestamp": unificado["timestamp"],
            "potencia_total_W": round(float(unificado["Power_Total_W"]), 2),
            "voltaje_avg_V": round(float(unificado["Voltage_Avg_V"]), 2),
            "energia_total_Wh": round(float(unificado["Energy_Total_Wh"]), 2)
        }
    }

def exportar_geojson_global(features_list):
    geojson_global = {
        "type": "FeatureCollection",
        "features": features_list
    }
    path_global = os.path.join(DATA_CONV_FOLDER, "monitoreo_global.geojson")
    with open(path_global, "w") as f:
        json.dump(geojson_global, f, indent=4)
    print(f"🌎 GeoJSON GLOBAL generado en: {path_global}")

# ======================================================================================
# 4) EJECUCIÓN
# ======================================================================================

def ejecutar():
    openapi = TuyaOpenAPI(API_ENDPOINT, ACCESS_ID, ACCESS_KEY)
    openapi.connect()
    ts = datetime.now(TZ_LOCAL).strftime("%Y-%m-%d %H:%M:%S")
    
    todas_las_features = []

    for viv, sensores in VIVIENDAS.items():
        print(f"Procesando {viv}...")
        fases_list = []
        
        for name, info in sensores.items():
            props = get_properties(openapi, info["device_id"])
            if not props: continue
            
            if info["tipo"] == "V1EXTRA_DUAL":
                fa, fb = procesar_v1_extra(props, ts)
                fases_list.extend([fa, fb])
            else:
                conv = procesar_v2_v3(props, ts)
                fases_list.append(conv)
        
        if fases_list:
            df_fases = pd.DataFrame(fases_list)
            unificado = {
                "timestamp": ts,
                "Voltage_Avg_V": df_fases["voltage_V"].mean(),
                "Power_Total_W": df_fases["power_W"].sum(),
                "Energy_Total_Wh": df_fases["energy_Wh"].sum()
            }
            
            # 1. Guardar CSV local
            folder = os.path.join(DATA_CONV_FOLDER, viv)
            os.makedirs(folder, exist_ok=True)
            csv_path = os.path.join(folder, UNIFICACIONES[viv]["archivo_salida"])
            pd.DataFrame([unificado]).to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)
            
            # 2. Crear feature para esta vivienda y guardarla en la lista global
            feature = crear_feature(viv, unificado, UNIFICACIONES[viv])
            todas_las_features.append(feature)
            
            # 3. Guardar GeoJSON individual (opcional, pero útil)
            with open(os.path.join(folder, "ultimo_estado.geojson"), "w") as f:
                json.dump({"type": "FeatureCollection", "features": [feature]}, f, indent=4)

    # 4. Generar el archivo global con la info de todos
    if todas_las_features:
        exportar_geojson_global(todas_las_features)

if __name__ == "__main__":
    ejecutar()