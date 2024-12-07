from flask import Flask, render_template, request
import ijson
import os
import pandas as pd
from io import BytesIO
import base64
import ast
import shutil
import pickle  

app = Flask(__name__)

# Variables globales para almacenar resultados
results_data = {
    "concentration": [],
    "direct_awards": [],
    "temporal_distribution": [],
}

# Ruta del archivo JSON
file_path = "data.json"
temp_dir = "temp_results"

# Guardar chunk como archivo temporal usando pickle
def save_chunk_to_temp_file(chunk, temp_file):
    df = pd.DataFrame(chunk)
    # Descomponer campos anidados relevantes
    if 'buyer' in df.columns:
        df['buyer.name'] = df['buyer'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)
    # Guardar como archivo pickle
    with open(temp_file, 'wb') as f:
        pickle.dump(df, f)
    print(f"Guardado chunk en archivo temporal: {temp_file}")

# Procesar archivo con ijson y guardar en archivos temporales
def process_with_ijson_and_temp_files(file_path, temp_dir="temp_results", chunk_size=5000):
    os.makedirs(temp_dir, exist_ok=True)
    temp_files = []
    chunk = []
    chunk_index = 0
    total_records = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        parser = ijson.items(f, 'item')

        for record in parser:
            chunk.append(record)
            total_records += 1

            if total_records % chunk_size == 0:
                print(f"Procesados {total_records} registros...")
                temp_file = os.path.join(temp_dir, f"chunk_{chunk_index}.pkl")
                save_chunk_to_temp_file(chunk, temp_file)
                temp_files.append(temp_file)
                chunk = []
                chunk_index += 1

        if chunk:  # Guardar el último chunk si no está vacío
            temp_file = os.path.join(temp_dir, f"chunk_{chunk_index}.pkl")
            save_chunk_to_temp_file(chunk, temp_file)
            temp_files.append(temp_file)

    print(f"Procesamiento completado. Total de registros procesados: {total_records}")
    print(f"Archivos temporales guardados en '{temp_dir}'")
    return temp_files

def process_blocks(temp_files, block_size, analysis_function, temp_dir):
    """
    Procesa archivos temporales en bloques para evitar interrupciones.
    """
    total_files = len(temp_files)
    for i in range(0, total_files, block_size):
        block_files = temp_files[i:i + block_size]
        print(f"Procesando bloque de archivos {i + 1} a {i + len(block_files)} de {total_files}...")
        analysis_function(block_files, temp_dir)
def load_processed_chunks(log_file):
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            return set(f.read().splitlines())
    return set()
def save_processed_chunk(log_file, chunk_name):
    with open(log_file, 'a') as f:
        f.write(chunk_name + '\n')
# Función para marcar chunk como procesado
def mark_chunk_as_processed(chunk, log_file):
    with open(log_file, 'a') as f:
        f.write(chunk + '\n')



# Eliminar archivos temporales
def clear_temp_files(temp_dir):
    """
    Elimina todos los archivos y directorios en el directorio temporal.
    """
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"Archivos temporales eliminados del directorio '{temp_dir}'.")
    else:
        print(f"El directorio temporal '{temp_dir}' no existe.")

def analyze_concentration_with_chunks(temp_files, temp_dir, log_file="processed_chunks.log"):
    print("Iniciando análisis de concentración...")
    all_results = []
    processed_chunks = load_processed_chunks(log_file)  # Cargar chunks ya procesados
    import traceback

    try:
        for temp_file in temp_files:
            # Verificar si ya fue procesado
            if temp_file in processed_chunks:
                print(f"El archivo {temp_file} ya fue procesado. Saltando...")
                continue

            print(f"Procesando archivo temporal: {temp_file}...")
            try:
                # Cargar el archivo pickle
                with open(temp_file, 'rb') as f:
                    chunk = pickle.load(f)

                if 'buyer.name' not in chunk.columns:
                    print(f"Advertencia: La columna 'buyer.name' no está presente en {temp_file}.")
                    mark_chunk_as_processed(temp_file, log_file)
                    continue

                if 'awards' not in chunk.columns:
                    print(f"Advertencia: La columna 'awards' no está presente en {temp_file}.")
                    mark_chunk_as_processed(temp_file, log_file)
                    continue

                # Expandir las listas en la columna 'awards'
                chunk = chunk.explode('awards').dropna(subset=['awards']).reset_index(drop=True)

                # Asegurarse de que cada elemento en 'awards' es un diccionario
                chunk['awards'] = chunk['awards'].apply(lambda x: x if isinstance(x, dict) else {})

                # Normalizar los datos de 'awards' y extraer campos anidados
                awards = pd.json_normalize(
                    chunk['awards'],
                    sep='.'
                )

                # Agregar 'buyer_name' al DataFrame awards
                awards['buyer_name'] = chunk['buyer.name'].values

                # Verificar si 'value.amount' está en awards
                if 'value.amount' in awards.columns:
                    awards['amount'] = awards['value.amount'].fillna(0)
                else:
                    print(f"Advertencia: La columna 'value.amount' no está presente en los datos normalizados.")
                    awards['amount'] = 0

                # Verificar si 'suppliers' está en awards
                if 'suppliers' in awards.columns:
                    # Expandir 'suppliers' para obtener 'supplier_name'
                    awards['supplier_name'] = awards['suppliers'].apply(
                        lambda x: x[0]['name'] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], dict) else "Desconocido"
                    )
                else:
                    print(f"Advertencia: La columna 'suppliers' no está presente en los datos normalizados.")
                    awards['supplier_name'] = "Desconocido"

                # Agrupar por comprador y proveedor
                grouped = awards.groupby(['buyer_name', 'supplier_name']).agg(
                    contracts=('id', 'count'),
                    total_amount=('amount', 'sum')
                ).reset_index()

                # Calcular participaciones porcentuales
                grouped['contract_share'] = grouped.groupby('buyer_name')['contracts'].transform(lambda x: x / x.sum() * 100)
                grouped['amount_share'] = grouped.groupby('buyer_name')['total_amount'].transform(lambda x: x / x.sum() * 100)

                # Seleccionar los top 100 por monto total
                top_100_by_amount = grouped.sort_values(by='total_amount', ascending=False).head(100)
                all_results.append(top_100_by_amount)

                # Marcar chunk como procesado
                mark_chunk_as_processed(temp_file, log_file)

            except Exception as chunk_error:
                print(f"Error procesando el archivo {temp_file}: {chunk_error}")
                continue

        if all_results:
            combined_results = pd.concat(all_results).reset_index(drop=True)
            final_results = combined_results.sort_values(by='amount_share', ascending=False).head(100)
            results_data['concentration'] = final_results.to_dict('records')
        else:
            print("No se generaron resultados.")
            results_data['concentration'] = []

    except Exception as e:
        print(f"Error en el análisis de concentración: {e}")
        traceback.print_exc()
        results_data['concentration'] = []

    finally:
        print("Archivos temporales no eliminados para análisis posterior.")



def analyze_direct_awards_with_chunks(temp_files, temp_dir, log_file="processed_chunks.log"):
    print("Iniciando análisis de adjudicaciones directas...")
    processed_chunks = load_processed_chunks(log_file)
    all_contracts = []

    try:
        for temp_file in temp_files:
            chunk_name = os.path.basename(temp_file)
            if chunk_name in processed_chunks:
                print(f"Saltando archivo ya procesado: {chunk_name}")
                continue

            print(f"Procesando archivo temporal: {temp_file}...")

            # Cargar el archivo pickle
            with open(temp_file, 'rb') as f:
                chunk = pickle.load(f)

            # Imprimir columnas disponibles para depuración
            print(f"Columnas disponibles en {temp_file}: {chunk.columns.tolist()}")

            # Desanidar la columna 'tender' si está presente
            if 'tender' in chunk.columns:
                tender_data = pd.json_normalize(chunk['tender'], sep='.')
                tender_data = tender_data.add_prefix('tender.')
                chunk = chunk.drop(columns=['tender'])
                chunk = pd.concat([chunk, tender_data], axis=1)

            # Verificar columnas requeridas
            required_columns = ['contracts', 'awards', 'tender.procurementMethod', 'buyer.name']
            missing_columns = [col for col in required_columns if col not in chunk.columns]

            if missing_columns:
                print(f"Advertencia: Las columnas requeridas no están presentes en el archivo: {temp_file}")
                continue

            # Filtrar adjudicaciones directas
            filtered_data = chunk[chunk['tender.procurementMethod'] == 'direct'].copy()

            if filtered_data.empty:
                print(f"No se encontraron contratos con el método 'direct' en {temp_file}.")
                continue

            # Asegurar que 'contracts' y 'awards' sean listas
            filtered_data['contracts'] = filtered_data['contracts'].apply(lambda x: x if isinstance(x, list) else [])
            filtered_data['awards'] = filtered_data['awards'].apply(lambda x: x if isinstance(x, list) else [])

            # Expandir contratos y adjudicaciones
            contracts = filtered_data.explode('contracts')
            awards = filtered_data.explode('awards')

            # Normalizar datos y agregar prefijos para evitar conflictos
            contracts_normalized = pd.json_normalize(contracts['contracts'].dropna(), sep='.')
            contracts_normalized = contracts_normalized.add_prefix('contracts.')
            awards_normalized = pd.json_normalize(awards['awards'].dropna(), sep='.')
            awards_normalized = awards_normalized.add_prefix('awards.')

            if 'awards.id' in awards_normalized.columns:
                awards_normalized = awards_normalized.rename(columns={'awards.id': 'award_id'})
            else:
                print("Error: La columna 'awards.id' no está presente en 'awards'.")
                continue

            if 'contracts.awardID' not in contracts_normalized.columns or 'award_id' not in awards_normalized.columns:
                print("Error: Las columnas 'contracts.awardID' o 'award_id' no están presentes después de la normalización.")
                continue

            # Fusionar contratos y adjudicaciones
            contracts_merged = contracts_normalized.merge(
                awards_normalized[['award_id', 'awards.suppliers']],
                left_on='contracts.awardID',
                right_on='award_id',
                how='left'
            )

            def extract_supplier_name(suppliers):
                try:
                    if isinstance(suppliers, list) and len(suppliers) > 0:
                        return suppliers[0].get('name', "Desconocido")
                except Exception:
                    pass
                return "Desconocido"

            # Enriquecer contratos con datos adicionales
            contracts_counts = filtered_data['contracts'].apply(len)
            buyer_names = filtered_data['buyer.name'].repeat(contracts_counts).reset_index(drop=True)
            contracts_merged = contracts_merged.reset_index(drop=True)
            contracts_merged['buyer_name'] = buyer_names
            contracts_merged['supplier_name'] = contracts_merged['awards.suppliers'].apply(extract_supplier_name)
            contracts_merged['amount'] = contracts_merged['contracts.value.amount'].fillna(0)

            # Acumular contratos
            all_contracts.append(contracts_merged)

            # Guardar el progreso
            save_processed_chunk(log_file, chunk_name)

        if not all_contracts:
            print("No se generaron resultados para adjudicaciones directas.")
            results_data['direct_awards'] = []
        else:
            combined_contracts = pd.concat(all_contracts, ignore_index=True)

            grouped = combined_contracts.groupby(['supplier_name', 'buyer_name']).agg(
                total_contratos_entidad=('contracts.id', 'count'),
                monto_total_entidad=('amount', 'sum')
            ).reset_index()

            total_contracts_per_supplier = grouped.groupby('supplier_name').agg(
                total_contratos=('total_contratos_entidad', 'sum'),
                monto_total_mxn=('monto_total_entidad', 'sum')
            ).reset_index()

            total_contracts_per_supplier['total_contratos'] = total_contracts_per_supplier['total_contratos'].astype(int)

            suppliers = grouped.groupby('supplier_name').apply(
                lambda x: {
                    'entidades_compradoras': [
                        {
                            'nombre_entidad': row['buyer_name'],
                            'total_contratos': int(row['total_contratos_entidad']),
                            'monto_total_mxn': float(row['monto_total_entidad'])
                        }
                        for _, row in x.iterrows()
                    ]
                }
            ).reset_index(name='detalles')

            suppliers = suppliers.merge(total_contracts_per_supplier, on='supplier_name')

            suppliers = suppliers.rename(columns={
                'supplier_name': 'proveedor',
                'total_contratos': 'total_contratos',
                'monto_total_mxn': 'monto_total_mxn'
            })

            suppliers['total_contratos'] = suppliers['total_contratos'].astype(int)
            suppliers = suppliers.sort_values(by='total_contratos', ascending=False)
            suppliers_top_100 = suppliers.head(100).reset_index(drop=True)

            results_data['direct_awards'] = suppliers_top_100.to_dict('records')

        print("Análisis de adjudicaciones directas completado exitosamente.")

    except Exception as e:
        print(f"Error en el análisis de adjudicaciones directas: {e}")
        results_data['direct_awards'] = []


def analyze_temporal_distribution_with_chunks(temp_files, temp_dir, log_file="processed_chunks.log"):
    print("Iniciando análisis de distribución temporal...")
    all_awards = []
    processed_chunks = load_processed_chunks(log_file)  # Cargar el registro de progreso

    try:
        for temp_file in temp_files:
            # Verificar si el archivo ya fue procesado
            if temp_file in processed_chunks:
                print(f"El archivo {temp_file} ya fue procesado. Saltando...")
                continue

            print(f"Procesando archivo temporal: {temp_file}...")

            try:
                # Cargar el archivo pickle
                with open(temp_file, 'rb') as f:
                    chunk = pickle.load(f)

                # Imprimir columnas disponibles para depuración
                print(f"Columnas disponibles en {temp_file}: {chunk.columns.tolist()}")

                # Verificar si la columna 'awards' está presente
                if 'awards' not in chunk.columns:
                    print(f"Advertencia: La columna 'awards' no está presente en {temp_file}.")
                    mark_chunk_as_processed(temp_file, log_file)
                    continue

                # Expandir las listas en la columna 'awards'
                chunk['awards'] = chunk['awards'].apply(lambda x: x if isinstance(x, list) else [])
                exploded_awards = chunk.explode('awards')

                # Normalizar los datos de la columna 'awards'
                awards = pd.json_normalize(exploded_awards['awards'].dropna(), sep='.')

                # Verificar si la columna 'contractPeriod.startDate' está presente
                if 'contractPeriod.startDate' not in awards.columns:
                    print(f"Advertencia: La columna 'contractPeriod.startDate' no está presente en {temp_file}.")
                    mark_chunk_as_processed(temp_file, log_file)
                    continue

                # Convertir las fechas a formato datetime
                awards['award_date'] = pd.to_datetime(awards['contractPeriod.startDate'], errors='coerce')

                # Verificar si hay fechas válidas
                if awards['award_date'].isnull().all():
                    print(f"No se encontraron fechas válidas en {temp_file}.")
                    mark_chunk_as_processed(temp_file, log_file)
                    continue

                # Agregar columna del mes
                awards['month'] = awards['award_date'].dt.to_period('M')

                # Guardar el chunk procesado
                all_awards.append(awards)

                # Marcar el archivo como procesado
                mark_chunk_as_processed(temp_file, log_file)

            except Exception as e:
                print(f"Error procesando el archivo {temp_file}: {e}")
                continue

        if not all_awards:
            print("No se generaron resultados para la distribución temporal.")
            results_data['temporal_distribution'] = []
            return

        # Combinar todos los awards de los chunks
        combined_awards = pd.concat(all_awards, ignore_index=True)

        # Agrupar por mes
        grouped = combined_awards.groupby('month').agg(
            num_contracts=('id', 'count'),
            total_amount=('value.amount', 'sum')
        ).reset_index()

        # Convertir el período a string
        grouped['month'] = grouped['month'].astype(str)

        # Guardar los resultados en el formato esperado
        results_data['temporal_distribution'] = {
            "labels": grouped['month'].tolist(),
            "num_contracts": grouped['num_contracts'].tolist(),
            "total_amount": grouped['total_amount'].fillna(0).tolist()
        }

        print("Análisis de distribución temporal completado exitosamente.")

    except Exception as e:
        print(f"Error en el análisis de distribución temporal: {e}")
        results_data['temporal_distribution'] = []

    finally:
        print("Archivos temporales no eliminados para permitir análisis posterior.")



def analyze_monopolistic_suppliers_with_chunks(temp_files, temp_dir, log_file="processed_chunks.log"):
    print("Iniciando análisis de proveedores monopólicos...")
    all_items = []
    processed_chunks = load_processed_chunks(log_file)  # Cargar chunks ya procesados

    try:
        for temp_file in temp_files:
            # Verificar si ya fue procesado
            if temp_file in processed_chunks:
                print(f"El archivo {temp_file} ya fue procesado. Saltando...")
                continue

            print(f"Procesando archivo temporal: {temp_file}...")

            try:
                # Cargar el archivo pickle
                with open(temp_file, 'rb') as f:
                    chunk = pickle.load(f)

                # Imprimir columnas disponibles para depuración
                print(f"Columnas disponibles en {temp_file}: {chunk.columns.tolist()}")

                # Verificar columnas necesarias
                if 'awards' not in chunk.columns:
                    print(f"Advertencia: La columna 'awards' no está presente en el archivo: {temp_file}")
                    mark_chunk_as_processed(temp_file, log_file)
                    continue

                # Explotar adjudicaciones
                chunk['awards'] = chunk['awards'].apply(lambda x: x if isinstance(x, list) else [])
                awards = chunk.explode('awards').reset_index(drop=True)
                awards = pd.json_normalize(awards['awards'].dropna(), sep='.')

                # Verificar si 'items' está presente en adjudicaciones
                if 'items' not in awards.columns:
                    print("Advertencia: La columna 'items' no está presente en las adjudicaciones.")
                    mark_chunk_as_processed(temp_file, log_file)
                    continue

                # Explotar items dentro de cada adjudicación
                awards['items'] = awards['items'].apply(lambda x: x if isinstance(x, list) else [])
                awards = awards.explode('items').reset_index(drop=True)
                items = pd.json_normalize(awards['items'].dropna(), sep='.')

                # Agregar datos relevantes desde awards
                items['award_id'] = awards['id']
                items['supplier_name'] = awards['suppliers'].apply(
                    lambda x: x[0]['name'] if isinstance(x, list) and len(x) > 0 else "Desconocido"
                )
                items['amount'] = awards['value.amount'].fillna(0)
                items['category_id'] = awards['classification.id']
                items['category_name'] = awards['classification.description']

                # Acumular items procesados
                all_items.append(items)

                # Marcar chunk como procesado
                mark_chunk_as_processed(temp_file, log_file)

            except Exception as e:
                print(f"Error procesando el archivo {temp_file}: {e}")
                continue

        if not all_items:
            print("No se encontraron datos para el análisis de proveedores monopólicos.")
            results_data['monopolistic_suppliers'] = []
            return

        # Combinar todos los items de todos los chunks
        combined_items = pd.concat(all_items, ignore_index=True)

        # Agrupar por categoría y proveedor
        grouped = combined_items.groupby(['category_id', 'category_name', 'supplier_name'], as_index=False).agg(
            num_contracts=('award_id', 'count'),
            total_amount=('amount', 'sum')
        )

        # Calcular el porcentaje de contratos por categoría
        grouped['contract_share'] = grouped.groupby('category_id')['num_contracts'].transform(lambda x: x / x.sum() * 100)

        # Filtrar proveedores monopólicos (>90% de los contratos en una categoría)
        monopolistic = grouped[grouped['contract_share'] > 90]

        # Ordenar por monto total adjudicado de mayor a menor
        monopolistic = monopolistic.sort_values(by='total_amount', ascending=False)

        # Seleccionar el top 100 por monto total adjudicado
        top_100 = monopolistic.head(100)

        # Convertir a diccionario
        results_data['monopolistic_suppliers'] = top_100.to_dict('records')
        print("Análisis de proveedores monopólicos completado.")

    except Exception as e:
        print(f"Error en el análisis de proveedores monopólicos: {e}")
        results_data['monopolistic_suppliers'] = []

    finally:
        print("Archivos temporales no eliminados para permitir análisis posterior.")

# Funciones auxiliares para el log de progreso
def load_processed_chunks(log_file):
    if not os.path.exists(log_file):
        return set()
    with open(log_file, 'r') as f:
        return set(line.strip() for line in f)

def mark_chunk_as_processed(chunk, log_file):
    with open(log_file, 'a') as f:
        f.write(chunk + '\n')


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        analysis_type = request.form.get("analysis_type")
        results = []
        message = None

        temp_files = process_with_ijson_and_temp_files(file_path, temp_dir=temp_dir, chunk_size=5000)
        block_size = 10  # Procesar 10 archivos temporales por bloque

        if analysis_type == "concentration":
            process_blocks(temp_files, block_size, analyze_concentration_with_chunks, temp_dir)
            results = results_data.get("concentration", [])
        elif analysis_type == "direct_awards":
            process_blocks(temp_files, block_size, analyze_direct_awards_with_chunks, temp_dir)
            results = results_data.get("direct_awards", [])
        elif analysis_type == "temporal_distribution":
            process_blocks(temp_files, block_size, analyze_temporal_distribution_with_chunks, temp_dir)
            results = results_data.get("temporal_distribution", [])
        elif analysis_type == "monopolistic_suppliers":
            process_blocks(temp_files, block_size, analyze_monopolistic_suppliers_with_chunks, temp_dir)
            results = results_data.get("monopolistic_suppliers", [])
        else:
            message = "Error: Tipo de análisis desconocido."

        return render_template("index.html", results=results, analysis_type=analysis_type, message=message)

    return render_template("index.html", results=[], analysis_type=None, message=None)




if __name__ == "__main__":
    app.run(debug=True)
