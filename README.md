# 🏐 AWS Beach Volley Ranking Pipeline

Pipeline de datos **serverless en AWS** para la ingesta, procesamiento y análisis del Ranking Nacional de Vóley Playa (España).

---

## 🚀 Qué hace este proyecto

- Ingesta de registros de jugadores en **Amazon Kinesis**
- Transformación y particionamiento con **Lambda + Firehose**
- Almacenamiento en **Amazon S3** (Data Lake)
- Procesamiento ETL con **AWS Glue**
- Análisis y ranking final con **Amazon Athena**

---

## 🏗️ Arquitectura (resumen)
```
Producer (Python)
    ↓
Kinesis Data Stream
    ↓
Firehose + Lambda
    ↓
S3 (raw)
    ↓
Glue (Crawler + Jobs)
    ↓
S3 (processed, Parquet)
    ↓
Athena
```

---

## 📂 Estructura del repositorio
```
.
├── src/producer/          # Productor Kinesis
├── lambda/                # Lambda de Firehose
├── jobs/                  # Glue ETL Jobs
├── scripts/               # Scripts de despliegue AWS
├── figuras/               # Capturas y diagramas
├── memoria.pdf            # Documentación completa
└── README.md
```

---

## ▶️ Cómo ejecutarlo (resumen)

### Requisitos

- Cuenta AWS
- AWS CLI configurado
- PowerShell
- Python 3.x

### Pasos

1. **Crear bucket y estructura S3**
```powershell
   scripts/create_bucket.ps1
```

2. **Configurar Firehose + Lambda**
```powershell
   scripts/firehose_setup.ps1
```

3. **Enviar datos a Kinesis**
```bash
   python src/producer/kinesis.py
```

4. **Ejecutar Glue (Crawler + Jobs)**
```powershell
   scripts/glue.ps1
```

5. **Consultar resultados en Athena**

---

## 📊 Datos de entrada (ejemplo)
```json
{
  "IdPersona": "392510",
  "ApellidosNombre": "VIERA IGLESIAS, ALVARO",
  "Puntos": "21,922",
  "EquipoVoleyPlaya": "VP Madrid"
}
```

---

## 👤 Autor

**Miguel Castellano Hernández**  
Grado en Ingeniería Informática – ULPGC