# Advanced File Ingestion

IceFrame supports advanced ingestion from SQL databases, XML files, and statistical software files (SAS, SPSS, Stata) through optional dependencies.

## Supported Formats

| Format | Extension | Install Command |
| :--- | :--- | :--- |
| **SQL Database** | (URI) | `pip install "iceframe[sql]"` |
| **XML** | `.xml` | `pip install "iceframe[xml]"` |
| **SAS** | `.sas7bdat` | `pip install "iceframe[stats]"` |
| **SPSS** | `.sav` | `pip install "iceframe[stats]"` |
| **Stata** | `.dta` | `pip install "iceframe[stats]"` |

## Usage

### SQL Database

Read from any database supported by `connectorx` (Postgres, MySQL, SQLite, etc.) or `sqlalchemy`.

```python
# Create table from SQL query
ice.create_table_from_sql(
    "my_table", 
    query="SELECT * FROM users WHERE active = true", 
    connection_uri="postgresql://user:pass@localhost:5432/db"
)
```

### XML

```python
# Create table from XML
ice.create_table_from_xml("my_table", "data.xml")
```

### Statistical Files (SAS, SPSS, Stata)

```python
# Create table from SAS
ice.create_table_from_sas("my_table", "study_data.sas7bdat")

# Create table from SPSS
ice.create_table_from_spss("my_table", "survey.sav")

# Create table from Stata
ice.create_table_from_stata("my_table", "analysis.dta")
```

### Generic Insert

You can also use `insert_from_file` with these formats.

```python
ice.insert_from_file("my_table", "data.xml", format="xml")
ice.insert_from_file("my_table", "data.sas7bdat", format="sas")
```
