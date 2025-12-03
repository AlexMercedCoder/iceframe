# Namespace Management

IceFrame allows you to manage namespaces (schemas/databases) in your Iceberg catalog.

## Accessing Namespace Manager

```python
# Access via the namespaces property
ns_manager = ice.namespaces
```

## Creating a Namespace

```python
ice.create_namespace("marketing", {"owner": "team-marketing"})
```

## Dropping a Namespace

```python
ice.drop_namespace("marketing")
```

## Listing Namespaces

```python
# List top-level namespaces
namespaces = ice.list_namespaces()
print(namespaces)

# List nested namespaces
sub_namespaces = ice.list_namespaces("marketing")
```
